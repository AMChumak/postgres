/*--------------------------------------------------------------------
 * guc_composite.c
 *
 * This file contains the implementation of functions
 * related to the custom composite type system.
 *
 * The functions are divided into 3 groups:
 * 1. registration and support for custom types
 * 2. support for custom type options
 * 3. parsing values of composite types
 *
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2025, PostgreSQL Global Development Group
 * Written by Anton Chumak <A.M.Chumak@yandex.com>.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_composite.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"


#include <alloca.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <float.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ucontext.h>
#include <time.h>
#include <unistd.h>

#include "guc_composite.h"
#include "utils/builtins.h"


int expand_array_view_thd;

#define STRUCT_FIELDS_DELIMETER ";"


/*
 * Get size in dynamic array. It places after pointer to data
 */
#define dynamic_array_size(ptr) (*(int *)((char *)ptr + sizeof(void *)))

struct DynArrTmp
{
	void *data;
	int size;
};


HTAB *guc_types_hashtab;

int get_static_array_size(const char * type_name);
static int get_field_offset(const char * type_name, const char *field);
char *get_array_basic_type(const char * array_type_name);
static int get_type_offset(const char *type_name);
bool is_assignment_list(const char *value);
bool parse_placeholder_patch_list(const char *value, const char *type, void **result,const void *prev_val, int flags, const char **hintmsg);
int canonize_idx(const char * field);
char *get_static_aray_element_type(const char *type_name, const char *field);
char *get_dynamic_array_element_type(const char *type_name, const char *field, const void *structure);
char *get_struct_field_type(const char *type_name, const char *field);
char *get_field_type_name(const char *type_name, const char *field);
char *static_array_to_str(const void *structp, const char *type, bool serialize);
char *dynamic_array_to_str(const void *structp, const char *type, bool serialize);
bool is_atomic_type(const char* type);
char *atomic_to_str(const void *structp, const char *type, bool serialize);
char *structure_to_str(const void *structp, const char *type, bool serialize);
void static_array_duplicate(void *dest_struct, const void *src_struct, const char *type);
void struct_duplicate(void *dest_struct, const void *src_struct, const char *type);
int array_data_cmp(const void *first, const void *second, const char *type, int size);
int dynamic_array_cmp(const void *first, const void *second, const char *type);
int structure_cmp(const void *first, const void *second, const char *type);
void free_aux_mem_stat_arr(void *delptr, const char *type);
void free_aux_mem_dyn_arr(void *delptr, const char *type);
void free_aux_structure_mem(void *delptr, const char *type);
void dynamic_array_duplicate(void *dest_struct, const void *src_struct, const char *type);
static int get_element_offset_with_index(const char *type_name, int index);
static int get_dynamic_array_mem_size_with_length(const char *type_name, const int length);
parser_res find_same_level_symbol(const char *start, const char symbol);
parser_res get_index(char *start);
parser_res get_name(char *start);
parser_res get_max_index(char *start);
bool is_empty_array(char *start, char *end);
char* check_braces(char* c, const char open, const char close, const char **hintmsg);
parser_res check_array_syntax(char *start, const char **hintmsg);
parser_res parse_array_element(char *strval, const char *array_type, void *res_arr, int prev_index, int flags, const char **hintmsg);
parser_res parse_struct_element(char *strval, const char *struct_type, void *res_struct, int flags, const char **hintmsg);
parser_res parse_prepared_array(char *start, const char *array_type, void *res_arr, int flags, const char **hintmsg);
parser_res parse_static_array(char *strval, const char *array_type, void *res_arr, int flags, const char **hintmsg);
parser_res parse_dynamic_array(char *strval, const char *array_type, void *res_arr, int flags, const char **hintmsg);
parser_res find_field(char *start, const char *name);
parser_res parse_extended_dynamic_array(char *strval, const char *array_type, void *res_arr, int flags, const char **hintmsg);
parser_res parse_atomic_type(char *strval, const char *struct_type, void *result, int flags, const char **hintmsg);
parser_res parse_structure(char *strval, const char *struct_type, void *res_struct, int flags, const char **hintmsg);
parser_res parse_composite_impl(char *value, const char *type, void *result, int flags, const char **hintmsg);


/*
 * Finds symbol on the same nest level.
 * Type could be ']' or '}'
 * Start is a position of search start. Start must be after open brace!
 * Returns position of found symbol or pointer to '\0' (if not found)
 * Returns NULL in error case
 */
parser_res find_same_level_symbol(const char *start, const char symbol)
{
	const char *c = start; /* pointer to current symbol          */
	int braces_cntr = 0; /* counter of nest level for braces   */
	int brackets_cntr = 0;  /* counter of nest level for brackets */
	bool in_str = false;   /* indicator that cymbol in string    */
	int quote_cntr = 0;
	parser_res result;

	for (; *c; c++)
	{
		if (*c == symbol && !in_str && braces_cntr == 0 && brackets_cntr == 0)
			break;
		if (*c == '{' && !in_str)
			braces_cntr++;
		else if (*c == '}' && !in_str)
			braces_cntr--;
		else if (*c == '[' && !in_str)
			brackets_cntr++;
		else if (*c == ']' && !in_str)
			brackets_cntr--;
		else if (*c == '\'')
		{
			quote_cntr ^= 1;
			if (in_str && quote_cntr != 0 && *(c+1) != '\'' )
			{
				quote_cntr = 0;
				in_str = false;
			}
			else if (!in_str)
			{
				quote_cntr = 0;
				in_str = true;
			}
		}

		/* counter < 0 means that array or struct ended */
		if (braces_cntr < 0 || brackets_cntr < 0)
			break;
	}

	result.res_str = (char *)c;
	if (*c == symbol)
		result.status = PARSER_OK;
	else
		result.status = PARSER_NOT_FOUND;

	return result;
}


/*
 * Gets index for array element from string
 * Parse ends on next delimeter (comma or close bracket)
 * Returns integer index if found it
 */
parser_res get_index(char *start)
{
	int index = -1;
	char *next_delimiter = NULL;
	char *colon = NULL;
	char *index_start;
	char *index_end;
	char *check_strtol;

	parser_res result = {};

	/* find next delimiter */
	parser_res search_res = find_same_level_symbol(start, ',');
	if (IS_STATUS_OK(search_res))
		next_delimiter = search_res.res_str;


	search_res = find_same_level_symbol(start, ']');
	if (IS_STATUS_OK(search_res) &&
		(!next_delimiter || search_res.res_str < next_delimiter))
			next_delimiter = search_res.res_str;
	else if (IS_STATUS_NOT_FOUND(search_res))
	{
		ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("array has no close bracket")));
		goto process_err;
	}

	if (!next_delimiter)
	{
		ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("incorrect bracket sequence")));
		goto process_err;
	}

	result.parse_end = next_delimiter;

	/* find colon */
	search_res = find_same_level_symbol(start, ':');

	if (IS_STATUS_OK(search_res))
		colon = search_res.res_str;
	else if (IS_STATUS_NOT_FOUND(search_res))
		goto process_not_found;
	if (colon > next_delimiter)
		goto process_not_found;

	/* extract index */
	for (index_start = start; index_start < colon && isspace(*index_start); index_start++);

	/* skip space after index */
	for (index_end = colon; index_end > index_start && isspace(*(index_end - 1) ); index_end--);

	/* examine index */
	if (index_start >= index_end)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("empty array index"),
				errdetail("there are space before \':\'"),
				errhint("Set number before \':\' or do not use \':\'")));
		goto process_err;
	}

	for (char *p = index_start; p < index_end; p++)
		if (!isdigit(*p))
		{
			char *incorrect_index = guc_malloc(ERROR, index_end - index_start + 1);
			strncpy(incorrect_index, index_start, index_end - index_start);
			incorrect_index[index_end - index_start] = 0;

			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errdetail("incorrect array index: %s", incorrect_index),
					errhint("array index must be a number >= 0")));
			guc_free(incorrect_index);

			goto process_err;
		}

	/* convert index to int*/
	index = (int)strtol(index_start, &check_strtol, 10);
	if (check_strtol != index_end)
	{
		char *incorrect_index = guc_malloc(ERROR, index_end - index_start + 1);
		strncpy(incorrect_index, index_start, index_end - index_start);
		incorrect_index[index_end - index_start] = 0;

		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("incorrect index: %s", incorrect_index),
				errdetail("index could not be coorrect parsed with strtol"),
				errhint("array index must be a number >= 0")));
		guc_free(incorrect_index);

		goto process_err;
	}

	result.status = PARSER_OK;
	result.res_int = index;
	return result;

process_not_found:
	result.status = PARSER_NOT_FOUND;
	return result;

process_err:
	result.status = PARSER_ERR;
	return result;
}


/*
 * Gets name of structure's field
 * Parse ends on next delimeter (comma or close brace)
 * Returns integer index if found it
 */
parser_res get_name(char *start)
{
	char *colon = NULL;
	char *next_delimiter = NULL;
	char *name_start = NULL;
	char *name_end = NULL;
	size_t name_len = 0;
	char *name = NULL;
	parser_res result = {};

	/* find next delimiter */
	parser_res search_res = find_same_level_symbol(start, ',');
	if (IS_STATUS_OK(search_res))
		next_delimiter = search_res.res_str;

	search_res = find_same_level_symbol(start, '}');
	if (IS_STATUS_OK(search_res) &&
		(!next_delimiter || search_res.res_str < next_delimiter))
			next_delimiter = search_res.res_str;
	else if (IS_STATUS_NOT_FOUND(search_res))
	{
		ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("structure has no close brace")));
		goto process_err;
	}

	if (!next_delimiter)
	{
		ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("incorrect brace sequence")));
		goto process_err;
	}

	result.parse_end = next_delimiter;

	/* find colon */
	search_res = find_same_level_symbol(start, ':');
	if (IS_STATUS_OK(search_res))
		colon = search_res.res_str;
	else if (IS_STATUS_NOT_FOUND(search_res))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_NAME),
				errmsg("name of field not found"),
				errhint("add name for every field, use SHOW to see them")));
		goto process_err;
	}

	/* extract name */
	for (name_start = start; name_start < colon && isspace(*name_start); name_start++);

	/* delete space after name */
	for (name_end = colon; name_end > name_start && isspace(*(name_end-1)); name_end--);

	/* examine pointers*/
	if (name_start >= name_end)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_NAME),
				errmsg("empty field name"),
				errdetail("there are space before \':\'"),
				errhint("Set name before \':\' (name starts with letter)")));
		goto process_err;
	}

	/* copy name */
	name_len = name_end - name_start;
	name = guc_malloc(ERROR, name_len + 1);
	strncpy(name, name_start, name_len);
	name[name_len] = '\0';

	result.status = PARSER_OK;
	result.res_str = name;
	return result;

process_err:
	result.status = PARSER_ERR;
	return result;
}


/*
 * Array elements could be written all with or all without indexes before them
 * This functions checks that rule and compute maximum index of element
 */
parser_res get_max_index(char *start)
{
	/*
	 * state = 0 means that style non-determined still (initial state)
	 * state = 1 means that there are indexes before elements
	 * state = -1 means that there are no indexes
	 */
	int state = 0;
	char *next_del = NULL;
	int index = 0;
	parser_res result = {};

	parser_res index_state = get_index(start);

	if (IS_STATUS_OK(index_state))
	{
		state = 1;
		index = index_state.res_int;
		next_del = index_state.parse_end;
	}
	else if (IS_STATUS_NOT_FOUND(index_state))
	{
		next_del = index_state.parse_end;
		state = -1;
	}
	else if (IS_STATUS_ERR(index_state))
		goto process_err;


	while (*next_del != ']')
	{
		parser_res next_idx_st;
		next_del++;

		next_idx_st = get_index(next_del);

		if (IS_STATUS_OK(next_idx_st))
		{
			if (state == -1)
			{
				ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("index in array without indeces"),
					errdetail("There is index must be for each element or for no one")));
				goto process_err;
			}

			next_del = next_idx_st.parse_end;
			if (next_idx_st.res_int > index)
				index = next_idx_st.res_int;
		}
		else if (IS_STATUS_NOT_FOUND(next_idx_st))
		{
			if (state == 1)
			{
				ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("empty index in array with indeces"),
					errdetail("There is index must be for each element or for no one")));
				goto process_err;
			}

			index++;
			next_del = next_idx_st.parse_end;
		}
		else if (IS_STATUS_ERR(next_idx_st))
			goto process_err;
	}

	result.status = PARSER_OK;
	result.res_int = index;
	return result;

process_err:
	result.status = PARSER_ERR;
	return result;
}

bool is_empty_array(char *start, char *end)
{
	char *c;
	for (c = start + 1; isspace(*c); c++);
	if (c != end)
		return false;
	return true;
}

/*
 * Parses array element
 * Returns pointer to next delimeter in success case.
 */
parser_res parse_array_element(char *strval, const char *array_type, void *res_arr, int prev_index, int flags, const char **hintmsg)
{
	char *c = strval;      /* pointer to current symbol                                              */
	char *next_colon;      /* colon is used to delimit index and value                               */
	bool next_colon_found = false;
	int index = -1;        /* index of element                                                       */
	char delimiter_ph = 0; /* placeholder for delimiter for case, when we replace delimiter to space */
	char *del_ptr;         /* pointer to position that will be end of elment text representation     */
	char *basic_type = get_array_basic_type(array_type);
	char *result_ptr;
	char *next_del = NULL;
	parser_res element_res = {};
	parser_res search_res = {};
	parser_res result = {};
	parser_res index_state = {};

	if (!basic_type)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("invalid array_type: %s", array_type)));
		goto process_err;
	}

	/* parse index */
	index_state = get_index(c);
	if (IS_STATUS_OK(index_state))
	{
		index = index_state.res_int;
		next_colon_found = true;
		result.parse_end = next_del = index_state.parse_end;
	}
	else if (IS_STATUS_NOT_FOUND(index_state))
	{
		index = prev_index + 1;
		result.parse_end = next_del = index_state.parse_end;
	}
	else if (IS_STATUS_ERR(index_state))
	{
		*hintmsg = gettext_noop("incorrect index");
		goto process_err;
	}

	/*if index is here, colon will be found automatically */
	if (next_colon_found)
	{
		search_res = find_same_level_symbol(strval, ':');
		next_colon = search_res.res_str;
	}

	/* delete space embedding */
	delimiter_ph = *next_del; /* we will recover next delimiter with delimiter placeholder */
	del_ptr = next_del;
	for (del_ptr = next_del; isspace(*(del_ptr - 1)); del_ptr--);
	*del_ptr = '\0';

	if (next_colon_found)
		c = next_colon + 1;

	for (;isspace(*c); c++);

	/* parse element */
	result_ptr = (char *)res_arr + get_element_offset_with_index(array_type, index);
	element_res = parse_composite_impl(c, basic_type, result_ptr, flags, hintmsg);

	/* recover */
	guc_free(basic_type);
	*next_del = delimiter_ph;

	if(IS_STATUS_ERR(element_res))
		goto process_err;

	result.status = PARSER_OK;
	return result;

process_err:
	elog(WARNING, "in element: %d", index);
	result.status = PARSER_ERR;
	return result; /* return error parse position */
}


/*
 * Parses structure element
 */
parser_res parse_struct_element(char *strval, const char *struct_type, void *res_struct, int flags, const char **hintmsg)
{
	char *c = strval;         /* pointer to current symbol                                               */
	char *next_del = NULL;    /* next delimiter between array elements                                   */
	char *next_colon = NULL;  /* colon is used to delimit index and value                                */
	int offset = 0;           /* offset of field in structure                                            */
	char delimiter_ph = 0;    /* placeholder for delimiter for case, when we replace delimiter to space  */
	char *del_ptr = NULL;     /* pointer to position that will be end of elment text representation      */
	char *field_type = NULL;
	char *result_ptr = NULL;
	parser_res element_res;
	parser_res search_res;
	parser_res result = {};

	/* parse name */
	parser_res name_state = get_name(c);
	if (IS_STATUS_OK(name_state))
	{
		result.parse_end = next_del = name_state.parse_end;

		offset = get_field_offset(struct_type, name_state.res_str);
		field_type = get_field_type_name(struct_type, name_state.res_str);

		/* examine computed offset */
		if (offset < 0 || !field_type)
		{
			*hintmsg = gettext_noop("incorrect name");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("invalid name of field: %s", name_state.res_str)));
			goto process_err;
		}
	}
	else if (IS_STATUS_ERR(name_state) || IS_STATUS_NOT_FOUND(name_state))
	{
		*hintmsg = gettext_noop("incorrect name");
		goto process_err;
	}

	/* get next colon position. It exists because name was parsed*/
	search_res = find_same_level_symbol(strval, ':');
	next_colon = search_res.res_str;

	/* delete space embedding */
	delimiter_ph = *next_del; /* we will recover next delimiter with delimiter placeholder */
	del_ptr = next_del;
	for (del_ptr = next_del; isspace(*(del_ptr - 1)); del_ptr--);
	*del_ptr = '\0';

	c = next_colon + 1;
	for (;isspace(*c); c++);

	/* parse element */
	result_ptr = (char *)res_struct + offset;

	element_res = parse_composite_impl(c, field_type, result_ptr, flags, hintmsg);

	/* recovery */
	guc_free(field_type);
	*next_del = delimiter_ph;

	if (IS_STATUS_ERR(element_res))
		goto process_err;

	result.status = PARSER_OK;
	return result;

process_err:
	elog(WARNING, "in field %s", name_state.res_str);
	guc_free(name_state.res_str);
	result.status = PARSER_ERR;
	return result; /* return error parse position */
}


/*
 * Checks that array is correct
 * returns NULL if error, pointer to close symbol if ok
 */
char *check_braces(char* start, const char open, const char close, const char **hintmsg)
{
	parser_res search_res;

	/* check open brace */
	if (*start != open) {
		*hintmsg = gettext_noop("composite object starts with wrong symbol");
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("composite object  starts with wrong symbol: %s", start)));
		return NULL;
	}
	start++;

	/* find close brace */
	search_res = find_same_level_symbol(start, close);
	if (IS_STATUS_OK(search_res))
		return search_res.res_str;
	else if (IS_STATUS_NOT_FOUND(search_res))
	{
		*hintmsg = gettext_noop("composite object starts with wrong symbol");
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("composite object  starts with wrong symbol: %s", start)));
		return NULL;
	}
	return NULL;
}

/*
 * Result contains parse_end and res_int - max index in array
 */
parser_res check_array_syntax(char *start, const char **hintmsg)
{
	char *c = start + 1;
	bool is_empty = false;
	parser_res result = {};

	/* check that open and close braces are correct */
	if ((result.parse_end = check_braces(start, '[', ']', hintmsg)) == NULL)
		goto process_err;

	/* check that array syntax is correct */
	is_empty = is_empty_array(start, result.parse_end);
	if (!is_empty)
	{
		parser_res max_idx_state = get_max_index(c);
		if (IS_STATUS_OK(max_idx_state))
			result.res_int = max_idx_state.res_int;
		else if (IS_STATUS_ERR(max_idx_state))
		{
			*hintmsg = gettext_noop("array has incorrect syntax");
			goto process_err;
		}
	}

	result.status = PARSER_OK;
	return result;

process_err:
	result.status = PARSER_ERR;
	return result;
}

/*
 * Core of array parsing. It used in static and dynamic array parsing
 */
parser_res parse_prepared_array(char *start, const char *array_type, void *res_arr, int flags, const char **hintmsg)
{
	parser_res result = {};
	int i = 0;
	char *c = start;
	c++;
	while (*(c-1) != ']')
	{
		parser_res element_res = parse_array_element(c, array_type, res_arr, i-1, flags, hintmsg);
		if (IS_STATUS_OK(element_res))
			c = element_res.parse_end + 1;
		else if (IS_STATUS_ERR(element_res))
		{
			result.status = PARSER_ERR;
			return result;
		}
		i++;
	}
	c--;

	result.parse_end = c;
	result.status = PARSER_OK;
	return result;
}

/*
 * Parse static array
 */
parser_res parse_static_array(char *strval, const char *array_type, void *res_arr, int flags, const char **hintmsg)
{
	parser_res check_res = {};
	parser_res result = {};
	char *c = strval;
	int arr_size = get_static_array_size(array_type);

	if(arr_size < 0)
		goto process_err;

	/* check array correctness */
	check_res = check_array_syntax(c, hintmsg);
	if(IS_STATUS_OK(check_res))
	{
		result.parse_end = check_res.parse_end;

		/* check that index is correct */
		if(check_res.res_int > arr_size)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
					errmsg("index out of bounds in array: %s", strval));
			goto process_err;
		}
	}
	else if (IS_STATUS_ERR(check_res))
		goto process_err;

	/*parse array elements*/
	return parse_prepared_array(c, array_type, res_arr, flags, hintmsg);

process_err:
	result.status = PARSER_ERR;
	return result;
}


/*
 * Parse dynamic array in [...] representation
 */
parser_res parse_dynamic_array(char *strval, const char *array_type, void *res_arr, int flags, const char **hintmsg)
{
	parser_res result = {};
	char *c = strval;
	int last_arr_len = dynamic_array_size(res_arr);
	int last_arr_mem_size = get_dynamic_array_mem_size_with_length(array_type, last_arr_len);
	int arr_len = 0;
	int max_idx = 0;
	void *new_data = NULL;
	int new_data_mem_size = 0;
	parser_res check_res = {};

	if (last_arr_mem_size < 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
				errmsg("invalid array type: %s", array_type));
		result.status = PARSER_ERR;
		return result;
	}

	/* check array correctness */
	check_res = check_array_syntax(c, hintmsg);
	if(IS_STATUS_OK(check_res))
		max_idx = check_res.res_int;
	else if (IS_STATUS_ERR(check_res))
	{
		result.status = PARSER_ERR;
		return result;
	}
	arr_len = max_idx + 1 > last_arr_len ? max_idx + 1 : last_arr_len;

	/*
	 * To prepare memory compute new size, clone old array
	 */
	new_data_mem_size = get_dynamic_array_mem_size_with_length(array_type, arr_len);
	new_data = guc_malloc(ERROR, new_data_mem_size);
	if (last_arr_mem_size)
		memcpy(new_data, *(void**)res_arr, last_arr_mem_size);

	memset((char *)new_data + last_arr_mem_size, 0, new_data_mem_size - last_arr_mem_size);

	/* replace old snapshot */
	guc_free(*(void **)res_arr);
	*(void **)res_arr = new_data;
	dynamic_array_size(res_arr) = arr_len;

	/*parse array elements*/
	return parse_prepared_array(c, array_type, new_data, flags, hintmsg);
}

/*
 * Find field in structure text representation by name
 */
parser_res find_field(char *start, const char *name)
{
	parser_res result = {};
	char *st = start;
	bool found_field = false;

	while (*st != '}')
	{
		char *field_name = NULL;
		parser_res name_state = get_name(st + 1);
		if (IS_STATUS_OK(name_state))
			field_name = name_state.res_str;
		else if (IS_STATUS_ERR(name_state))
		{
			result.status = PARSER_ERR;
			return result;
		}

		if (!strcmp(field_name,name))
		{
			found_field = true;
			guc_free(field_name);
			break;
		}
		guc_free(field_name);
		st = name_state.parse_end;
	}

	if (found_field)
	{
		result.status = PARSER_OK;
		result.res_str = st + 1;
		return result;
	}

	result.status = PARSER_NOT_FOUND;
	return result;
}

/*
 * Parse dynamic arrray in {data: [...], size: <number>} representation (fields could be shuffled)
 * It checks that array has no excess fields.
 */
parser_res parse_extended_dynamic_array(char *strval, const char *array_type, void *res_arr, int flags, const char **hintmsg)
{
	char *c = strval + 1;
	int last_arr_len = dynamic_array_size(res_arr);
	int last_arr_mem_size = get_dynamic_array_mem_size_with_length(array_type, last_arr_len);
	int arr_len = -1;
	int max_idx = 0;
	void *new_data = NULL;
	int new_data_mem_size = 0;
	char *data_st = NULL;
	char *size_st = NULL;
	int cnt_fields = 0;
	parser_res search_res = {};
	parser_res result = {};
	if (last_arr_mem_size < 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
				errmsg("invalid array type: %s", array_type));
		goto process_err;
	}

	/* check that open and close braces are correct */
	if ((result.parse_end = check_braces(strval, '{', '}', hintmsg)) == NULL)
		goto process_err;

	/* check that array is empty */
	if (is_empty_array(strval, result.parse_end))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
				errmsg("dynamic array hasn't size nor data field"));
		goto process_err;
	}

	/* if array not empty, count fields with delimiters */
	for(char *del = strval; *del != '}'; cnt_fields++)
	{
		parser_res comma_res = find_same_level_symbol(del+1, ',');
		del = comma_res.res_str;
	}
	if (cnt_fields > 2)
		goto process_excess_fields;

	/* check fields data and size */
	search_res = find_field(strval, "size");
	/* parse size from structure field */
	if (IS_STATUS_OK(search_res))
	{
		parser_res element_res;
		struct DynArrTmp dyn_array = {NULL,-1};

		size_st = search_res.res_str;
		element_res = parse_struct_element(size_st, array_type, &dyn_array, flags, hintmsg);
		if (IS_STATUS_OK(element_res))
			arr_len = dyn_array.size;
		else if (IS_STATUS_ERR(element_res))
			goto process_err;
	}
	else if (IS_STATUS_NOT_FOUND(search_res) && cnt_fields > 1)
		goto process_excess_fields;
	else if (IS_STATUS_ERR(search_res))
		goto process_err;

	search_res = find_field(strval, "data");
	if (IS_STATUS_OK(search_res))
		data_st = search_res.res_str;
	else if (IS_STATUS_NOT_FOUND(search_res)) /* if there is no data, we just resize */
	{
		if (cnt_fields > 1)
			goto process_excess_fields;

		/* if arr_len not determined - error */
		if (arr_len < 0)
			{
				ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
					errmsg("dynamic array hasn't size nor data field"));
				goto process_err;
			}

		/* allocate new array */
		new_data_mem_size = get_dynamic_array_mem_size_with_length(array_type, arr_len);
		new_data = guc_malloc(ERROR, new_data_mem_size);

		/* copy data between arrays */
		if (last_arr_mem_size && last_arr_mem_size < new_data_mem_size)
		{
			memcpy(new_data, *(void**)res_arr, last_arr_mem_size);
			memset((char *)new_data + last_arr_mem_size, 0, new_data_mem_size - last_arr_mem_size);
		}
		else if (last_arr_mem_size)
			memcpy(new_data, *(void**)res_arr, new_data_mem_size);

		/* replace old snapshot */
		guc_free(*(void **)res_arr);
		*(void **)res_arr = new_data;
		dynamic_array_size(res_arr) = arr_len;

		goto process_ok;
	}
	else if (IS_STATUS_ERR(search_res))
		goto process_err;


	/* parse data from data field */

	/* check array correctness */
	search_res = find_same_level_symbol(data_st, ':');
	c = search_res.res_str + 1;

	/*go to the start of the nest array*/
	for (;isspace(*c);c++);
	search_res = check_array_syntax(c, hintmsg);
	if (IS_STATUS_OK(search_res))
		max_idx = search_res.res_int;
	else if (IS_STATUS_ERR(search_res))
		goto process_err;

	/*
	 * Dynamic array in extend representation could have 2 fields: size and data
	 * This fields mustn't conflict: maximal index in data must be less than size
	 * If size not determined than length is maximum of (max index + 1) and previous length
	 */
	if (arr_len == -1)
		arr_len = max_idx + 1 > last_arr_len ? max_idx + 1 : last_arr_len;
	else if (arr_len <= max_idx)
	{
		*hintmsg = gettext_noop("array size less than maximum index from data for array");
		ereport( WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
					errmsg("index out of bounds in array: %s", strval));
		goto process_err;
	}

	/*
	 * To prepare memory compute new size, clone old array
	 */
	new_data_mem_size = get_dynamic_array_mem_size_with_length(array_type, arr_len);
	new_data = guc_malloc(ERROR, new_data_mem_size);

	/* if old array was smaller than new, set 0 at new the new position */
	if (last_arr_mem_size && last_arr_mem_size < new_data_mem_size)
	{
		memcpy(new_data, *(void**)res_arr, last_arr_mem_size);
		memset((char *)new_data + last_arr_mem_size, 0, new_data_mem_size - last_arr_mem_size);
	}
	else if (last_arr_mem_size)
		memcpy(new_data, *(void**)res_arr, new_data_mem_size);

	/* replace old snapshot */
	guc_free(*(void **)res_arr);
	*(void **)res_arr = new_data;
	dynamic_array_size(res_arr) = arr_len;

	/*parse array elements*/
	return parse_prepared_array(c, array_type, new_data, flags, hintmsg);
process_ok:
	result.status = PARSER_OK;
	return result;

process_excess_fields:
	*hintmsg = gettext_noop("dynamic array could have only 'data' and 'size' fields");
	ereport(WARNING,
			(errcode(ERRCODE_INVALID_OBJECT_DEFINITION)),
			errmsg("excess fields in dynamic array"));

process_err:
	result.status = PARSER_ERR;
	return result;
}

bool is_atomic_type(const char* type)
{
	if (!strcmp(type,"bool") ||
		!strcmp(type,"int")  ||
		!strcmp(type,"real") ||
		!strcmp(type,"string"))
		return true;
	return false;
}

parser_res parse_atomic_type(char *strval, const char *struct_type, void *result, int flags, const char **hintmsg)
{
	parser_res parser_result = {};
	/* extract value */
	char *prepared_strval = strval;
	char *end = strval;
	for (;*end; end++);
	end--;
	parser_result.parse_end = end;

	/* prepare value if neccessary */
	if (*strval == '\'')
		prepared_strval = DeescapeQuotedString(strval);

	/* set OK status, ruin status in error case */
	parser_result.status = PARSER_OK;

	if (!strcmp(struct_type, "bool"))
	{
		if (!parse_bool(prepared_strval, (bool *)result))
		{
			*hintmsg = gettext_noop("failed to parse bool value, use 'on' and 'off'");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("failed to parse bool value: %s", prepared_strval),
					errhint("use 'on' or 'off'")));
			parser_result.status = PARSER_ERR;
		}
	}
	else if (!strcmp(struct_type, "int"))
	{
		if (!parse_int(prepared_strval, (int *)result, flags, hintmsg))
		{
			*hintmsg = gettext_noop("failed to parse int value, check units");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("failed to parse int value: %s", prepared_strval),
					errhint("check unit, symbols")));
			parser_result.status = PARSER_ERR;
			goto out;
		}
	}
	else if (!strcmp(struct_type, "real"))
	{
		if (!parse_real(prepared_strval, (double *)result, flags, hintmsg))
		{
			*hintmsg = gettext_noop("failed to parse real value, check delimiter");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("failed to parse real value: %s", prepared_strval),
					errhint("use dot to delimit ")));
			parser_result.status = PARSER_ERR;
			goto out;
		}
	}
	else if (!strcmp(struct_type, "string"))
	{
		if (!strcmp(prepared_strval, "nil"))
		{
				*((char **)result) = NULL;
				parser_result.status = PARSER_OK;
		}
		else
		*((char **)result) = guc_strdup(ERROR, prepared_strval);
	}
	else
	{
		*hintmsg = gettext_noop("failed to determine type of simple field");
		parser_result.status = PARSER_ERR;
	}

out:
	if (strval != prepared_strval)
		pfree(prepared_strval);
	return parser_result;
}

/*
 * Parse structure. Each field must have name.
 */
parser_res parse_structure(char *strval, const char *struct_type, void *res_struct, int flags, const char **hintmsg)
{
	parser_res result = {};
	char *c = strval + 1;

	/* process atomic structure types*/
	if (is_atomic_type(struct_type))
		return parse_atomic_type(strval, struct_type, res_struct, flags, hintmsg);
	/* chek that open and close braces are correct */

	if ((result.parse_end = check_braces(strval, '{', '}', hintmsg)) == NULL)
		goto process_err;

	/* go throw fields */
	while (*(c-1) != '}')
	{
		parser_res element_res = parse_struct_element(c, struct_type, res_struct, flags, hintmsg);
		if (IS_STATUS_OK(element_res))
			 c = element_res.parse_end + 1;
		else if (IS_STATUS_ERR(element_res))
			goto process_err;
	}
	c--;

	result.parse_end = c;
	result.status = PARSER_OK;
	return result;

process_err:
	result.status = PARSER_ERR;
	return result;
}


/*
 * Parse composite object. It could be static, dynamic array or structure
 */
parser_res parse_composite_impl(char *value, const char *type, void *result, int flags, const char **hintmsg)
{
	if (is_static_array_type(type))
		return parse_static_array(value, type, result, flags, hintmsg);
	if (is_dynamic_array_type(type))
	{
		if (*value == '{')
			return parse_extended_dynamic_array(value, type, result, flags, hintmsg);
		else
			return parse_dynamic_array(value, type, result, flags, hintmsg);
	}
	return parse_structure(value, type, result, flags, hintmsg);
}

/*
 * Entry point in parsing structure. This function is used to parse composite objects.
 * It allocate memory for the new object and uses parse_compoite_impl.
 * It also is used to parse placeholder patch list
 */
bool parse_composite(const char *value, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg)
{
	int size = 0;
	char *scheme = NULL;
	void *val = NULL;
	parser_res parser_result = {};
	bool check = false;
	*hintmsg = NULL;

	if (is_assignment_list(value))
		return parse_placeholder_patch_list(value, type, result, prev_val, flags, hintmsg);

	size = get_type_size(type);
	scheme = guc_strdup(ERROR, value);
	val = guc_malloc(ERROR, size);
	check = true;

	if (prev_val)
		struct_dup_impl(val, prev_val, type);
	else
		memset(val, 0, size);

	parser_result = parse_composite_impl(scheme, type, val, flags, hintmsg);

	if (IS_STATUS_OK(parser_result))
		*result = val;
	else if (IS_STATUS_ERR(parser_result))
	{
		elog(WARNING, "in composite object: %s", value);
		guc_free(val);
		*result = NULL;
		check = false;
	}
	guc_free(scheme);
	return check;
}


/*
 * Functions examine string and decides that is recovery of placeholder (assignment list) or structure definition
 * assignemt list has signature: <path>=<value>;...;<path>=<value>;
 * maybe we should use signature: ;<path>=<value>;...;<path>=<value> when we will check first simbol, but this form is unusual
 */
 bool is_assignment_list(const char *value) {
	return ';' == value[strlen(value) - 1];
}


/*
 * Placeholder patch list is used to support incremental semantic
 * for composite types placeholders.
 * Function parses assignment list in the way:
 * 1. slpit assignement by ';'
 * 2. assign patch
 */
bool parse_placeholder_patch_list(const char *value, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg) {
	char *strval = guc_strdup(ERROR, value);
	char *cur_patch = strval;
	void *last_value = struct_dup(prev_val, type);

	/* go throw list of patches delimited and ended with ';' */
	while(*(cur_patch))
	{
		void *next_value;
		char *next_del;
		parser_res search_res = find_same_level_symbol(cur_patch, ';');
		next_del = search_res.res_str;
		*next_del = '\0';

		if (!parse_composite(cur_patch, type, &next_value, last_value, flags, hintmsg))
		{
			guc_free(strval);
			*result = last_value;
			return false;
		}

		guc_free(last_value);
		last_value = next_value;
		cur_patch = next_del + 1;
	}
	guc_free(strval);
	*result = last_value;

	return true;
}

/*
 * Check that composite type is static array
 */
bool is_static_array_type(const char *type_name)
{
	char *size_str_begin = strchr(type_name, '[');
	if (!size_str_begin)
		return false;

	if (!strchr(type_name, ']'))
		return false;

	if (size_str_begin && atoi(size_str_begin + 1) > 0)
		return true;

	return false;
}

/*
 * Check that composite type is dynamic array
 */
bool is_dynamic_array_type(const char *type_name)
{
	char *size_str_begin = strchr(type_name, '[');
	if (!size_str_begin)
		return false;

	if (!strchr(type_name, ']'))
		return false;

	if (size_str_begin && atoi(size_str_begin + 1) <= 0)
		return true;
	return false;
}


/*
 * Gets size of static array from type definition
 */
int get_static_array_size(const char * type_name)
{
	char * size_str_begin = strchr(type_name, '[');
	if (size_str_begin == NULL)
		return -1;
	return atoi(size_str_begin + 1);
}


/*
 * Gets type of array elements (works for static and dynamic arrays)
 */
char *get_array_basic_type(const char * array_type)
{
	ptrdiff_t first_part_len;
	ptrdiff_t second_part_len;
	size_t type_len;
	char *type_name;
	const char *brace_close;
	const char *brace_open = strchr(array_type, '[');
	if (!brace_open)
		return NULL;

	brace_close = strchr(brace_open, ']');
	if (!brace_open || !brace_close)
		return NULL;

	first_part_len = brace_open - array_type;
	second_part_len = strchr(brace_close, '\0') - brace_close - 1;
	type_len = first_part_len + second_part_len;

	type_name = guc_malloc(ERROR, (type_len + 1) * sizeof(char));
	strncpy(type_name, array_type, first_part_len);
	strncpy(type_name + first_part_len, brace_close + 1, second_part_len);
	type_name[type_len] = 0;
	return type_name;
}


/*
 * Gets type definition struct from guc_types_hashtab by type name
 */
struct type_definition *get_type_definition(const char *type_name)
{
	struct type_definition *definition;
	bool found = false;
	OptionTypeHashEntry *type_hentry = NULL;
	type_hentry = (OptionTypeHashEntry *)hash_search(guc_types_hashtab, &type_name, HASH_FIND, &found);
	if (found) {
		definition = type_hentry->definition;
		return definition;
	}
	return NULL;
}


/*
 * Returns index as int (-1 if index is invalid)
 */
int canonize_idx(const char * field)
{
	int field_idx = -1;
	//check that first significant character is digit (because atoi returns 0 in incorrect cases)
	const char *cp = field;
	for (; *cp; cp++){
		if(*cp != ' ' && *cp != '\t' && *cp != '\v' && *cp != '\n')
			break;
	}

	if (*cp < '0' || *cp > '9')
		return -1;

	field_idx = atoi(field);
	return field_idx;
}


/*
 * Gets static array size with computing
 */
static int get_array_mem_size(const char *type_name)
{
	int array_size;
	char *basic_type = get_array_basic_type(type_name);

	int element_offset = get_type_offset(basic_type);
	int element_size = get_type_size(basic_type);

	if (element_offset < 0 || element_size < 0)
		return -1;

	array_size = get_static_array_size(type_name) * (element_size + (element_size % element_offset)); // for dynamic arrays
	guc_free(basic_type);
	return array_size;
}

/*
 * Gets dynamic array size
 * Casual way for using: see dynamic array size in next int field, after use that function
 */
static int get_dynamic_array_mem_size(const char *type_name, const void *structp)
{
	int array_length = dynamic_array_size(structp);
	return get_dynamic_array_mem_size_with_length(type_name, array_length);
}

/*
 * Gets dynamic array size
 * Casual way for using: see dynamic array size in next int field, after use that function
 */
static int get_dynamic_array_mem_size_with_length(const char *type_name, const int length)
{
	int array_size;
	int element_size;
	int element_offset;
	char *basic_type = get_array_basic_type(type_name);
	if (!basic_type)
		return -1;

	element_offset = get_type_offset(basic_type);
	element_size = get_type_size(basic_type);
	guc_free(basic_type);

	if (element_offset < 0 || element_size < 0)
		return -1;

	array_size = length * (element_size + (element_size % element_offset));
	return array_size;
}


/*
 * Gets structure size from type definition
 */
static int get_struct_size(const char *type_name)
{
	struct type_definition *struct_type = NULL;
	if ((struct_type = get_type_definition(type_name)))
		return struct_type->type_size;

	return -1;
}


/*
 * Gets size of any composite type
 */
int get_type_size(const char* type_name )
{
	if (!type_name)
		return -1;

	/*
	 * Dynamic array in struct that is 2 fields: pointer, int
	 * Therefore size equals size of pointer + size of int
	 */
	if (is_dynamic_array_type(type_name))
		return sizeof(void *) * 2; /* sizeof(int) <= sizeof(ptr) */

	if (is_static_array_type(type_name))
		return get_array_mem_size(type_name);

	return get_struct_size(type_name);
}


/*
 * Gets offset of static array by C rules for type offsets
 */
static int get_array_offset(const char *type_name)
{
	int element_offset;
	char *basic_type = get_array_basic_type(type_name);
	if (!basic_type)
		return -1;

	element_offset = get_type_offset(basic_type);
	if (element_offset < 0)
		return -1;

	guc_free(basic_type);
	return element_offset;
}


/*
 * Gets offset of structure by C rules for type offsets
 */
static int get_struct_offset(const char *type_name)
{
	struct type_definition *struct_type = NULL;
	if (!(struct_type = get_type_definition(type_name)))
		return -1;

	return struct_type->offset;
}


/*
 * Gets offset of any type by C rules for type offsets
 */
static int get_type_offset(const char *type_name)
{
	if (!type_name)
		return -1;

	/*
	 * Dynamic array in struct that is 2 fields: pointer, int
	 * Therefore offset of pointer, int and offset of the pointer are same
	 */
	if (is_dynamic_array_type(type_name))
		return sizeof(void *);

	if (is_static_array_type(type_name))
		return get_array_offset(type_name);

	return get_struct_offset(type_name);
}


/*
 * Gets type of static array's element
 */
char *get_static_aray_element_type(const char *type_name, const char *field)
{
	if (canonize_idx(field) < 0)
		return NULL;

	return get_array_basic_type(type_name);
}

/*
 * Gets type of dynamic array's element
 */
char *get_dynamic_array_element_type(const char *type_name, const char *field, const void *structure)
{
	int index = -1;
	int length = dynamic_array_size(structure);
	if (!structure)
		return NULL;

	if (((index = canonize_idx(field)) < 0) || index >= length)
		return NULL;

	return get_array_basic_type(type_name);
}


/*
 * Gets type of structure's field
 */
char *get_struct_field_type(const char *type_name, const char *field)
{
	struct type_definition *struct_type = NULL;
	if (!(struct_type = get_type_definition(type_name)))
		return NULL;

	for (int i = 0; i < struct_type->cnt_fields; i++)
		if (!strcmp(field,struct_type->fields[i].name))
			return guc_strdup(ERROR, struct_type->fields[i].type);

	return NULL;
}


/*
 * Gets type of field of any composite type
 * field - string representation of field name or index (in array case)
 * Attention: this function does not check index for dynamic arrays
 */
char *get_field_type_name(const char *type_name, const char *field)
{
	if (!type_name || !field)
		return NULL;

	/*
	 * Each dynamic array has hidden fields: data and size.
	 * data - content of an array, so that field is idempotent, it has the same
	 * dynamic array type
	 * size - size of an array, has type int
	 */
	if (is_dynamic_array_type(type_name))
	{
		if (strcmp(field, "size") == 0)
			return	guc_strdup(ERROR, "int");
		if (strcmp(field, "data") == 0)
			return	guc_strdup(ERROR, type_name);
	}

	if (is_static_array_type(type_name) || is_dynamic_array_type(type_name))
		return get_array_basic_type(type_name);

	return get_struct_field_type(type_name, field);
}


/*
 * Gets offset of element of array by int index
 */
static int get_element_offset_with_index(const char *type_name, int index)
{
	int rest;
	int element_size;
	int element_offset;
	char *basic_type = get_array_basic_type(type_name);
	if (!basic_type)
		return -1;

	element_offset = get_type_offset(basic_type);
	element_size = get_type_size(basic_type);
	guc_free(basic_type);
	if (element_offset < 0 || element_size < 0)
		return -1;

	rest = element_size % element_offset;
	return (element_size + rest) * index; // we need rest for dynamic array
}

/*
 * Gets offset of element of an array
 */
static int get_array_element_offset(const char *type_name, const char *field)
{
	int field_idx = -1;
	if ((field_idx = canonize_idx(field)) < 0)
		return -1;

	return get_element_offset_with_index(type_name, field_idx);
}


/*
 * Gets offset of field of any composite type
 * Attention: this function couldn't check length of dynamic array
 */
static int get_struct_field_offset(const char *type_name, const char *field)
{
	struct type_definition *struct_type = NULL;
	if (!(struct_type = get_type_definition(type_name)))
		return -1;

	for (int i = 0, total_offset = 0; i < struct_type->cnt_fields; ++i)
	{
		int increment;
		int local_off = get_type_offset(struct_type->fields[i].type);
		if (local_off < 0)
			return -1;

		if (total_offset % local_off != 0)
			total_offset += local_off - total_offset % local_off;

		if (!strcmp(struct_type->fields[i].name, field))
			return total_offset;

		increment = get_type_size(struct_type->fields[i].type);
		total_offset += increment;
	}
	return -1;
}

/*
 * Gets offset of field of any composite type
 * Attentions:
 * 1) This function couldn't check length of dynamic array
 * 2) For dynamic arrays behavior of function might be so surprising.
 *    Fields "data" and "size" have offsets 0 and sizeof(ptr) respectively
 *    and start pointer is pointer to "data" (start of meta information).
 *    However for elements of array offset computing in the same way as for
 *    static array. But start pointer is dereferenced "data" field
 *    (start of array elements) in this case.
 */
static int get_field_offset(const char * type_name, const char *field) {
	if (!type_name || !field)
		return -1;

	/* extended dynamic arrya case */
	if (is_dynamic_array_type(type_name))
	{
		if (!strcmp(field, "data"))
			return 0;
		else if (!strcmp(field, "size"))
			return sizeof(void *);
	}

	if (is_static_array_type(type_name) || is_dynamic_array_type(type_name))
		return get_array_element_offset(type_name, field);

	return get_struct_field_offset(type_name, field);
}


/*
 * Initializes composite type:
 * Fills meta information in type_definition structure
 */
void init_type_definition(struct type_definition *definition) {
	const char *def_del = STRUCT_FIELDS_DELIMETER, *word_del = " \t\n\v";
	int max_offset = 0;
	int count_fields = 0;
	struct_field *fields = NULL; /* meta about fields */
	char *signature_saveptr;
	char *field_def_saveptr;
	char *signature,*field_def;
	char *field_def_token;
	char *word_token;
	char *signature_buffer;
	int  curr_offset = 0;
	int  i;

	/* count fields in signature */
	const char *sym = definition->signature;
	if (!sym || !*sym) {
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("signature of \"%s\" type is empty", definition->type));
		return;
	}

	count_fields = 1;
	while (*sym) {
		if (*sym == def_del[0])
			count_fields++;
		sym++;
	}

	/* allocate structures for field definitions */
	fields = (struct_field *)guc_malloc(ERROR, count_fields * sizeof(struct_field));

	/* parse signature */

	signature = guc_strdup(ERROR, definition->signature);
	signature_buffer = signature;

	/* parse sequence of structure field definitions */
	for (i = 0; ; i++, signature = NULL) /* signature = NULL for strtok_r on the next string */
	{
		int word_cnt = 0;
		field_def_token = strtok_r(signature, def_del, &signature_saveptr);
		if (!field_def_token)
			break;

		/*
		 * Parse field definition
		 * First word is a type, second is a name of field.
		 * Definitions separated with STRUCT_FIELDS_DELIMETER
		 */
		for (field_def = field_def_token; ; field_def = NULL) /* field_def = NULL for strtok_r on the next string */
		{
			word_token = strtok_r(field_def, word_del, &field_def_saveptr);

			if (!word_token) {
				if (word_cnt != 2) {
					ereport(ERROR,
							errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("wrong field definition: \"%s\" in definition of type \"%s\"",
								field_def_token, definition->type));
					goto out;
				}
				break;
			}

			word_token = guc_strdup(ERROR, word_token);

			/* parse field type */
			if (word_cnt == 0)
			{
				int type_offset = get_type_offset(word_token);
				int type_size = get_type_size(word_token);

				if (type_offset < 0 || type_size < 0) {
					ereport(ERROR,
							errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("wrong type \"%s\"is used in field definition: \"%s\" in definition of type \"%s\"",
								word_token, field_def_token, definition->type));
					goto out;
				}

				fields[i].type = word_token;

				/* structure offset = max offset of field offsets */
				if (type_offset > max_offset)
					max_offset = type_offset;

				/* field offset in structure % field type offset = 0 */
				if (curr_offset % type_offset != 0)
					curr_offset += type_offset - curr_offset % type_offset;

				curr_offset += type_size;
			}
			else if (word_cnt == 1) /* parse field name */
				fields[i].name = word_token;
			else
			{
				ereport(ERROR,
							errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("wrong field definition: \"%s\" in definition of type \"%s\"",
								 field_def_token, definition->type));
				goto out;
			}
			word_cnt++;
		}
	}

	/* structure size % structure offset = 0 */
	if (curr_offset % max_offset != 0)
		curr_offset += max_offset - curr_offset % max_offset;

	definition->offset = max_offset;
	definition->type_size = curr_offset;
	definition->cnt_fields = count_fields;
	definition->fields = fields;
	fields = NULL;
	word_token = NULL;
out:
	guc_free(fields);
	guc_free(word_token);
	guc_free(signature_buffer);
	return;
}


/*
 * Gets type of field on any nest level
 */
char *get_nest_field_type(const char *struct_type, const char *field_path)
{
	char *path;
	char *type;
	char *cur_field;

	if (!struct_type || !field_path)
		return NULL;

	path = guc_strdup(ERROR, field_path);
	type = guc_strdup(ERROR, struct_type);
	cur_field = tokenize_field_path(path);
	cur_field = tokenize_field_path(NULL); /* skip name of structure name */

	/*Follow the path of the field*/
	while (cur_field && type)
	{
		char *next_type = get_field_type_name(type, cur_field);
		guc_free(type);
		type = next_type;
		cur_field = tokenize_field_path(NULL);
	}
	guc_free(path);

	return type;
}


void *get_nest_field_ptr(const void *structure, const char *struct_type, const char *field_path)
{
	char *path;
	char *type;
	char *cur_field;
	char *cur_ptr;

	if (!structure || !field_path || !struct_type)
		return NULL;

	path = guc_strdup(ERROR, field_path);
	type = guc_strdup(ERROR, struct_type);

	cur_field = tokenize_field_path(path);
	cur_field = tokenize_field_path(NULL); /* skip name of structure */
	cur_ptr = (char *)structure;

	while (cur_field && type)
	{
		char *next_type;
		int local_offset;
		/* go to memory of dynamic array */
		if (is_dynamic_array_type(type) &&
			strcmp(cur_field,"data") != 0 &&
			strcmp(cur_field, "size") != 0)
			cur_ptr = *((char **)cur_ptr);

		local_offset = get_field_offset(type, cur_field);
		if (local_offset < 0)
		{
			cur_ptr = NULL;
			break;
		}
		cur_ptr += local_offset;

		next_type = get_field_type_name(type, cur_field);

		guc_free(type);
		type = next_type;
		cur_field = tokenize_field_path(NULL);
	}
	guc_free(path);
	guc_free(type);

	return cur_ptr;
}


/*
 *  Converts object of static array type to string
 */
char *static_array_to_str(const void *structp, const char *type, bool serialize)
{
	char *glue_arr = NULL;
	char *glue_arr_term = NULL;
	int total_size = 3; /* outer braces and \0 */
	int array_size = get_static_array_size(type);
	char **parts;
	int i = 0;
	char *element_type = get_array_basic_type(type);
	if (!element_type || array_size < 0)
		return NULL;

	/* allocate string for each field, fill it, and then concatenate */
	parts = (char **)guc_malloc(ERROR, array_size * sizeof(char *));

	/*recursive call for each element of array */
	for (; i < array_size; i++)
	{
		int offset = get_element_offset_with_index(type, i);
		if (offset < 0)
			goto out;
		parts[i] = struct_to_str((char *)structp + offset, element_type, serialize);
		if (!parts[i])
			goto out;
		total_size += strlen(parts[i]) + 2;
	}
	i = 0;

	/* concatenate part strings */
	glue_arr = (char *)guc_malloc(ERROR, total_size * sizeof(char));
	sprintf(glue_arr, "[");
	glue_arr_term = glue_arr + 1;

	for (int j = 0; j < array_size - 1; j++)
	{
		sprintf(glue_arr_term, "%s, ", parts[j]);
		glue_arr_term += strlen(parts[j]) + 2;
		guc_free(parts[j]);
	}
	sprintf(glue_arr_term, "%s]", parts[array_size - 1]);
	glue_arr_term += strlen(parts[array_size - 1]) + 1;
	guc_free(parts[array_size - 1]);

out:
	/* free all previous parts and go out */
	for (int j = 0; j < i; j++)
		guc_free(parts[j]);
	guc_free(element_type);
	guc_free(parts);
	return glue_arr;
}


/*
 * Converts object of dynamic array type to string
 * structptr is a pointer to pointer to allocated memory
 * (after pointer should be array's size)
 */
char *dynamic_array_to_str(const void *structp, const char *type, bool serialize)
{
	void *datap;
	bool is_expand;
	int total_size;
	char **parts;
	char *glue_arr = NULL;
	char *glue_arr_term = NULL;
	int i = 0;
	int array_size = dynamic_array_size(structp);
	char *element_type = get_array_basic_type(type);
	if (!element_type)
		return NULL;

	/* allocate string for each field, fill it, and then concatenate */
	parts = (char **)guc_malloc(ERROR, array_size * sizeof(char *));
	total_size = 3; /* outer braces and \0 */
	is_expand = array_size >= expand_array_view_thd; /* expand_array_view_thd - global variable (GUC) */

	datap = *((void **)structp);

	if (is_expand)
		total_size += 30; /* max len of decimal int32 + facade */

	/*recursive call for each element of array*/
	for (; i < array_size; i++)
	{
		int offset = get_element_offset_with_index(type, i);
		if (offset < 0)
			goto out;
		parts[i] = struct_to_str((char *)datap + offset, element_type, serialize);
		if (!parts[i])
			goto out;
		total_size += strlen(parts[i]) + 2;
	}
	i = 0;

	/* concatenate part strings */
	glue_arr = (char *)guc_malloc(ERROR, total_size * sizeof(char));

	if (is_expand)
		sprintf(glue_arr,"{size: %d, data: [", array_size);
	else
		sprintf(glue_arr,"[");

	glue_arr_term = glue_arr + strlen(glue_arr);
	for (int j = 0; j < array_size-1; j++)
	{
		sprintf(glue_arr_term, "%s, ", parts[j]);
		glue_arr_term += strlen(parts[j]) + 2;
		guc_free(parts[j]);
	}
	sprintf(glue_arr_term, "%s]", parts[array_size - 1]);
	glue_arr_term += strlen(parts[array_size - 1]) + 1;
	guc_free(parts[array_size - 1]);

	if (is_expand)
		sprintf(glue_arr_term++, "}");

out:
	/* free all previous parts and go out */
	for (int j = 0; j < i; j++)
		guc_free(parts[j]);
	guc_free(element_type);
	guc_free(parts);
	return glue_arr;
}

char *atomic_to_str(const void *structp, const char *type, bool serialize)
{
	char *buf;
	char *quoted;

	if (!strcmp(type, "bool"))
	{
		buf = (char *)guc_malloc(ERROR, 6 * sizeof(char));
		if (*(bool *)structp)
			sprintf(buf, "%s", "true");
		else
			sprintf(buf, "%s", "false");
	}
	else if (!strcmp(type, "int"))
	{
		buf = (char *)guc_malloc(ERROR, 12 * sizeof(char)); /* max length of decimal number int32 */
		sprintf(buf, "%d", *(int *)structp);
	}
	else if (!strcmp(type, "real"))
	{
		buf = (char *)guc_malloc(ERROR, (DBL_MAX_10_EXP + 3) * sizeof(char)); /* max length of decimal float */
		sprintf(buf, "%lf", *(double *)structp);
	}
	else if (!strcmp(type, "string"))
	{
		if (*(char **)structp == NULL)
			buf = guc_strdup(ERROR, "nil");
		else
		{
			/* escape quotes only in serialize case */
			if (serialize)
			{
				char *escaped = escape_single_quotes_ascii(*(char **)structp);
				buf = guc_strdup(ERROR, escaped);
				free(escaped);
			}
			else
				buf = guc_strdup(ERROR, *(char **)structp);
		}
	}
	else
		return NULL;

	/*
	 * add apostrophes:
	 * In serialize case add apostrophes for each type
	 * Else add apostrophes only for strings
	 */
	if (serialize || (!strcmp(type, "string") && strcmp(buf,"nil")))
	{
		quoted = (char *)guc_malloc(ERROR, (strlen(buf) + 3) * sizeof(char));
		sprintf(quoted,"\'%s\'", buf);
		guc_free(buf);
	}
	else
		quoted = buf;

	return quoted;
}

/*
 *  Converts structure to string
 */
char *structure_to_str(const void *structp, const char *type, bool serialize)
{
	struct type_definition *struct_type;
	char **parts = NULL;
	int total_size = 0;
	char *glue_struct = NULL;
	char *glue_struct_term = NULL;
	int cnt_fields = 0;
	int i = 0;
	/*check built-in types*/
	if (is_atomic_type(type))
		return atomic_to_str(structp, type, serialize);

	/* standard algorithm of serialize structure to string */

	/* check type */
	struct_type = NULL;
	if (!(struct_type = get_type_definition(type)))
		return NULL;

	cnt_fields = struct_type->cnt_fields;

	/* allocate string for each field, fill it, and then concatenate */
	parts = (char **)guc_malloc(ERROR, cnt_fields * sizeof(char *));
	total_size = 3;   /* outer braces and \0 */

	/* recurse call for fields */
	for (; i < cnt_fields; i++)
	{
		void *sptr;
		int offset = get_field_offset(struct_type->type,
									 struct_type->fields[i].name);
		if (offset < 0)
			goto out;

		sptr = (char *)structp + offset;
		parts[i] = struct_to_str(sptr, struct_type->fields[i].type, serialize);
		if (!parts[i])
			goto out;
		/* strlen(name) + 4 = strlen(", <name>: ") */
		total_size += strlen(parts[i]) + strlen(struct_type->fields[i].name) + 4;
	}
	i = 0;

	/* concatenate strings */
	glue_struct = (char *)guc_malloc(ERROR, total_size * sizeof(char));
	sprintf(glue_struct, "{");
	glue_struct_term = glue_struct + 1;

	for (int j = 0; j < cnt_fields - 1; j++)
	{
		sprintf(glue_struct_term, "%s: %s, ", struct_type->fields[j].name, parts[j]);
		glue_struct_term += strlen(struct_type->fields[j].name) + strlen(parts[j]) + 4;
		guc_free(parts[j]);
	}

	sprintf(glue_struct_term, "%s: %s}", struct_type->fields[cnt_fields - 1].name, parts[cnt_fields - 1]);
	glue_struct_term += strlen(struct_type->fields[cnt_fields - 1].name) + strlen(parts[cnt_fields - 1]) + 3;

	guc_free(parts[cnt_fields - 1]);
out:
	/* free all previous parts and go out */
	for (int j = 0; j < i; j++)
		guc_free(parts[j]);
	guc_free(parts);
	return glue_struct;
}


/*
 *  Converts object of composite type to string
 */
char *struct_to_str(const void *structp, const char *type, bool serialize)
{
	if (is_static_array_type(type))
		return static_array_to_str(structp, type, serialize);
	if (is_dynamic_array_type(type))
		return dynamic_array_to_str(structp, type, serialize);
	return structure_to_str(structp, type, serialize);
}

char *normalize_struct_value(const char *name, const char *value)
{
	/*
	 * Composite value couldn't be wrapped in quotes
	 * atomic types must be escaped and wrapped in quotes
	 * All names related to composite values ended with "->"
	 */
	bool is_composite = (name[strlen(name) - 2] == '-' && name[strlen(name) - 1] == '>');
	char *prepared_val;
	char *str_val;

	/*
		* Each value that goes throw this function went throw
		* parser before. If value is atomic, it was
		* deescaped, else (if value is composite) it wasn't.
		* Function parse_composite always deescapes atomic values.
		* Therefore we must escape atomic values for parse_composite
		*/
	if (!is_composite)
	{
		char *escaped = escape_single_quotes_ascii(value);
		/* escape */
		prepared_val = guc_malloc(ERROR, strlen(escaped) + 3);
		sprintf(prepared_val, "\'%s\'",escaped);
		free(escaped);
	}
	else
		prepared_val = (char *)value; /* be careful */

	str_val = convert_path_composite(name, prepared_val);

	if (prepared_val != value)
		guc_free(prepared_val);

	return str_val;
}


/*
 * Gets size of serialized array
 */
static Size get_len_serialized_array(const void *structp, const char *type)
{
	char *element_type = get_array_basic_type(type);
	int total_size = 3;
	void *datap = NULL;
	int array_size = 0;
	if (is_dynamic_array_type(type))
	{
		array_size = *((int *) structp + 2); //fix for 32-bit systems
		datap = *((void**)structp);
	}
	else
	{
		array_size = get_static_array_size(type);
		datap = (void *)structp;
	}

	/* compute length for first element*/
	for (int i = 0; i < array_size; i++)
	{
		int element_len = get_length_struct_str((char *)datap + get_element_offset_with_index(type, i), element_type) + 2;
		total_size += element_len;
	}
	guc_free(element_type);
	return total_size;
}

/*
 * Gets size of serialized structure
 */
static Size get_len_serialized_struct(const void *structp, const char *type)
{
	struct type_definition *struct_type = NULL;
	int total_size = 3;
	/* check built-in types */
	if (!strcmp(type,"bool"))
		return  6;
	else if (!strcmp(type,"int"))
	{
		if (*(int *)structp < 100)
			return 4;
		return 11;
	}
	else if (!strcmp(type,"real"))
		return 1 + 1 + 1 + REALTYPE_PRECISION + 5;
	else if (!strcmp(type,"string"))
	{
		if (*(char **)structp)
			return strlen(*(char **)structp);
		return 5;
	}

	/* compute length for composite structure */

	/* check type */
	if (!(struct_type = get_type_definition(type)))
		return 0;

	/* compute length recursive for each field*/
	for (int i = 0; i < struct_type->cnt_fields; i++)
		total_size += get_length_struct_str((char *)structp + get_field_offset(struct_type->type, struct_type->fields[i].name),
											struct_type->fields[i].type) + 2;

	return total_size;
}


/*
 * Gets size of serialized composite object
 */
size_t get_length_struct_str(const void *structp, const char *type)
{
	if (is_static_array_type(type) || is_dynamic_array_type(type))
		return get_len_serialized_array(structp, type);
	return get_len_serialized_struct(structp, type);
}


/*
 * Convert path to field and value to part of the composite type
 * Path starts with name of guc option
 */
char *convert_path_composite (const char *field_path, const char *value)
{
	char *path = guc_strdup(ERROR, field_path);
	char *cur_field = tokenize_field_path(path);
	char* prefix = guc_strdup(ERROR, "");
	char *suffix = guc_strdup(ERROR, "");
	char *result;

	/* skip guc name */
	cur_field = tokenize_field_path(NULL);

	/* for each step in path generate derived braces and name of field*/
	while(cur_field)
	{
		int prefix_len = strlen(prefix);
		int suffix_len = strlen(suffix);

		char *next_prefix = guc_malloc(ERROR, prefix_len + 3 + strlen(cur_field) + 1); /* 3 for "[: ", 1 for '\0' */
		char *next_suffix = guc_malloc(ERROR, suffix_len + 2);

		sprintf(next_prefix, "%s", prefix);
		/* define array or structure */
		if (isdigit(cur_field[0]))
		{
			sprintf(next_prefix + prefix_len, "[");
			sprintf(next_suffix, "]");
		}
		else
		{
			sprintf(next_prefix + prefix_len, "{");
			sprintf(next_suffix, "}");
		}
		sprintf(next_prefix + prefix_len + 1, "%s: ", cur_field);
		sprintf(next_suffix + 1, "%s", suffix);

		guc_free(prefix);
		guc_free(suffix);

		prefix = next_prefix;
		suffix = next_suffix;

		cur_field = tokenize_field_path(NULL);
	}

	/* construct result from prefix, suffix and value */
	result = guc_malloc(ERROR, strlen(prefix) + strlen(value) + strlen(suffix) + 1);
	sprintf(result, "%s%s%s", prefix, value, suffix);

	guc_free(prefix);
	guc_free(suffix);
	return result;
}


/*
 * Duplicate static array in GUC memoru context
 */
void static_array_duplicate(void *dest_struct, const void *src_struct, const char *type)
{
	const char *basic_type = get_array_basic_type(type);
	int arr_size = get_static_array_size(type);

	/* recursive duplicate array elements*/
	for (int i = 0; i < arr_size; i++)
	{
		struct_dup_impl((char *)dest_struct + get_element_offset_with_index(type, i),
		 (char *)src_struct + get_element_offset_with_index(type, i), basic_type);
	}
}

/*
 * Duplicate dynamic array in GUC memoru context
 * Beware! src_struct - pointer to pointer to allocated data, after that size is placed
 */
void dynamic_array_duplicate(void *dest_struct, const void *src_struct, const char *type)
{
	void *datap;
	void **dstpp;
	void *dstp;
	const char *basic_type = get_array_basic_type(type);
	int arr_mem_size = get_dynamic_array_mem_size(type, src_struct);
	int arr_size = dynamic_array_size(src_struct);
	if (!arr_size)
	{
		*(void **)dest_struct = NULL;
		*((void **)dest_struct + 1) = NULL;
		return;
	}
	datap = *((void **)src_struct);
	dstpp = (void **)dest_struct;
	*dstpp = guc_malloc(ERROR, arr_mem_size * sizeof(char));
	dstp = *dstpp;

	/* recursive duplicate array elements*/
	for (int i = 0; i < arr_size; i++)
	{
		struct_dup_impl((char *)dstp + get_element_offset_with_index(type, i),
		 (char *)datap + get_element_offset_with_index(type, i), basic_type);
	}

	/*duplicate array size*/
	*((int*)dest_struct + 2) = arr_size;
}


/*
 * Duplicate structure in GUC memory context
 */
void struct_duplicate(void *dest_struct, const void *src_struct, const char *type)
{
	struct type_definition *struct_type = NULL;
	if (!(struct_type = get_type_definition(type)))
		return;

	/* process atomic types like int, real, etc*/
	if (struct_type->cnt_fields == 0)
	{
		/* string require to allocate new memory for duplicate */
		if (!strcmp(type,"string"))
		{
			if (*(char **)src_struct)
				*(char **)dest_struct = guc_strdup(ERROR, *(char **)src_struct);
			else
				*(char **)dest_struct = NULL;
			return;
		}

		memcpy(dest_struct, src_struct, struct_type->type_size);
		return;
	}

	/* recursive process each field of structure */
	for (int i = 0; i < struct_type->cnt_fields; i++)
	{
		int field_offset = get_field_offset(type, struct_type->fields[i].name);
		struct_dup_impl((char *)dest_struct + field_offset,
						(char *)src_struct + field_offset,
						struct_type->fields[i].type);
	}
}


/*
 * Recursive implementation of duplicate composite object in GUC memory context
 */
void struct_dup_impl(void *dest_struct, const void *src_struct, const char *type)
{
	if (is_static_array_type(type))
		return static_array_duplicate(dest_struct, src_struct, type);
	if (is_dynamic_array_type(type))
		return dynamic_array_duplicate(dest_struct, src_struct, type);
	return struct_duplicate(dest_struct, src_struct, type);
}

/*
 * Duplicate composite object in GUC memory context
 */
void *struct_dup(const void *structp, const char *type) {
	int struct_size;
	void *duplicate;

	if (!structp)
		return NULL;

	struct_size = get_type_size(type);
	duplicate = guc_malloc(ERROR, struct_size);
	/* recursive bypass and searching string */
	struct_dup_impl(duplicate, structp, type);
	return duplicate;
}


/*
 * Compare array datas
 */
int array_data_cmp(const void *first, const void *second, const char *type, int size)
{
	const char *base_type = get_array_basic_type(type);
	int base_type_size = get_type_size(base_type);
	int res = 0;
	/* recursive compare each element*/
	for (int i = 0; i < size; i++) {
		res = struct_cmp((char *)first + base_type_size * i, (char *)second + base_type_size * i, base_type);
		if (res)
			break;
	}
	return res;
}

/*
 * Compare dynamic arrays
 */
int dynamic_array_cmp(const void *first, const void *second, const char *type)
{
	void *first_data = *((void **)first);
	void *second_data = *((void **)second);

	int first_size = dynamic_array_size(first);
	int second_size = dynamic_array_size(second);

	int cmp = 0;
	if ((cmp = first_size - second_size))
		return cmp;

	return array_data_cmp(first_data, second_data, type, first_size);
}

/*
 * Compare structures
 */
int structure_cmp(const void *first, const void *second, const char *type)
{
	int res;
	/* check type */
	struct type_definition *struct_type = NULL;
	if (!(struct_type = get_type_definition(type)))
		return 2; /* error code */

	/* check atomic types like int, real, etc */
	if (struct_type->cnt_fields == 0)
	{
		/*compare string with strcmp, not pointers!*/
		res = 0;
		if (!strcmp(type,"string"))
		{
			if (!*(char **)first && !*(char **)second)
				return 0;
			if (!*(char **)first)
				return -1;
			if (!*(char **)second)
				return 1;
			res = strcmp(*(char **)first, *(char **)second);
		}
		else if (!strcmp(type, "bool"))
			res = *(bool *)first - *(bool *)second;
		else if (!strcmp(type, "int"))
			res = *(int *)first - *(int *)second;
		else if (!strcmp(type, "real")){
			double res = *(double *)first - *(double *)second;
			if (res == 0)
				return 0;
			if (res > 0)
				return 1;
			return -1;
		}
		else
			return 2;

		if (res == 0)
			return 0;
		if (res > 0)
			return 1;
		return -1;
	}

	/* recursive comparison of fields*/
	res = 0;
	for (int i = 0 ; i < struct_type->cnt_fields; i++) {
		int field_offset = get_field_offset(type, struct_type->fields[i].name);
		res = struct_cmp((char *)first + field_offset, (char *)second + field_offset, struct_type->fields[i].type);
		if (res)
			break;
	}
	return res;
}


/*
 * Comparison of two composite objects
 */
int struct_cmp(const void *first, const void *second, const char *type)
{
	if (is_static_array_type(type))
		return array_data_cmp(first, second, type, get_static_array_size(type));
	if (is_dynamic_array_type(type))
		return dynamic_array_cmp(first, second, type);
	return structure_cmp(first, second, type);
}


/*
 * Frees all allocated auxilary memory in static array
 */
void free_aux_mem_stat_arr(void *delptr, const char *type)
{
	const char *base_type = get_array_basic_type(type);
	int arr_size = get_static_array_size(type);

	/* recursive free in each element of array*/
	for (int i = 0; i < arr_size; i++)
		free_aux_struct_mem((char *)delptr + get_element_offset_with_index(type, i), base_type);
}

/*
 * Frees all allocated auxilary memory in dynamic array
 * after that free dynamic array
 */
void free_aux_mem_dyn_arr(void *delptr, const char *type)
{
	const char *base_type = get_array_basic_type(type);
	int arr_size = get_static_array_size(type);
	void **datapp = NULL;

	/* recursive free in each element of array*/
	for (int i = 0; i < arr_size; i++)
		free_aux_struct_mem((char *)delptr + get_element_offset_with_index(type, i), base_type);

	datapp = (void **) delptr;
	guc_free(*datapp);
	*datapp = NULL;
}


/*
 * Frees all allocated auxiliary memory in structure
 */
void free_aux_structure_mem(void *delptr, const char *type)
{
	/* check type */
	struct type_definition *struct_type = NULL;
	if (!(struct_type = get_type_definition(type)))
		return;

	/* process atomic types */
	if (struct_type->cnt_fields == 0)
	{
		if (!strcmp(type,"string"))
		{
			char **strp = (char **)delptr;
			guc_free(*strp);
			*strp = NULL;
		}
		return;
	}

	/* recursive free for each field*/
	for (int i = 0; i < struct_type->cnt_fields; i++)
	{
		int field_offset = get_field_offset(type, struct_type->fields[i].name);
		free_aux_struct_mem((char *)delptr + field_offset, struct_type->fields[i].type);
	}
}


/*
 * Frees all allocated auxiliary memory in composite object
 */
void free_aux_struct_mem(void *delptr, const char *type)
{
	if (is_static_array_type(type))
		free_aux_mem_stat_arr(delptr, type);
	if (is_dynamic_array_type(type))
		free_aux_mem_dyn_arr(delptr, type);
	free_aux_structure_mem(delptr, type);
}


/*
 * Frees composite object in GUC memory context with it's auxiliary memory
 */
void free_struct(void *delptr, const char *type) {
	free_aux_struct_mem(delptr, type);
	guc_free(delptr);
}