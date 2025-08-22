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
#include "lib/stringinfo.h"


int expand_array_view_thd;

#define STRUCT_FIELDS_DELIMETER ";"

struct DynArrTmp
{
	void *data;
	int size;
};


HTAB *guc_types_hashtab;



static int get_type_offset(const char *type_name);
int canonize_idx(const char * field);
char *get_static_aray_element_type(const char *type_name, const char *field);
char *get_dynamic_array_element_type(const char *type_name, const char *field, const void *structure);
char *get_struct_field_type(const char *type_name, const char *field);
char *array_to_str(const void *data, int size, const char *type, bool serialize, bool extend);
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
int get_dynamic_array_mem_size(const char *type_name, const void *structp)
{
	int array_length = dynamic_array_size(structp);
	return get_dynamic_array_mem_size_with_length(type_name, array_length);
}

/*
 * Gets dynamic array size
 * Casual way for using: see dynamic array size in next int field, after use that function
 */
int get_dynamic_array_mem_size_with_length(const char *type_name, const int length)
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
int get_element_offset_with_index(const char *type_name, int index)
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
int get_field_offset(const char * type_name, const char *field) {
	if (!type_name || !field)
		return -1;

	/* extended dynamic array case */
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
 * Core of static_array_to_str and dynamic_array_to_str
 */
char *array_to_str(const void *data, int size, const char *type, bool serialize, bool extend)
{
	const char *tab_prefix = extend ? "\t\t" : "\t";
	StringInfoData buf;
	char *result = NULL;
	char *element_type;

	/* process empty array */
	if (!size)
	{
		if (extend)
		{
			if (serialize)
				return guc_strdup(ERROR, "{size: 0, data: []}");
			else
			 	return guc_strdup(ERROR, "{\n\tsize: 0,\n\tdata: []\n}");
		}
		else
			return guc_strdup(ERROR, "[]");
	}

	initStringInfo(&buf);

	element_type = get_array_basic_type(type);


	/* write prefix */
	if (extend)
	{
		if (serialize)
			appendStringInfo(&buf,"{size: %d, data: [", size);
		else
			appendStringInfo(&buf,"{\n\tsize: %d,\n\tdata: [\n", size);
	}
	else
	{
		if (serialize)
			appendStringInfo(&buf,"[");
		else
			appendStringInfo(&buf,"[\n");
	}

	/* recursive call for each element of array */
	for (int i = 0; i < size; i++)
	{
		char *element;
		int offset = get_element_offset_with_index(type, i);
		if (offset < 0)
			goto out;

		element = struct_to_str((char *)data + offset, element_type, serialize);
		if (!element)
			goto out;


		if (serialize)
		{
			appendStringInfo(&buf, "%s", element);
			if (i < size - 1)
				appendStringInfo(&buf, ", ");
		}
		else
		{
			/* in non-serialized version add tab_prefix at the beginning of each line */
			char *str_saveptr;
			char *str_begin = strtok_r(element, "\n", &str_saveptr);
			appendStringInfo(&buf, "%s%s", tab_prefix, str_begin);

			while ((str_begin = strtok_r(NULL, "\n", &str_saveptr)) != NULL)
				appendStringInfo(&buf, "\n%s%s", tab_prefix, str_begin);

			if (i < size - 1)
				appendStringInfo(&buf, ",\n");
			else
				appendStringInfo(&buf, "\n");
		}

		guc_free(element);
	}

	/* write suffix*/
	if (extend)
	{
		if (serialize)
			appendStringInfo(&buf, "]}");
		else
			appendStringInfo(&buf, "\n\t]\n}");
	}
	else
	{
		if (serialize)
			appendStringInfo(&buf, "]");
		else
		 	appendStringInfo(&buf, "\n]");
	}


	result = guc_strdup(ERROR, buf.data);

out:
	pfree(buf.data);
	guc_free(element_type);
	return result;
}

/*
 *  Converts object of static array type to string
 */
char *static_array_to_str(const void *structp, const char *type, bool serialize)
{
	int array_size = get_static_array_size(type);
	return array_to_str(structp, array_size, type, serialize, false);
}


/*
 * Converts object of dynamic array type to string
 * structptr is a pointer to pointer to allocated memory
 * (after pointer should be array's size)
 */
char *dynamic_array_to_str(const void *structp, const char *type, bool serialize)
{
	int array_size = dynamic_array_size(structp);

	/* expand_array_view_thd - global variable (GUC) */
	bool expand = array_size >= expand_array_view_thd;
	return array_to_str(*(void**)structp, array_size, type, serialize, expand);
}


char *atomic_to_str(const void *structp, const char *type, bool serialize)
{
	char *buf;
	char *quoted;

	if (!strcmp(type, "bool"))
	{
		buf = (char *)guc_malloc(ERROR, 6 * sizeof(char));
		if (*(bool *)structp)
			snprintf(buf, 6, "%s", "true");
		else
			snprintf(buf, 6, "%s", "false");
	}
	else if (!strcmp(type, "int"))
	{
		buf = (char *)guc_malloc(ERROR, 12 * sizeof(char)); /* max length of decimal number int32 */
		snprintf(buf, 12, "%d", *(int *)structp);
	}
	else if (!strcmp(type, "real"))
	{
		int maxlen = DBL_MAX_10_EXP + 3;
		buf = (char *)guc_malloc(ERROR,  maxlen * sizeof(char)); /* max length of decimal float */
		snprintf(buf, maxlen, "%lf", *(double *)structp);
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
		int maxlen = strlen(buf) + 3;
		quoted = (char *)guc_malloc(ERROR, maxlen * sizeof(char));
		snprintf(quoted, maxlen, "\'%s\'", buf);
		guc_free(buf);
	}
	else
		quoted = buf;

	return quoted;
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

/*
 *  Converts structure to string
 */
char *structure_to_str(const void *structp, const char *type, bool serialize)
{
	struct type_definition *struct_type;
	StringInfoData buf;
	int cnt_fields;
	char *result = 0;

	initStringInfo(&buf);

	/*check built-in types*/
	if (is_atomic_type(type))
		return atomic_to_str(structp, type, serialize);

	/* standard algorithm of serialize structure to string */

	/* check type */
	struct_type = NULL;
	if (!(struct_type = get_type_definition(type)))
		return NULL;

	cnt_fields = struct_type->cnt_fields;

	/* print prefix */
	appendStringInfo(&buf, "{");
	if (!serialize)
		appendStringInfo(&buf, "\n");

	/* recurse call for fields */
	for (int i = 0; i < cnt_fields; i++)
	{
		char *field;
		void *sptr;
		int offset = get_field_offset(struct_type->type,
									 struct_type->fields[i].name);
		if (offset < 0)
			goto out;

		sptr = (char *)structp + offset;
		field = struct_to_str(sptr, struct_type->fields[i].type, serialize);
		if (!field)
			goto out;

		if (serialize)
		{
			appendStringInfo(&buf, "%s: %s", struct_type->fields[i].name, field);
			if (i < cnt_fields - 1)
				appendStringInfo(&buf, ", ");
		}
		else
		{
			/* in non-serialized version add tabs at the beginning of each line */
			char *str_saveptr;
			char *str_begin = strtok_r(field, "\n", &str_saveptr);
			appendStringInfo(&buf, "\t%s: %s", struct_type->fields[i].name, str_begin);

			while ((str_begin = strtok_r(NULL, "\n", &str_saveptr)) != NULL)
				appendStringInfo(&buf, "\n\t%s", str_begin);

			if (i < cnt_fields - 1)
				appendStringInfo(&buf, ",\n");
			else
				appendStringInfo(&buf, "\n");
		}
		guc_free(field);
	}

	/* print suffix */
	appendStringInfo(&buf, "}");

	result = guc_strdup(ERROR, buf.data);
out:
	pfree(buf.data);
	return result;
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