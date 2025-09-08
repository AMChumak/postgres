/*--------------------------------------------------------------------
 * guc_composite.c
 *
 * This file contains the implementation of functions
 * related to the custom composite type system.
 *
 * The functions are divided into 3 groups:
 * 1. registration and support for composite types
 * 2. support for composite options
 * 3. printing composite options
 *
 * See src/backend/utils/misc/README for more information.
 *
 * Copyright (c) 2000-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_composite.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <stddef.h>
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


bool		extended_array_view;

#define STRUCT_FIELDS_DELIMETER ';'
#define STRUCT_FIELDS_DELIMETER_STR ";"

#define element_of_data_of_dynamic_array(type_name, field) (is_dynamic_array_type(type_name) &&\
											strcmp(field,"data") != 0 &&\
											strcmp(field, "size") != 0)

/*
 * That structure is template of dynamic array representation in C code
 * It is used only to get sizes and offsets of it's fields
 */
struct DynArrTmp
{
	void	   *data;
	int			size;
};

/* The global hash table of definitions of composite types for guc options */
HTAB	   *guc_types_hashtab;

static int	get_type_offset(const char *type_name);
static char *get_struct_field_type(const char *type_name, const char *field);
static char *print_empty_array(bool writing_to_file, bool extend);
static char *array_to_str(const void *data, int size, const char *type, bool writing_to_file, bool extend);
static char *static_array_to_str(const void *structp, const char *type, bool writing_to_file);
static char *dynamic_array_to_str(const void *structp, const char *type, bool writing_to_file);
static char *scalar_to_str(const void *structp, const char *type_name, bool writing_to_file);
static char *struct_to_str(const void *structp, const char *type, bool writing_to_file);
static void static_array_dup(void *dest_struct, const void *src_struct, const char *type_name);
static void struct_dup(void *dest_struct, const void *src_struct, const char *type_name);
static int	array_data_cmp(const void *first, const void *second, const char *type, int size);
static int	dynamic_array_cmp(const void *first, const void *second, const char *type);
static int	struct_cmp(const void *first, const void *second, const char *type);
static void free_static_array(void *delptr, const char *type);
static void free_dynamic_array(void *delptr, const char *type);
static void free_struct(void *delptr, const char *type);
static void dynamic_array_dup(void *dest_struct, const void *src_struct, const char *type);
static int	get_dynamic_array_size(const char *type_name, const void *structp);


int
get_static_array_length(const char *type_name)
{
	char	   *length_str_begin = strchr(type_name, '[');
	char	   *length_str_end = NULL;
	char	   *parsed_end = NULL;
	long		length = 0;

	if (length_str_begin == NULL)
		return 0;

	length_str_begin++;

	length_str_end = strchr(length_str_begin, ']');

	if (!length_str_end)
		return 0;

	length = strtol(length_str_begin, &parsed_end, 10);

	if (errno == ERANGE)
		return 0;

	while (parsed_end != length_str_end)
	{
		if (!isblank(*parsed_end++))
			return 0;
	}

	return length;
}

bool
is_static_array_type(const char *type_name)
{
	return (bool) get_static_array_length(type_name);
}

bool
is_dynamic_array_type(const char *type_name)
{
	char	   *size_str_begin = strchr(type_name, '[');

	if (size_str_begin && *(size_str_begin + 1) == ']')
		return true;

	return false;
}

char *
get_array_basic_type(const char *array_type)
{
	ptrdiff_t	first_part_len;
	ptrdiff_t	second_part_len;
	size_t		type_len;
	char	   *type_name;
	const char *bracket_close;
	const char *bracket_open = strchr(array_type, '[');

	if (!bracket_open)
		return NULL;

	bracket_close = strchr(bracket_open, ']');

	if (!bracket_close)
		return NULL;

	first_part_len = bracket_open - array_type;
	second_part_len = strchr(bracket_close, '\0') - bracket_close;
	type_len = first_part_len + second_part_len;

	type_name = guc_malloc(ERROR, type_len);
	strncpy(type_name, array_type, first_part_len);
	strncpy(type_name + first_part_len, bracket_close + 1, second_part_len);

	return type_name;
}

struct type_definition *
get_type_definition(const char *type_name)
{
	struct type_definition *definition;
	bool		found = false;
	OptionTypeHashEntry *type_hentry = NULL;

	type_hentry = (OptionTypeHashEntry *) hash_search(guc_types_hashtab, &type_name, HASH_FIND, &found);

	if (found)
	{
		definition = type_hentry->definition;

		return definition;
	}

	return NULL;
}

int
get_array_size(const char *type_name, const int length)
{
	int			array_size;
	int			element_size;
	int			element_offset;
	char	   *basic_type = get_array_basic_type(type_name);

	if (!basic_type || length < 0)
		return -1;

	element_offset = get_type_offset(basic_type);
	element_size = get_composite_size(basic_type);
	guc_free(basic_type);

	if (element_offset < 0 || element_size < 0)
		return -1;

	array_size = length * (element_size + (element_size % element_offset));

	return array_size;
}

static int
get_static_array_size(const char *type_name)
{
	int			length = get_static_array_length(type_name);

	return get_array_size(type_name, length);
}

static int
get_dynamic_array_size(const char *type_name, const void *structp)
{
	int			length = dynamic_array_size(structp);

	return get_array_size(type_name, length);
}

static int
get_struct_size(const char *type_name)
{
	struct type_definition *struct_type = NULL;

	if ((struct_type = get_type_definition(type_name)))
		return struct_type->type_size;

	return -1;
}

int
get_composite_size(const char *type_name)
{
	if (!type_name)
		return -1;

	/*
	 * Dynamic array is a struct that has 2 fields: pointer, int (see
	 * DynArrTmp) Do not use this function for getting size of allocated data
	 * of dynamic array
	 */
	if (is_dynamic_array_type(type_name))
		return sizeof(struct DynArrTmp);

	if (is_static_array_type(type_name))
		return get_static_array_size(type_name);

	return get_struct_size(type_name);
}

static int
get_array_offset(const char *type_name)
{
	int			element_offset;
	char	   *basic_type = get_array_basic_type(type_name);

	if (!basic_type)
		return -1;

	element_offset = get_type_offset(basic_type);

	if (element_offset < 0)
		return -1;

	guc_free(basic_type);

	return element_offset;
}

static int
get_struct_offset(const char *type_name)
{
	struct type_definition *struct_type = NULL;

	if (!(struct_type = get_type_definition(type_name)))
		return -1;

	return struct_type->offset;
}

static int
get_type_offset(const char *type_name)
{
	if (!type_name)
		return -1;

	/*
	 * Dynamic array in struct that is 2 fields: pointer, int Therefore offset
	 * of pointer, int and offset of the pointer are same
	 */
	if (is_dynamic_array_type(type_name))
		return sizeof(void *);

	if (is_static_array_type(type_name))
		return get_array_offset(type_name);

	return get_struct_offset(type_name);
}

static char *
get_struct_field_type(const char *type_name, const char *field)
{
	struct type_definition *struct_type = NULL;

	if (!(struct_type = get_type_definition(type_name)))
		return NULL;

	for (int i = 0; i < struct_type->cnt_fields; i++)
	{
		if (struct_type->fields[i].name != NULL &&
			strcmp(field, struct_type->fields[i].name) == 0)
			return guc_strdup(ERROR, struct_type->fields[i].type);
	}

	return NULL;
}

char *
get_field_type_name(const char *type_name, const char *field)
{
	if (!type_name || !field)
		return NULL;

	/*
	 * if field is "data" or "size", dynamic array is DynArrTmp else dynamic
	 * array is allocated data
	 */
	if (is_dynamic_array_type(type_name))
	{
		if (strcmp(field, "size") == 0)
			return guc_strdup(ERROR, "int");

		if (strcmp(field, "data") == 0)
			return guc_strdup(ERROR, type_name);

		return get_array_basic_type(type_name);
	}

	if (is_static_array_type(type_name))
		return get_array_basic_type(type_name);

	return get_struct_field_type(type_name, field);
}

int
get_element_offset(const char *type_name, int index)
{
	int			element_size;
	int			element_offset;
	char	   *basic_type = get_array_basic_type(type_name);

	if (!basic_type || index < 0)
		return -1;

	element_offset = get_type_offset(basic_type);
	element_size = get_composite_size(basic_type);
	guc_free(basic_type);

	if (element_offset < 0 || element_size < 0)
		return -1;

	return element_size * index;
}

static int
get_element_offset_with_parse_index(const char *type_name, const char *field)
{
	char	   *parsed_end = NULL;
	long		field_idx = -1;

	field_idx = strtol(field, &parsed_end, 10);

	if (errno == ERANGE)
		return -1;

	while (*parsed_end)
	{
		if (!isblank(*parsed_end++))
			return -1;
	}

	return get_element_offset(type_name, (int) field_idx);
}

static int
get_struct_field_offset(const char *type_name, const char *field_name, int position)
{
	struct type_definition *struct_type = NULL;

	if ((struct_type = get_type_definition(type_name)) == NULL)
		return -1;

	for (int i = 0, total_offset = 0; i < struct_type->cnt_fields; ++i)
	{
		int			increment;
		int			local_off = get_type_offset(struct_type->fields[i].type);

		if (local_off < 0)
			return -1;

		if (total_offset % local_off != 0)
			total_offset += local_off - total_offset % local_off;

		if (struct_type->fields[i].name != NULL && field_name != NULL)
		{
			if (strcmp(struct_type->fields[i].name, field_name) == 0)
				return total_offset;
		}
		else if (i == position)
			return total_offset;

		increment = get_composite_size(struct_type->fields[i].type);
		total_offset += increment;
	}

	return -1;
}

int
get_field_offset(const char *type_name, const char *field_name, int position)
{
	if (type_name == NULL || (field_name == NULL && position < 0))
		return -1;

	/*
	 * if field_name is "data" or "size", composite value is a DynArrTmp else
	 * composite value is allocated data for dynamic array (Attention! This
	 * function cannot check length of dynamic array)
	 */
	if (is_dynamic_array_type(type_name))
	{
		if (field_name)
		{
			if (strcmp(field_name, "data") == 0)
				return offsetof(struct DynArrTmp, data);
			else if (strcmp(field_name, "size") == 0)
				return offsetof(struct DynArrTmp, size);
			else
				return get_element_offset_with_parse_index(type_name, field_name);
		}
		else
			return get_element_offset(type_name, position);
	}

	if (is_static_array_type(type_name))
	{
		if (field_name)
			return get_element_offset_with_parse_index(type_name, field_name);
		else
			return get_element_offset(type_name, position);
	}

	return get_struct_field_offset(type_name, field_name, position);
}

void
init_type_definition(struct type_definition *definition)
{
	const char *word_del = " \t\n\v";
	int			max_offset = 0;
	int			count_fields = 1;
	struct_field *fields = NULL;	/* meta about fields */
	char	   *signature_saveptr;
	char	   *field_def_saveptr;
	char	   *signature,
			   *field_def;
	char	   *field_def_token;
	char	   *word_token;
	char	   *signature_buffer;
	int			curr_offset = 0;
	int			i;

	/* count fields in signature */
	const char *sym = definition->signature;

	if (!sym || !*sym)
	{
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("signature of \"%s\" type is empty", definition->type_name));

		return;
	}

	while (*sym)
	{
		if (*sym == STRUCT_FIELDS_DELIMETER)
			count_fields++;

		sym++;
	}

	/* allocate structures for field definitions */
	fields = (struct_field *) guc_malloc(ERROR, count_fields * sizeof(struct_field));

	/* parse signature */

	signature = guc_strdup(ERROR, definition->signature);
	signature_buffer = signature;

	/* parse sequence of structure field definitions */
	for (i = 0;; i++, signature = NULL)
	{
		int			parsed_word_no = 0;

		field_def_token = strtok_r(signature, STRUCT_FIELDS_DELIMETER_STR, &signature_saveptr);

		if (!field_def_token)
			break;

		if (i >= count_fields)
			{
				ereport(ERROR,
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("wrong type signature: \"%s\" in definition of type \"%s\". It has too many fields",
								definition->signature, definition->type_name));

				goto out;
			}

		fields[i].type = NULL;
		fields[i].name = NULL;

		/*
		 * Parse field definition First word is a type, second is a name of
		 * field. Name is optional, cause there might be anonymous fields.
		 * Definitions are separated with STRUCT_FIELDS_DELIMETER
		 */
		for (field_def = field_def_token;; field_def = NULL)
		{
			word_token = strtok_r(field_def, word_del, &field_def_saveptr);

			if (!word_token)
			{
				if (parsed_word_no < 1)
				{
					ereport(ERROR,
							errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("wrong field definition: \"%s\" in definition of type \"%s\"",
								   field_def_token, definition->type_name));

					goto out;
				}

				break;
			}

			word_token = guc_strdup(ERROR, word_token);

			/* parse field type */
			if (parsed_word_no == 0)
			{
				int			type_offset = get_type_offset(word_token);
				int			type_size = get_composite_size(word_token);

				if (type_offset < 0 || type_size < 0)
				{
					ereport(ERROR,
							errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("wrong type \"%s\"is used in field definition: \"%s\" in definition of type \"%s\"",
								   word_token, field_def_token, definition->type_name));

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
			else if (parsed_word_no == 1)	/* parse field name */
				fields[i].name = word_token;
			else
			{
				ereport(ERROR,
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("wrong field definition: \"%s\" in definition of type \"%s\"",
							   field_def_token, definition->type_name));

				goto out;
			}

			parsed_word_no++;
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
	if (fields != NULL)
	{
		for (int j = 0; j < i; j++)
		{
			guc_free(fields[j].type);
			guc_free(fields[j].name);
		}
		guc_free(fields);
	}
	guc_free(word_token);
	guc_free(signature_buffer);

	return;
}

char *
get_nested_field_type_name(const char *type_name, const char *field_path)
{
	char	   *path;
	char	   *cur_type_name;
	char	   *cur_field;

	if (!type_name || !field_path)
		return NULL;

	path = guc_strdup(ERROR, field_path);
	cur_type_name = guc_strdup(ERROR, type_name);
	cur_field = tokenize_field_path(path);
	cur_field = tokenize_field_path(NULL);	/* skip name of structure name */

	/* Follow the path of the field */
	while (cur_field && cur_type_name)
	{
		char	   *next_type = get_field_type_name(cur_type_name, cur_field);

		guc_free(cur_type_name);
		cur_type_name = next_type;

		cur_field = tokenize_field_path(NULL);
	}

	guc_free(path);

	return cur_type_name;
}

void *
get_nested_field_ptr(const void *composite_start, const char *type_name, const char *field_path)
{
	char	   *path;
	char	   *cur_type_name;
	char	   *cur_field;
	char	   *cur_ptr;

	if (!composite_start || !field_path || !type_name)
		return NULL;

	path = guc_strdup(ERROR, field_path);
	cur_type_name = guc_strdup(ERROR, type_name);

	cur_field = tokenize_field_path(path);
	cur_field = tokenize_field_path(NULL);	/* skip name of structure */
	cur_ptr = (char *) composite_start;

	while (cur_field && cur_type_name)
	{
		char	   *next_type;
		int			local_offset;

		/* go to memory of dynamic array */
		if (element_of_data_of_dynamic_array(cur_type_name, cur_field))
			cur_ptr = *((char **) cur_ptr);

		local_offset = get_field_offset(cur_type_name, cur_field, -1);

		if (local_offset < 0)
		{
			cur_ptr = NULL;
			break;
		}

		cur_ptr += local_offset;
		next_type = get_field_type_name(cur_type_name, cur_field);

		guc_free(cur_type_name);
		cur_type_name = next_type;
		cur_field = tokenize_field_path(NULL);
	}

	guc_free(path);
	guc_free(cur_type_name);

	return cur_ptr;
}

static char *
print_empty_array(bool writing_to_file, bool extend)
{
	if (extend)
	{
		if (writing_to_file)
			return guc_strdup(ERROR, "{size: 0, data: []}");
		else
			return guc_strdup(ERROR, "{\n\tsize: 0,\n\tdata: []\n}");
	}
	else
		return guc_strdup(ERROR, "[]");
}

static char *
array_to_str(const void *data, int size, const char *type, bool writing_to_file, bool extend)
{
	const char *tab_prefix = extend ? "\t\t" : "\t";
	StringInfoData buf;
	char	   *result = NULL;
	char	   *element_type;

	/* process empty array */
	if (!size)
		print_empty_array(writing_to_file, extend);

	initStringInfo(&buf);

	element_type = get_array_basic_type(type);

	/* write prefix */
	if (extend)
	{
		if (writing_to_file)
			appendStringInfo(&buf, "{size: %d, data: [", size);
		else
			appendStringInfo(&buf, "{\n\tsize: %d,\n\tdata: [\n", size);
	}
	else
	{
		if (writing_to_file)
			appendStringInfo(&buf, "[");
		else
			appendStringInfo(&buf, "[\n");
	}

	/* recursive call for each element of array */
	for (int i = 0; i < size; i++)
	{
		char	   *element;
		int			offset = get_element_offset(type, i);

		if (offset < 0)
			goto out;

		element = composite_to_str((char *) data + offset, element_type, writing_to_file);

		if (!element)
			goto out;

		if (writing_to_file)
		{
			appendStringInfo(&buf, "%s", element);

			if (i < size - 1)
				appendStringInfo(&buf, ", ");
		}
		else
		{
			/*
			 * if not writing to file, add tab_prefix at the beginning of each
			 * line
			 */
			char	   *str_saveptr;
			char	   *str_begin = strtok_r(element, "\n", &str_saveptr);

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

	/* write suffix */
	if (extend)
	{
		if (writing_to_file)
			appendStringInfo(&buf, "]}");
		else
			appendStringInfo(&buf, "\t]\n}");
	}
	else
		appendStringInfo(&buf, "]");

	result = guc_strdup(ERROR, buf.data);

out:
	pfree(buf.data);
	guc_free(element_type);

	return result;
}

static char *
static_array_to_str(const void *structp, const char *type, bool writing_to_file)
{
	int			array_size = get_static_array_length(type);

	return array_to_str(structp, array_size, type, writing_to_file, false);
}

static char *
dynamic_array_to_str(const void *structp, const char *type, bool writing_to_file)
{
	int			array_size = dynamic_array_size(structp);

	/* extended_array_view - global variable (GUC) */
	return array_to_str(*(void **) structp,
						array_size,
						type,
						writing_to_file,
						extended_array_view);
}

static char *
scalar_to_str(const void *structp, const char *type_name, bool writing_to_file)
{
	char	   *buf;
	char	   *quoted;

	if (strcmp(type_name, "bool") == 0)
	{
		int			maxlen = 6;

		buf = (char *) guc_malloc(ERROR, maxlen);

		if (*(bool *) structp)
			snprintf(buf, maxlen, "%s", "true");
		else
			snprintf(buf, maxlen, "%s", "false");
	}
	else if (strcmp(type_name, "int") == 0)
	{
		int			maxlen = 12;

		buf = (char *) guc_malloc(ERROR, maxlen);
		snprintf(buf, maxlen, "%d", *(int *) structp);
	}
	else if (strcmp(type_name, "real") == 0)
	{
		int			maxlen = DBL_MAX_10_EXP + 3;

		buf = (char *) guc_malloc(ERROR, maxlen);
		snprintf(buf, maxlen, "%lf", *(double *) structp);
	}
	else if (strcmp(type_name, "string") == 0)
	{
		if (*(char **) structp == NULL)
			buf = guc_strdup(ERROR, "nil");
		else
		{
			/* escape quotes only if writing to file */
			if (writing_to_file)
			{
				char	   *escaped = escape_single_quotes_ascii(*(char **) structp);

				buf = guc_strdup(ERROR, escaped);
				free(escaped);
			}
			else
				buf = guc_strdup(ERROR, *(char **) structp);
		}
	}
	else
		return NULL;

	/*
	 * add apostrophes: If write to file, add apostrophes for each type Else
	 * add apostrophes only for strings
	 */
	if (writing_to_file || (strcmp(type_name, "string") == 0 && strcmp(buf, "nil") != 0))
	{
		int			maxlen = strlen(buf) + 3;

		quoted = (char *) guc_malloc(ERROR, maxlen * sizeof(char));
		snprintf(quoted, maxlen, "\'%s\'", buf);
		guc_free(buf);
	}
	else
		quoted = buf;

	return quoted;
}

bool
is_scalar_type(const char *type_name)
{
	if (strcmp(type_name, "bool") == 0 ||
		strcmp(type_name, "int") == 0 ||
		strcmp(type_name, "real") == 0 ||
		strcmp(type_name, "string") == 0)
		return true;

	return false;
}

static char *
struct_to_str(const void *structp, const char *type, bool writing_to_file)
{
	struct type_definition *definition;
	StringInfoData buf;
	int			cnt_fields;
	char	   *result = 0;

	initStringInfo(&buf);

	/* check built-in types */
	if (is_scalar_type(type))
		return scalar_to_str(structp, type, writing_to_file);

	/* standard algorithm of preparing structure for writing to file */
	definition = NULL;
	if ((definition = get_type_definition(type)) == NULL)
		return NULL;

	cnt_fields = definition->cnt_fields;

	/* print prefix */
	appendStringInfo(&buf, "{");

	if (!writing_to_file)
		appendStringInfo(&buf, "\n");

	/* recurse call for fields */
	for (int i = 0; i < cnt_fields; i++)
	{
		char	   *field;
		void	   *sptr;
		int			offset;

		if (definition->fields[i].name == NULL)
			continue;

		offset = get_field_offset(definition->type_name, NULL, i);

		if (offset < 0)
			goto out;

		sptr = (char *) structp + offset;
		field = composite_to_str(sptr, definition->fields[i].type, writing_to_file);

		if (!field)
			goto out;

		if (writing_to_file)
		{
			appendStringInfo(&buf, "%s: %s", definition->fields[i].name, field);

			if (i < cnt_fields - 1)
				appendStringInfo(&buf, ", ");
		}
		else
		{
			/* if not write ro file, add tabs at the beginning of each line */
			char	   *str_saveptr;
			char	   *str_begin = strtok_r(field, "\n", &str_saveptr);

			appendStringInfo(&buf, "\t%s: %s", definition->fields[i].name, str_begin);

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

char *
composite_to_str(const void *structp, const char *type, bool writing_to_file)
{
	if (is_static_array_type(type))
		return static_array_to_str(structp, type, writing_to_file);

	if (is_dynamic_array_type(type))
		return dynamic_array_to_str(structp, type, writing_to_file);

	return struct_to_str(structp, type, writing_to_file);
}

char *
normalize_composite_value(const char *option_name, const char *value)
{
	/*
	 * Composite value couldn't be wrapped in quotes scalar types must be
	 * escaped and wrapped in quotes All names related to composite values
	 * ended with "->"
	 */
	bool		is_composite = suffix_is_arrow(option_name);
	char	   *prepared_val;
	char	   *str_val;

	/*
	 * Each value that goes throw this function went throw parser before. If
	 * value is scalar, it was deescaped, else (if value is composite) it
	 * wasn't. Function parse_composite always deescapes scalar values.
	 * Therefore we must escape scalar values for parse_composite
	 */
	if (!is_composite)
	{
		char	   *escaped = escape_single_quotes_ascii(value);
		int			len = strlen(escaped) + 3;

		/* escape */
		prepared_val = guc_malloc(ERROR, len);
		snprintf(prepared_val, len, "\'%s\'", escaped);

		free(escaped);
	}
	else
		prepared_val = (char *) value;	/* be careful with free */

	str_val = convert_path_to_composite_value(option_name, prepared_val);

	if (prepared_val != value)
		guc_free(prepared_val);

	return str_val;
}

static Size
get_len_serialized_array(const void *structp, const char *type)
{
	char	   *element_type = get_array_basic_type(type);
	int			total_size = 3;
	void	   *datap = NULL;
	int			array_size = 0;

	if (is_dynamic_array_type(type))
	{
		array_size = dynamic_array_size(structp);
		datap = *((void **) structp);
	}
	else
	{
		array_size = get_static_array_length(type);
		datap = (void *) structp;
	}

	/* compute length for first element */
	for (int i = 0; i < array_size; i++)
	{
		int			offset = get_element_offset(type, i);
		int			element_len = get_length_composite_str((char *) datap + offset, element_type);

		total_size += element_len + 2;	/* 2 = len(", ") */
	}

	guc_free(element_type);

	return total_size;
}

static Size
get_len_serialized_struct(const void *structp, const char *type)
{
	struct type_definition *definition = NULL;
	int			total_size = 3;

	if (strcmp(type, "bool") == 0)
		return 6;
	else if (strcmp(type, "int") == 0)
	{
		if (*(int *) structp < 100)
			return 4;

		return 12;
	}
	else if (strcmp(type, "real") == 0)
		return 1 + 1 + 1 + REALTYPE_PRECISION + 5;
	else if (strcmp(type, "string") == 0)
	{
		if (*(char **) structp)
			return strlen(*(char **) structp);

		return 5;				/* len of "\nil" */
	}

	if ((definition = get_type_definition(type)) == NULL)
		return 0;

	for (int i = 0; i < definition->cnt_fields; i++)
	{
		int			offset;
		int			field_len;

		if (definition->fields[i].name == NULL)
			continue;

		offset = get_field_offset(definition->type_name, NULL, i);
		field_len = get_length_composite_str((char *) structp + offset, definition->fields[i].type);

		total_size += field_len + 2;
	}

	return total_size;
}

Size
get_length_composite_str(const void *structp, const char *type_name)
{
	if (is_static_array_type(type_name) || is_dynamic_array_type(type_name))
		return get_len_serialized_array(structp, type_name);

	return get_len_serialized_struct(structp, type_name);
}

char *
convert_path_to_composite_value(const char *field_path, const char *value)
{
	char	   *path = guc_strdup(ERROR, field_path);
	char	   *cur_field = tokenize_field_path(path);
	char	   *prefix = guc_strdup(ERROR, "");
	char	   *suffix = guc_strdup(ERROR, "");
	char	   *result;
	int			len;

	/* skip guc name */
	cur_field = tokenize_field_path(NULL);

	/* for each step in path generate derived braces and name of field */
	while (cur_field)
	{
		int			prefix_len = strlen(prefix);
		int			suffix_len = strlen(suffix);

		int			prefix_diff_len = 3 + strlen(cur_field) + 1;	/* 3 for "[: ", 1 for
																	 * '\0' */
		int			suffix_diff_len = 2;

		char	   *next_prefix = guc_malloc(ERROR, prefix_len + prefix_diff_len);
		char	   *next_suffix = guc_malloc(ERROR, suffix_len + suffix_diff_len);

		snprintf(next_prefix, prefix_len + 1, "%s", prefix);
		/* define array or structure */
		if (isdigit(cur_field[0]))
		{
			snprintf(next_prefix + prefix_len, 2, "[");
			snprintf(next_suffix, 2, "]");
		}
		else
		{
			snprintf(next_prefix + prefix_len, 2, "{");
			snprintf(next_suffix, 2, "}");
		}

		snprintf(next_prefix + prefix_len + 1, prefix_diff_len - 1, "%s: ", cur_field);
		snprintf(next_suffix + 1, suffix_len + 1, "%s", suffix);

		guc_free(prefix);
		guc_free(suffix);

		prefix = next_prefix;
		suffix = next_suffix;

		cur_field = tokenize_field_path(NULL);
	}

	/* construct result from prefix, suffix and value */
	len = strlen(prefix) + strlen(value) + strlen(suffix) + 1;
	result = guc_malloc(ERROR, len);
	snprintf(result, len, "%s%s%s", prefix, value, suffix);

	guc_free(prefix);
	guc_free(suffix);

	return result;
}

static void
static_array_dup(void *dest_struct, const void *src_struct, const char *type_name)
{
	const char *basic_type = get_array_basic_type(type_name);
	int			arr_size = get_static_array_length(type_name);

	/* recursive duplicate array elements */
	for (int i = 0; i < arr_size; i++)
	{
		int			offset = get_element_offset(type_name, i);
		void	   *dest_ptr = (char *) dest_struct + offset;
		void	   *src_ptr = (char *) src_struct + offset;

		composite_dup_impl(dest_ptr, src_ptr, basic_type);
	}
}

/*
 * Beware! src_struct points to structure like DynArrTmp
 */
static void
dynamic_array_dup(void *dest_struct, const void *src_struct, const char *type)
{
	void	   *datap;
	void	  **dstpp;
	void	   *dstp;
	const char *basic_type = get_array_basic_type(type);
	int			arr_mem_size = get_dynamic_array_size(type, src_struct);
	int			arr_size = dynamic_array_size(src_struct);

	if (!arr_size)
	{
		*(void **) dest_struct = NULL;
		*((void **) dest_struct + 1) = NULL;

		return;
	}

	datap = *((void **) src_struct);
	dstpp = (void **) dest_struct;
	*dstpp = guc_malloc(ERROR, arr_mem_size * sizeof(char));
	dstp = *dstpp;

	for (int i = 0; i < arr_size; i++)
	{
		int			offset = get_element_offset(type, i);
		void	   *dest_ptr = (char *) dstp + offset;
		void	   *src_ptr = (char *) datap + offset;

		composite_dup_impl(dest_ptr, src_ptr, basic_type);
	}

	dynamic_array_size(dest_struct) = arr_size;
}

static void
struct_dup(void *dest_struct, const void *src_struct, const char *type_name)
{
	struct type_definition *struct_type = NULL;

	if (!(struct_type = get_type_definition(type_name)))
		return;

	if (is_scalar_type(type_name))
	{
		if (!strcmp(type_name, "string"))
		{
			if (*(char **) src_struct)
				*(char **) dest_struct = guc_strdup(ERROR, *(char **) src_struct);
			else
				*(char **) dest_struct = NULL;

			return;
		}

		memcpy(dest_struct, src_struct, struct_type->type_size);

		return;
	}

	for (int i = 0; i < struct_type->cnt_fields; i++)
	{
		const char *	field_type = struct_type->fields[i].type;
		int				field_offset = get_field_offset(type_name, NULL, i);
		void *			dest_ptr = (char *) dest_struct + field_offset;
		void *			src_ptr = (char *) src_struct + field_offset;

		composite_dup_impl(dest_ptr, src_ptr, field_type);
	}
}

void
composite_dup_impl(void *dest_struct, const void *src_struct, const char *type_name)
{
	if (is_static_array_type(type_name))
		return static_array_dup(dest_struct, src_struct, type_name);
	if (is_dynamic_array_type(type_name))
		return dynamic_array_dup(dest_struct, src_struct, type_name);

	return struct_dup(dest_struct, src_struct, type_name);
}

void *
composite_dup(const void *structp, const char *type_name)
{
	int			struct_size;
	void	   *duplicate;

	if (!structp)
		return NULL;

	struct_size = get_composite_size(type_name);
	duplicate = guc_malloc(ERROR, struct_size);

	/* recursive bypass and searching string */
	composite_dup_impl(duplicate, structp, type_name);

	return duplicate;
}

static int
array_data_cmp(const void *first, const void *second, const char *type, int size)
{
	const char *base_type = get_array_basic_type(type);
	int			base_type_size = get_composite_size(base_type);
	int			res = 0;

	/* recursive compare each element */
	for (int i = 0; i < size; i++)
	{
		int			offset = base_type_size * i;
		void	   *first_element = (char *) first + offset;
		void	   *second_element = (char *) second + offset;

		res = composite_cmp(first_element, second_element, base_type);

		if (res)
			break;
	}

	return res;
}

static int
dynamic_array_cmp(const void *first, const void *second, const char *type)
{
	void	   *first_data = *((void **) first);
	void	   *second_data = *((void **) second);

	int			first_size = dynamic_array_size(first);
	int			second_size = dynamic_array_size(second);

	int			cmp = 0;

	if ((cmp = first_size - second_size))
		return cmp;

	return array_data_cmp(first_data, second_data, type, first_size);
}

static int
struct_cmp(const void *first, const void *second, const char *type_name)
{
	int			res;

	/* check type */
	struct type_definition *struct_type = NULL;

	if (!(struct_type = get_type_definition(type_name)))
		return 2;				/* error code */

	/* check scalar types like int, real, etc */
	if (struct_type->cnt_fields == 0)
	{
		/* compare string with strcmp, not pointers! */
		res = 0;

		if (strcmp(type_name, "string") == 0)
		{
			if (!*(char **) first && !*(char **) second)
				return 0;

			if (!*(char **) first)
				return -1;

			if (!*(char **) second)
				return 1;

			res = strcmp(*(char **) first, *(char **) second);
		}
		else if (strcmp(type_name, "bool") == 0)
			res = *(bool *) first - *(bool *) second;
		else if (strcmp(type_name, "int") == 0)
			res = *(int *) first - *(int *) second;
		else if (strcmp(type_name, "real") == 0)
		{
			double		res = *(double *) first - *(double *) second;

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

	/* recursive comparison of fields */
	res = 0;

	for (int i = 0; i < struct_type->cnt_fields; i++)
	{
		const char *	field_type = struct_type->fields[i].type;
		int				field_offset = get_field_offset(type_name, NULL, i);
		void *			first_field = (char *) first + field_offset;
		void *			second_field = (char *) second + field_offset;

		res = composite_cmp(first_field, second_field, field_type);

		if (res)
			break;
	}

	return res;
}

int
composite_cmp(const void *first, const void *second, const char *type_name)
{
	if (is_static_array_type(type_name))
		return array_data_cmp(first, second, type_name, get_static_array_length(type_name));

	if (is_dynamic_array_type(type_name))
		return dynamic_array_cmp(first, second, type_name);

	return struct_cmp(first, second, type_name);
}

static void
free_static_array(void *delptr, const char *type)
{
	const char *base_type = get_array_basic_type(type);
	int			arr_size = get_static_array_length(type);

	for (int i = 0; i < arr_size; i++)
	{
		void	   *element_ptr = (char *) delptr + get_element_offset(type, i);

		free_composite_impl(element_ptr, base_type);
	}
}

static void
free_dynamic_array(void *delptr, const char *type)
{
	const char *base_type = get_array_basic_type(type);
	int			arr_size = get_static_array_length(type);
	void	  **datapp = NULL;

	for (int i = 0; i < arr_size; i++)
	{
		void	   *element_ptr = (char *) delptr + get_element_offset(type, i);

		free_composite_impl(element_ptr, base_type);
	}

	datapp = (void **) delptr;
	guc_free(*datapp);
	*datapp = NULL;
}

static void
free_struct(void *delptr, const char *type)
{
	struct type_definition *struct_type = NULL;

	if ((struct_type = get_type_definition(type)) == NULL)
		return;

	if (struct_type->cnt_fields == 0)
	{
		if (strcmp(type, "string") == 0)
		{
			char	  **strp = (char **) delptr;

			guc_free(*strp);
			*strp = NULL;
		}

		return;
	}

	for (int i = 0; i < struct_type->cnt_fields; i++)
	{
		const char *	field_type = struct_type->fields[i].type;
		int				field_offset = get_field_offset(type, NULL, i);

		free_composite_impl((char *) delptr + field_offset, field_type);
	}
}

void
free_composite_impl(void *delptr, const char *type_name)
{
	if (is_static_array_type(type_name))
		free_static_array(delptr, type_name);

	if (is_dynamic_array_type(type_name))
		free_dynamic_array(delptr, type_name);

	free_struct(delptr, type_name);
}

void
free_composite(void *delptr, const char *type_name)
{
	free_composite_impl(delptr, type_name);
	guc_free(delptr);
}
