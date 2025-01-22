/*--------------------------------------------------------------------
 * guc_composite.h
 *
 * Declarations shared between backend/utils/misc/guc.c and
 * backend/utils/misc/guc_composite.c
 *
 * Copyright (c) 2000-2025, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/guc_composite.h
 *--------------------------------------------------------------------
 */
#ifndef GUC_COMPOSITE_H
#define GUC_COMPOSITE_H

#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/hsearch.h"

/*
 * This enum are used to return state of parser between
 */
enum parser_status
{
	PARSER_OK,
	PARSER_FAIL,
	PARSER_ERR,
	PARSER_NOT_FOUND
};

struct parser_res
{
	enum parser_status status;
	bool res_bool;
	int  res_int;
	double res_double;
	char *res_str;
	char *parse_end;

	char *errmsg;
};


typedef struct
{
	const char *typename;
	struct type_definition *definition;
} OptionTypeHashEntry;


typedef struct parser_res parser_res;

#define CNT_SIMPLE_TYPES 5

#define IS_STATUS_OK(val) (val.status == PARSER_OK)
#define IS_STATUS_FAIL(val) (val.status == PARSER_FAIL)
#define IS_STATUS_ERR(val) (val.status == PARSER_ERR)
#define IS_STATUS_NOT_FOUND(val) (val.status == PARSER_NOT_FOUND)


/*
 * Tokenized path to nest structures. It replaces '->' to '\0' and
 * returns pointer to first member name.
 */
#define tokenize_field_path(path) strtok(path, "->[]")


extern HTAB *guc_types_hashtab;


size_t get_length_struct_str(const void *structp, const char *type);
void init_type_definition(struct type_definition *definition);
struct type_definition *get_type_definition(const char *type_name);
bool is_static_array_type(const char * type_name);
int get_type_size(const char* type_name);
void struct_dup_impl(void *dest_struct, const void *src_struct, const char *type);
void *struct_dup(const void *structp, const char *type);
int struct_cmp (const void *first, const void *second, const char *type);
char *get_nest_field_type(const char * struct_type, const char *field_path);
bool is_dynamic_array_type(const char *type_name);
void free_aux_struct_mem(void *delptr, const char *type);
void free_struct(void *delptr, const char *type);
void *get_nest_field_ptr(const void *structure, const char *struct_type, const char *field_path);
char *normalize_struct_value(const char *name, const char *value);
bool parse_composite(const char *value, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg);
char *convert_path_composite (const char *field_path, const char *value);


#endif							/* GUC_COMPOSITE_H */
