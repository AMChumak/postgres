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
 * Get size in dynamic array. It places after pointer to data
 */
#define dynamic_array_size(ptr) (*(int *)((char *)ptr + sizeof(void *)))

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
bool is_dynamic_array_type(const char *type_name);
int get_static_array_size(const char * type_name);
int get_type_size(const char* type_name);
void struct_dup_impl(void *dest_struct, const void *src_struct, const char *type);
void *struct_dup(const void *structp, const char *type);
int struct_cmp (const void *first, const void *second, const char *type);
char *get_nest_field_type(const char * struct_type, const char *field_path);
void free_aux_struct_mem(void *delptr, const char *type);
void free_struct(void *delptr, const char *type);
void *get_nest_field_ptr(const void *structure, const char *struct_type, const char *field_path);
char *normalize_struct_value(const char *name, const char *value);
bool parse_composite(const char *strvalue, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg);
char *convert_path_composite (const char *field_path, const char *value);
char *get_array_basic_type(const char * array_type_name);
int get_element_offset_with_index(const char *type_name, int index);
int get_field_offset(const char * type_name, const char *field);
char *get_field_type_name(const char *type_name, const char *field);
int get_dynamic_array_mem_size(const char *type_name, const void *structp);
int get_dynamic_array_mem_size_with_length(const char *type_name, const int length);
parser_res find_same_level_symbol(const char *start, const char symbol);

/*
 * Internal functions for parsing guc_composite grammar,
 * in guc_composite_gram.y and guc_composite_scan.l
 */
union YYSTYPE;
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif
extern int guc_composite_yyparse(void *composite_ptr, const char *composite_type, const char **hintmsg, int flags, yyscan_t yyscanner);
extern void guc_composite_yyerror(void *composite_ptr, const char *composite_type, const char **hintmsg, int flags, yyscan_t yyscanner, const char *message);
extern int guc_composite_yylex(union YYSTYPE *yylval_param, const char **hintmsg, yyscan_t yyscanner);
extern void guc_composite_scanner_init(const char *str, yyscan_t *yyscannerp);
extern void guc_composite_scanner_finish(yyscan_t yyscanner);

#endif							/* GUC_COMPOSITE_H */
