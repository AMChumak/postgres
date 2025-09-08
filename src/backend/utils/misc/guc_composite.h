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

typedef struct
{
	const char *type_name;
	struct type_definition *definition;
}			OptionTypeHashEntry;

#define IS_STATUS_OK(val) (val.status == PARSER_OK)
#define IS_STATUS_FAIL(val) (val.status == PARSER_FAIL)
#define IS_STATUS_ERR(val) (val.status == PARSER_ERR)
#define IS_STATUS_NOT_FOUND(val) (val.status == PARSER_NOT_FOUND)

/*
 * Get size in dynamic array. It places after pointer to data
 */
#define dynamic_array_size(ptr) (*(int *)((char *)ptr + sizeof(void *)))

#define suffix_is_arrow(name) (name[strlen(name) - 2] == '-' && name[strlen(name) - 1] == '>')

/*
 * Tokenized path to nest structures. It replaces '->' to '\0' and
 * returns pointer to first member name.
 */
#define tokenize_field_path(path) strtok(path, "->[]")

extern HTAB *guc_types_hashtab;

extern Size get_length_composite_str(const void *structp, const char *type_name);
extern void init_type_definition(struct type_definition *definition);
extern struct type_definition *get_type_definition(const char *type_name);
extern bool is_static_array_type(const char *type_name);
extern bool is_dynamic_array_type(const char *type_name);
extern int	get_static_array_length(const char *type_name);
extern int	get_array_size(const char *type_name, const int length);
extern int	get_composite_size(const char *type_name);
extern void composite_dup_impl(void *dest_struct, const void *src_struct, const char *type_name);
extern void *composite_dup(const void *structp, const char *type_name);
extern int	composite_cmp(const void *first, const void *second, const char *type_name);
extern char *get_field_type_name(const char *type_name, const char *field);
extern char *get_nested_field_type_name(const char *type_name, const char *field_path);
extern int	get_field_offset(const char *type_name, const char *field_name, int position);
extern int	get_element_offset(const char *type_name, int index);
extern void free_composite_impl(void *delptr, const char *type_name);
extern void free_composite(void *delptr, const char *type_name);
extern void *get_nested_field_ptr(const void *composite_start, const char *type_name, const char *field_path);
extern char *normalize_composite_value(const char *option_name, const char *value);
extern bool parse_composite(const char *strvalue, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg);
extern char *convert_path_to_composite_value(const char *field_path, const char *value);
extern char *get_array_basic_type(const char *array_type_name);
extern bool is_scalar_type(const char *type_name);

/*
 * Internal functions for parsing guc_composite grammar,
 * in guc_composite_gram.y and guc_composite_scan.l
 */
union YYSTYPE;
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif
extern int	guc_composite_yyparse(void *composite_ptr, const char *composite_type, const char **hintmsg, int flags, yyscan_t yyscanner);
extern void guc_composite_yyerror(void *composite_ptr, const char *composite_type, const char **hintmsg, int flags, yyscan_t yyscanner, const char *message);
extern int	guc_composite_yylex(union YYSTYPE *yylval_param, const char **hintmsg, yyscan_t yyscanner);
extern void guc_composite_scanner_init(const char *str, yyscan_t *yyscannerp);
extern void guc_composite_scanner_finish(yyscan_t yyscanner);

#endif							/* GUC_COMPOSITE_H */
