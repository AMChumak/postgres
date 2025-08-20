%{
/*-------------------------------------------------------------------------
 *
 * guc_composite_gram.y				- Parser for all composite guc options
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_composite_gram.y
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/pg_list.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "guc_composite.h"
#include "guc_composite_gram.h"
#include <string.h>
#include <stdlib.h>

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

#define context(num) ((parser_ctx *)list_nth(contexts, num))
#define list_empty(l) (list_length(l) == 0)
#define last_context (*(parser_ctx **)list_tail(contexts))

/* Stack is needed to memoize valuable data between nested layers in composite object */
enum name_usage
{
    NAME_USAGE_UNKNOWN,
    NAME_USAGE_ALWAYS,
    NAME_USAGE_NEVER
};

/* Stages of setting size for dynamic arrays */
enum fixed_size
{
    FIXED_SIZE_IS_NOT_SETTED,   /* Size was not defined for dynamic array */
    FIXED_SIZE_IS_BEING_SETTED, /* Current value must be setted as a length of dynamic array */
    FIXED_SIZE_IS_SETTED        /* Indexes in array must be less than size */
};

typedef struct parser_ctx
{
    char *type;                     /* type of composite object */
    void *start;                    /* pointer to start of composite object */
    int  idx;                       /* automatic computed index for current child */
    bool has_name;                  /* name (index) was parsed before current composite object */
    bool extended;                  /* flag for dynamic arrays. True = extended text representation */
    enum fixed_size fixed_size;     /* field for outer context of dynamic array. see comments for enum */
    int  max_idx;                   /* field for outer context of dynamic array. sets value of max idx in data */
    enum name_usage name_usage;     /* names (indexes) are used for children of composite object */
} parser_ctx;

static List *contexts = NIL;              /* stack of contexts */

static void init_context(const char *type, void *ptr);
static void push_context(const char* type, void *start);
static void free_context(parser_ctx *context);
static void free_context_list(void);
static void check_name(yyscan_t yyscanner, const char **hintmsg);
static void reallocate_dynamic_array(const char *array_type, void *array, int new_len);
static void check_memory(yyscan_t yyscanner, const char **hintmsg);
static void prepare_value_context(yyscan_t yyscanner, const char **hintmsg);
static void prepare_structure(void);
static void prepare_array(void);
static void parse_composite_end(void);
static void parse_field_end(void);
static int parse_index(const char *index, yyscan_t yyscanner, const char **hintmsg);
static void parse_element(const char *index, yyscan_t yyscanner, const char **hintmsg);
static void parse_field(const char *name, yyscan_t yyscanner, const char **hintmsg);
static void parse_name(const char *name, yyscan_t yyscanner, const char **hintmsg);
static void parse_simple_opt(char *strval, const char *struct_type, void *result, int flags, yyscan_t yyscanner, const char **hintmsg);
static bool is_assignment_list(const char *value);
static bool parse_placeholder_patch_list(const char *value, const char *type, void **result,const void *prev_val, int flags, const char **hintmsg);
%}
%parse-param {void *composite_ptr}
%parse-param {const char *composite_type}
%parse-param {const char **hintmsg}
%parse-param {int flags}
%parse-param {yyscan_t yyscanner}
%lex-param   {const char **hintmsg}
%lex-param   {yyscan_t yyscanner}
%pure-parser
%expect 0
%name-prefix="guc_composite_yy"

%union
{
	char	   *str;
}

%token <str> IDENT JUNK

%type patch_list
%type composite
%type list_or_empty
%type list
%type item

%start patch_list

%initial-action
{
    init_context(composite_type, composite_ptr);
}
%%
patch_list:
    composite                               { init_context(composite_type, composite_ptr); }
    ';' patch_list
    | composite

composite:
    '{'                                     {
                                                prepare_value_context(yyscanner, hintmsg);
                                                prepare_structure();
                                            }
    list_or_empty                           { parse_composite_end(); }
    '}'
    | '['                                   {
                                                prepare_value_context(yyscanner, hintmsg);
                                                prepare_array();
                                            }
    list_or_empty                           { parse_composite_end(); }
    ']'
    | IDENT                                 {
                                                prepare_value_context(yyscanner, hintmsg);
                                                parse_simple_opt($1, context(0)->type, context(0)->start, flags, yyscanner, hintmsg);
                                                parse_composite_end();
                                            }
    ;

list_or_empty:
    list
    | %empty
    ;

list:
    item                                    { parse_field_end(); }
    ',' list
    | item
    ;

item:
    IDENT ':'                               { parse_name($1, yyscanner, hintmsg); }
    composite
    | composite
    ;
%%

static void init_context(const char *type, void *ptr)
{
    free_context_list();
    push_context(type, ptr);
}

static void push_context(const char* type, void *start)
{
    parser_ctx *ctx = palloc(sizeof(parser_ctx));
    if (type)
        ctx->type = pstrdup(type);
    else
        ctx->type = NULL;
    ctx->start = start;
    ctx->idx = 0;
    ctx->has_name = false;
    ctx->extended = false;
    ctx->fixed_size = FIXED_SIZE_IS_NOT_SETTED;
    ctx->max_idx = -1;
    ctx->name_usage = NAME_USAGE_UNKNOWN;
    contexts = lcons(ctx, contexts);
}

static void free_context(parser_ctx *context)
{
    pfree(context->type);
    pfree(context);
}

static void free_context_list(void)
{
    while (!list_empty(contexts))
    {
        free_context((parser_ctx *)list_nth(contexts, 0));
        contexts = list_delete_first(contexts);
    }
    list_free(contexts);
}


static void check_name(yyscan_t yyscanner, const char **hintmsg)
{
    /* Indexes in array are exist either for each element or for no one */
    if (is_static_array_type(context(1)->type) ||
        is_dynamic_array_type(context(1)->type))
    {
        if (context(0)->has_name)
        {
            if (context(1)->name_usage == NAME_USAGE_NEVER)
                guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "use indexes for either everyone or no one");
            context(1)->name_usage = NAME_USAGE_ALWAYS;
        }
        else
        {
            if (context(1)->name_usage == NAME_USAGE_ALWAYS)
                guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "use indexes for either everyone or no one");
            context(1)->name_usage = NAME_USAGE_NEVER;

            if (context(1)->start)
                context(0)->start = (char *)context(1)->start + get_element_offset_with_index(context(1)->type, context(1)->idx);
        }
    }
    else if (!context(0)->has_name) /* fields of structures must be labeled always */
        guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "all fields in structure must be labeled");
}

static void reallocate_dynamic_array(const char *array_type, void *array, int new_len)
{
    int new_arr_mem_size = get_dynamic_array_mem_size_with_length(array_type, new_len);
    int last_len = dynamic_array_size(array);
    int last_arr_mem_size = get_dynamic_array_mem_size_with_length(array_type, last_len);
    int min_arr_mem_size = new_arr_mem_size < last_arr_mem_size ? new_arr_mem_size : last_arr_mem_size;
    void *new_data = guc_malloc(ERROR, new_arr_mem_size);

    if (new_len < last_len)
    {
        /* free elements from deleted part */
        char *basic_type = get_array_basic_type(array_type);
        for (int i = new_len; i < last_len; i++)
        {
            int offset = get_element_offset_with_index(array_type, i);
            free_aux_struct_mem((char *)(*(void**)array) + offset, basic_type);
        }
        guc_free(basic_type);
    }
    else
        memset((char *)new_data + last_arr_mem_size, 0, new_arr_mem_size - last_arr_mem_size);

    memcpy(new_data, *(void**)array, min_arr_mem_size);
    guc_free(*(void **)array);
    *(void**)array = new_data;
    dynamic_array_size(array) = new_len;
}

static void check_memory(yyscan_t yyscanner, const char **hintmsg)
{
    int len = 0;
    int idx = 0;

    /*
     * next part of function process a case, when
     * we expect parsing elements of data of dynamic array
     * therefore context(1) must be not extended (i.e. inner context)
     *
     * If context(1) is extended dynamic array (i.e. outer context),
     * we are going to parse "data" or "size" field
     */
    if (!(is_dynamic_array_type(context(1)->type) && !context(1)->extended))
        return;

    len = dynamic_array_size(context(2)->start);
    idx = context(1)->idx;
    if (idx > context(2)->max_idx)
        context(2)->max_idx = idx;

    if (idx >= len)
    {
        if (context(2)->fixed_size == FIXED_SIZE_IS_SETTED)
            guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "there is index greater than array length. Change \"size\" field");
        reallocate_dynamic_array(context(2)->type, context(2)->start, idx + 1);
        /*update contexts about dynamic array*/
        context(1)->start = *(void **)context(2)->start;
        context(0)->start = (char *)context(1)->start + get_element_offset_with_index(context(1)->type, context(1)->idx);
    }
}

/*
 * Check all conditions related to context: name, allocated memory
 */
static void prepare_value_context(yyscan_t yyscanner, const char **hintmsg)
{
    /* top-level structure always is okey */
    if (list_length(contexts) == 1)
        return;
    check_name(yyscanner, hintmsg);
    check_memory(yyscanner, hintmsg);
}


/*
 * Prepare context for structure. Add new layer in stack that will be filled by parse_name
 */
static void prepare_structure(void)
{
    /* if type is dynamic array => that is extended form of array */
    if (is_dynamic_array_type(context(0)->type))
        context(0)->extended = true;
    /* add context for structure field */
    push_context(NULL, context(0)->start);
}

/*
 * Prepare context for array. Add new layer to stack that will be filled by parse_name.
 * In case of parsing dynamic array we must guarantee that
 * current context->start points to pointer to data of dynamic array
 */
static void prepare_array(void)
{
    void *data = NULL;
    char *basic_type = get_array_basic_type(context(0)->type);
    if (!is_dynamic_array_type(context(0)->type))
    {
        push_context(pstrdup(basic_type), context(0)->start);
        goto out;
    }
    /*
     * Each dynamic array has 2 contexts: outer context for structure {data, size}
     * And inner context for allocated data. Because of different ways to set
     * dynamic array (extended and compact forms) outer context of dynamic array
     * has flag "extended". When we pop from stack, we see this flag and for
     * compact representation of array we pop twice (for purpose of popping
     * inner and outer contexts at one time)
     */
    data = *(void **)context(0)->start;
    if (data)
        push_context(context(0)->type, data);
    else
        push_context(context(0)->type, NULL);

    /*
     * Current dynamic array could be empty. In this case create fictitious stack layer
     * (NULL in start field means that now we parse element of empty dynamic array)
     * for purposes of recursive algorithm
     */
    if (data)
        push_context(pstrdup(basic_type), data);
    else
        push_context(pstrdup(basic_type), NULL);
out:
    guc_free(basic_type);
}


/*
 * Update context when parser go out of nested
 * level of structure. So, rewind stack of contexts.
 */
static void parse_composite_end(void)
{
    /*
     * is_dynamic is used to not miss in case:
     * Delete context and view not extended dynamic array. So that is inner or outer context?
     * If is_dynamic == true => that is outer context
     * Else that is inner context
     */
    bool is_dynamic = is_dynamic_array_type(context(0)->type);
    free_context((parser_ctx *)list_nth(contexts, 0));
    contexts = list_delete_first(contexts);
    /* Free up outer context for dynamic array in compact form.
     * See comments in parse_array function
     */
    if (!list_empty(contexts) && is_dynamic &&
        is_dynamic_array_type(context(0)->type) && !context(0)->extended)
    {
        free_context((parser_ctx *)list_nth(contexts, 0));
        contexts = list_delete_first(contexts);
    }
}

/*
 * Update context before parser go to parse next field
 */
static void parse_field_end(void)
{
    context(0)->idx++;  /* context of structure/array */
    if (is_dynamic_array_type(context(0)->type) || is_static_array_type(context(0)->type))
    {
        void *data = *(void **)context(0)->start;
        char *basic_type = get_array_basic_type(context(0)->type);
        if (data)
            push_context(pstrdup(basic_type), data);
        else
            push_context(pstrdup(basic_type), NULL);
        guc_free(basic_type);
    }
    else
        push_context(NULL, context(0)->start);
    context(0)->has_name = false; /* context of field/element */
}


static int parse_index(const char *index, yyscan_t yyscanner, const char **hintmsg)
{
    for (const char *c = index; *c; c++)
    {
        if (!isdigit(*c))
            guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "incorrect index");
    }
    return atoi(index);
}


/*
 * Parse index and compute offset in array
 * Fill current context
 */
static void parse_element(const char *index, yyscan_t yyscanner, const char **hintmsg)
{
    int idx = parse_index(index, yyscanner, hintmsg);
    /* for static array check len */
    if (is_static_array_type(context(1)->type))
    {
        int len = get_static_array_size(context(1)->type);
        if (idx >= len)
            guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "there is index which is greater than size of array");
    }
    context(1)->idx = idx;
    context(0)->start = (char *)context(1)->start + get_element_offset_with_index(context(1)->type, idx);
}


/*
 * Parse name, check correctness
 * Fill current context
 */
static void parse_field(const char *name, yyscan_t yyscanner, const char **hintmsg)
{
    char *field_type;
    int offset = get_field_offset(context(1)->type, name);
    if (offset < 0)
        guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "incorrect field name");
    /* create new context */
    field_type = get_field_type_name(context(1)->type, name);
    context(0)->type = pstrdup(field_type);
    context(0)->start = (char *)context(1)->start + offset;
    guc_free(field_type);
}


/*
 * Compute the pointer to field by name of field
 * fill current context
 */
static void parse_name(const char *name, yyscan_t yyscanner, const char **hintmsg)
{
    /* Name could be an index for arrays */
    if (is_static_array_type(context(1)->type) ||
        (is_dynamic_array_type(context(1)->type) && context(1)->extended == false))
    {
        if (context(1)->name_usage == NAME_USAGE_NEVER)
            guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "use indexes for either everyone or no one");

        context(1)->name_usage = NAME_USAGE_ALWAYS;
        parse_element(name, yyscanner, hintmsg);
    }
    else
        parse_field(name, yyscanner, hintmsg);
    if (strcmp(name, "size") == 0 && context(1)->extended == true)
            context(1)->fixed_size = FIXED_SIZE_IS_BEING_SETTED;
    context(0)->has_name = true;
}


static void parse_simple_opt(char *strval, const char *struct_type, void *result, int flags, yyscan_t yyscanner, const char **hintmsg)
{
	if (!strcmp(struct_type, "bool"))
	{
		if (!parse_bool(strval, (bool *)result))
			guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "failed to parse bool value, use 'on' and 'off'");
	}
	else if (!strcmp(struct_type, "int"))
	{
        /*
         * Block of code for field "size" in dynamic array
         * This block of code must be above parse_int(). Because standard parsing will
         * overwrite old lenght, but we need it in reallocate_dynamic_array()
         */
        if (list_length(contexts) > 1 && context(1)->fixed_size == FIXED_SIZE_IS_BEING_SETTED)
        {
            int len = parse_index(strval, yyscanner, hintmsg);
            if (*hintmsg)
                return;
            reallocate_dynamic_array(context(1)->type, context(1)->start, len);
        }

		if (!parse_int(strval, (int *)result, flags, hintmsg))
			guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "failed to parse int value, check units");
	}
	else if (!strcmp(struct_type, "real"))
	{
		if (!parse_real(strval, (double *)result, flags, hintmsg))
			guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "failed to parse real value, check delimiter");
	}
	else if (!strcmp(struct_type, "string"))
	{
		if (!strcmp(strval, "\\nil"))
				*((char **)result) = NULL;
		else
        {
            if (*((char **)result))
                guc_free(*((char **)result));
            *((char **)result) = guc_strdup(ERROR, strval);
        }
	}
	else
		guc_composite_yyerror(last_context->start, last_context->type, hintmsg, 0, yyscanner, "failed to determine type of simple field");
}

/*
 * Entry point in parsing structure. This function is used to parse composite objects.
 * It allocate memory for the new object and uses parse_compoite_impl.
 * It also is used to parse placeholder patch list
 */
bool parse_composite(const char *strvalue, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg)
{
	int size = 0;
	yyscan_t scanner;
	void *val = NULL;
    int parser_result = 0;
	bool check = true;

	*hintmsg = NULL;
	size = get_type_size(type);
	val = guc_malloc(ERROR, size);

	if (prev_val)
		struct_dup_impl(val, prev_val, type);
	else
		memset(val, 0, size);

    guc_composite_scanner_init(strvalue, &scanner);
    parser_result = guc_composite_yyparse(val, type, hintmsg, flags, scanner);
    guc_composite_scanner_finish(scanner);
    free_context_list();

    if (parser_result != 0 || *hintmsg)
	{
		guc_free(val);
		*result = NULL;
		check = false;
	}
    else
        *result = val;
	return check;
}


/*
 * Functions examine string and decides that is recovery of placeholder (assignment list) or structure definition
 * assignemt list has signature: <path>=<value>;...;<path>=<value>;
 * maybe we should use signature: ;<path>=<value>;...;<path>=<value> when we will check first simbol, but this form is unusual
 */
static bool is_assignment_list(const char *value) {
	return ';' == value[strlen(value) - 1];
}


/*
 * Placeholder patch list is used to support incremental semantic
 * for composite types placeholders.
 * Function parses assignment list in the way:
 * 1. slpit assignement by ';'
 * 2. assign patch
 */
static bool parse_placeholder_patch_list(const char *value, const char *type, void **result, const void *prev_val, int flags, const char **hintmsg) {
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