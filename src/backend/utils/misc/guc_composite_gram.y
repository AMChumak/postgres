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
#include "utils.guc.h"
#include "guc_composite.h"
#include "guc_composite_gram.h"

static SyncRepConfigData *create_syncrep_config(const char *num_sync,
					List *members, uint8 syncrep_method);

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

#define context(num) ((parser_ctx *)list_nth(contexts, num))

/*
 * Stack is needed to memoize valuable data between nested layers in composite object
 */
enum name_usage
{
    NAME_USAGE_UNKNOWN,
    NAME_USAGE_ALWAYS,
    NAME_USAGE_NEVER
};

typedef struct parser_ctx
{
    char *type;
    void *start;
    int  idx;
    enum name_usage name_usage;
} parser_ctx;

List *contexts = NIL;      /* stack of contexts */

%}

%parse-param {void *composite_ptr}
%parse-param {char *composite_type}
%parse-param {char **hintmsg}
%parse-param {yyscan_t yyscanner}
%lex-param   {char **hintmsg}
%lex-param   {yyscan_t yyscanner}
%pure-parser
%expect 0
%name-prefix="composite_yy"


%union
{
	char	   *str;
}

%token <str> IDENT JUNK


%type composite
%type list
%type name

%start composite


%initial-action
{
    /* initialize context */

    parser_ctx *ctx = palloc(sizeof(parser_ctx));

    ctx->type = pstrdup(composite_type);
    ctx->start = composite_ptr;
    ctx->idx = 0;
    ctx->name_usage = NAME_USAGE_UNKNOWN;

    contexts = lcons(ctx, contexts);
}

%%

composite:
            '{' list                { parse_composite_end(); }
            '}'
            | '[' list              { parse_composite_end(); }
            ']'
            | IDENT                 {
                                        parse_simple_opt($1, context(0)->type, context(0)->start, hintmsg);
                                        parse_composite_end();
                                    }
            ;

list:
            name composite          { parse_field_end(); }
            ',' list
            | name composite        { parse_field_end(); }
            ;

name:
    IDENT ':'                       { parse_name($1); }
    |                               { parse_empty_prefix(); }

%%

void parse_composite_end(void)
{
    /* rewind stack */
    free_context((parser_ctx *)list_nth(contexts, 0));
    contexts = list_delete_first(contexts);

    if (list_empty(contexts))
        list_free(contexts);
}

void parse_field_end(void)
{
    /* update index */
    context(0)->index++;
}


int parse_index(const char *index)
{
    for (const char *c = index; *c; c++)
        if (!is_digit(*c))
                parser_abort(""); /* log location and error type */

    return atoi(index);
}

parse_res parse_element(const char *index)
{
    int idx = context(0)->idx;
    int len;
    char *basic_type;

    if (index)
        idx = parse_index(index);

    /* get length for array */
    if (is_dynamic_array_type(context(0)->type))
        len = dynamic_array_size(context(0)->start);
    else if (is_static_array_type(context(0)->type))
        len = get_static_array_size(context(0)->type);
    else
        parser_abort(""); /* log location and error type */

    /* examine index */
    if (idx >= len)
        parser_abort(""); /* log location and error type */

    /* create new context */
    parser_ctx *ctx = palloc(sizeof(parser_ctx));
    basic_type = get_array_basic_type(context(0)->type);
    ctx->type = pstrdup(basic_type);
    ctx->start = (char *)context(0)->start + get_element_offset_with_index(context(0)->type, idx);
    ctx->idx = 0;
    ctx->name_usage = NAME_USAGE_UNKNOWN;
    contexts = lcons(ctx, contexts);

    guc_free(basic_type);
}

parse_res parse_field(const char *name)
{
    char *field_type;
    int offset = get_field_offset(context(0)->type, name);
    if (offset < 0)
        parser_abort(""); /* log location and error type */

    /* create new context */
    parser_ctx *ctx = palloc(sizeof(parser_ctx));
    field_type = get_field_type_name(context(0)->type, name);
    ctx->type = pstrdup(field_type);
    ctx->start = (char *)context(0)->start + offset;
    ctx->idx = 0;
    ctx->name_usage = NAME_USAGE_UNKNOWN;
    contexts = lcons(ctx, contexts);

    guc_free(field_type);
}

void parse_name(const char *name)
{
    parse_res name_res;

    if (is_static_array_type(context(0)->type) || is_dynamic_array_type(context(0)->type))
    {
        if (context(0)->name_usage == NAME_USAGE_NEVER)
            parser_abort(""); /* log location and error type */

        context(0)->name_usage = NAME_USAGE_ALWAYS;
        name_res = parse_element($1);
    }
    else
        name_res = parse_field($1);

    if (!IS_STATUS_OK(name_res))
        parser_abort(""); /* log location and error type */
}

void parse_empty_prefix(void)
{
    parse_res name_res;

    if (is_static_array_type(context(0)->type) || is_dynamic_array_type(context(0)->type))
    {
        if (context(0)->name_usage == NAME_USAGE_ALWAYS)
            parser_abort(""); /* log location and error type */

        context(0)->name_usage = NAME_USAGE_NEVER;
    }
    else
        parser_abort(""); /* log location and error type */

    name_res = parse_element(NULL);
}

parser_res parse_simple_opt(char *strval, const char *struct_type, void *result, const char **hintmsg)
{
	parser_res parser_result = {};
    parser_result.parse_end = strval + strlen(strval) - 1;

	/* set OK status, ruin status in error case */
	parser_result.status = PARSER_OK;

	if (!strcmp(struct_type, "bool"))
	{
		if (!parse_bool(strval, (bool *)result))
		{
			*hintmsg = gettext_noop("failed to parse bool value, use 'on' and 'off'");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("failed to parse bool value: %s", strval),
					errhint("use 'on' or 'off'")));
			parser_result.status = PARSER_ERR;
		}
	}
	else if (!strcmp(struct_type, "int"))
	{
		if (!parse_int(strval, (int *)result, flags, hintmsg))
		{
			*hintmsg = gettext_noop("failed to parse int value, check units");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("failed to parse int value: %s", strval),
					errhint("check unit, symbols")));
			parser_result.status = PARSER_ERR;
		}
	}
	else if (!strcmp(struct_type, "real"))
	{
		if (!parse_real(strval, (double *)result, flags, hintmsg))
		{
			*hintmsg = gettext_noop("failed to parse real value, check delimiter");
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("failed to parse real value: %s", strval),
					errhint("use dot to delimit ")));
			parser_result.status = PARSER_ERR;
		}
	}
	else if (!strcmp(struct_type, "string"))
	{
		if (!strcmp(strval, "\\nil"))
		{
				*((char **)result) = NULL;
				parser_result.status = PARSER_OK;
		}
		else
		*((char **)result) = guc_strdup(ERROR, strval);
	}
	else
	{
		*hintmsg = gettext_noop("failed to determine type of simple field");
		parser_result.status = PARSER_ERR;
	}

	return parser_result;
}

void free_context(parser_ctx *context)
{
    pfree(context->type);
    pfree(context);
}

void free_context_list(List *contexts)
{
    if (context == NIL)
        return;

    while (!list_empty(contexts))
    {
        free_context((parser_ctx *)list_nth(contexts, 0));
        contexts = list_delete_first(contexts);
    }
    list_free(contexts);
}


void parser_abort(const char *msg)
{
    free_context_list(contexts);
    YYABORT;
}

void composite_yycleanup()
{
    free_context_list(contexts);
}