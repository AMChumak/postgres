%top{
/*-------------------------------------------------------------------------
 *
 * guc_composite_scan.l
 *	  a lexical scanner for all composite guc values
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_composite_scan.l
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/guc.h"
#include "guc_composite.h"
#include "guc_composite_gram.h"

#define YY_DECL extern int	guc_composite_yylex(union YYSTYPE *yylval_param, const char **hintmsg, yyscan_t yyscanner)

}

%option reentrant
%option bison-bridge
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option warn
%option prefix="guc_composite_yy"

SPACE			[ \t\n\r\f\v]

LETTER_OR_DIGIT [A-Za-z_0-9\200-\377]

STRING          \'([^'\\\n]|\\.|\'\')*\'
UNQUOTED_STRING ({LETTER_OR_DIGIT}|\.)*


%%

{SPACE}         /* Eat whitespace */


{STRING}            {
                        /* Deescape string and return value */
                        yylval->str = DeescapeQuotedString(yytext);
                        return IDENT;
                    }

{UNQUOTED_STRING}   {
                        yylval->str = pstrdup(yytext);
                        return IDENT;
                    }

"{"                 { return '{'; }
"}"                 { return '}'; }
"["                 { return '['; }
"]"                 { return ']'; }
","                 { return ','; }
":"                 { return ':'; }
";"                 { return ';'; }

.					{
						guc_composite_yyerror(NULL, NULL, hintmsg, 0, yyscanner, "illegible lexeme");
						return JUNK;
					}

%%

/* functions  for lexer */


void guc_composite_scanner_init(const char *str, yyscan_t *yyscannerp)
{
	yyscan_t	yyscanner;

	if (yylex_init(yyscannerp) != 0)
		elog(ERROR, "yylex_init() failed: %m");

	yyscanner = *yyscannerp;

	yy_scan_string(str, yyscanner);
}

void guc_composite_scanner_finish(yyscan_t yyscanner)
{
	yylex_destroy(yyscanner);
}

void guc_composite_yyerror(void *composite_ptr, const char *composite_type, const char **hintmsg, int flags, yyscan_t yyscanner, const char *message)
{
	struct yyguts_t *yyg = (struct yyguts_t *) yyscanner;	/* needed for yytext
															 * macro */
	/* report only the first error in a parse operation */
	if (*hintmsg)
		return;
	if (yytext[0])
		*hintmsg = psprintf("%s at or near \"%s\"",
										   message, yytext);
	else
		*hintmsg = psprintf("%s at end of input",
										   message);
}


/*
 * Interface functions to make flex use palloc() instead of malloc().
 * It'd be better to make these static, but flex insists otherwise.
 */

void *yyalloc(yy_size_t size, yyscan_t yyscanner)
{
	return palloc(size);
}

void *yyrealloc(void *ptr, yy_size_t size, yyscan_t yyscanner)
{
	if (ptr)
		return repalloc(ptr, size);
	else
		return palloc(size);
}

void yyfree(void *ptr, yyscan_t yyscanner)
{
	if (ptr)
		pfree(ptr);
}