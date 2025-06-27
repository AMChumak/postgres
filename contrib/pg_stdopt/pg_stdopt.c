#include "postgres.h"
#include "utils/builtins.h"
#include "c.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(hello_cworld);

Datum hello_cworld(PG_FUNCTION_ARGS) {
    PG_RETURN_TEXT_P(cstring_to_text("hello from pg_stdopt!"));
}