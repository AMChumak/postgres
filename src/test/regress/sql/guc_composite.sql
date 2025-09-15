-- STRUCT
SHOW replica;

SET replica->max_delay to 42;
SHOW replica->max_delay;

SET replica to {enable_connections: true, max_slot_size: 10};
SHOW replica;
SET replica->invalid_field to 4;

-- STATIC ARRAY
SHOW log_level_names;
SET log_level_names[0] to 'DEBUG';
SHOW log_level_names[0];
SET log_level_names[10] to 'EXTRA';
SET log_level_names[-1] to 'EXTRA';

-- DYNAMIC ARRAY

SHOW shared_preload_libraries_list;

SET shared_preload_libraries_list[0] to 'yet_another_ext';
SHOW shared_preload_libraries_list->data[0];

SET extended_guc_arrays to true;
SHOW shared_preload_libraries_list;

SET shared_preload_libraries_list->size to 2;
SHOW shared_preload_libraries_list;

SET shared_preload_libraries_list to {size: 2, data: [3: 'third_ext']};
SET extended_guc_arrays to false;

-- CHECK GUC STACK

SHOW replica;
BEGIN;
SET replica->max_delay to 6;
SHOW replica;
COMMIT;
SHOW replica;

SHOW replica;
BEGIN;
SET replica->max_delay to 28;
SHOW replica;
ABORT;
SHOW replica;

SHOW replica;
BEGIN;
SET LOCAL replica->max_delay to 28;
SHOW replica;
COMMIT;
SHOW replica;
