# Copyright (c) 2022-2025, PostgreSQL Global Development Group

backend_sources += files(
  'conffiles.c',
  'guc_composite.c',
  'guc.c',
  'guc_funcs.c',
  'guc_tables.c',
  'help_config.c',
  'injection_point.c',
  'pg_config.c',
  'pg_controldata.c',
  'pg_rusage.c',
  'ps_status.c',
  'queryenvironment.c',
  'rls.c',
  'sampling.c',
  'stack_depth.c',
  'superuser.c',
  'timeout.c',
  'tzparser.c',
)

# see ../../parser/meson.build
guc_composite_parser_sources = []

guc_composite_scan = custom_target('guc_composite_scan',
  input: 'guc_composite_scan.l',
  output: 'guc_composite_scan.c',
  command: flex_cmd,
)
generated_sources += guc_composite_scan
guc_composite_parser_sources += guc_composite_scan

guc_composite_gram = custom_target('guc_composite_gram',
  input: 'guc_composite_gram.y',
  kwargs: bison_kw,
)
generated_sources += guc_composite_gram.to_list()
guc_composite_parser_sources += guc_composite_gram

backend_link_with += static_library('guc_composite_parser',
  guc_composite_parser_sources,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)

guc_scan = custom_target('guc_scan',
  input: 'guc-file.l',
  output: 'guc-file.c',
  command: flex_cmd)
generated_sources += guc_scan

# so we don't need to add . as an include dir for the whole backend
backend_link_with += static_library('guc-file',
  guc_scan,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)

install_data('postgresql.conf.sample',
  install_dir: dir_data,
)
