create function get_logs_count() returns int AS 'MODULE_PATHNAME', 'get_logs_count' LANGUAGE C;

create function access_scan_column(relation oid, collumn text) returns void AS 'MODULE_PATHNAME', 'access_scan_column' LANGUAGE C;