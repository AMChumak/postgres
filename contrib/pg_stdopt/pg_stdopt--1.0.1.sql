create function hello_cworld() returns text AS 'MODULE_PATHNAME', 'hello_cworld' LANGUAGE C;

create function hello_world() returns text AS $$
BEGIN
    RETURN 'Hello world';
END;
$$ LANGUAGE plpgsql;