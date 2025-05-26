create function hello_world() returns text AS $$
BEGIN
    RETURN 'Hello world';
END;
$$ LANGUAGE plpgsql;
