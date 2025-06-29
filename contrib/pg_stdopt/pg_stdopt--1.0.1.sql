create function check_init() returns text AS $$
BEGIN
    RETURN 'Extension pg_stdopt for standard optimizer was included successfully!';
END;
$$ LANGUAGE plpgsql;