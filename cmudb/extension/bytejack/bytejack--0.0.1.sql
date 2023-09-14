CREATE
OR REPLACE FUNCTION bytejack_cache_clear(OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/bytejack', 'bytejack_cache_clear';

CREATE
OR REPLACE FUNCTION bytejack_connect(OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/bytejack', 'bytejack_connect';

CREATE
OR REPLACE FUNCTION bytejack_disconnect(OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/bytejack', 'bytejack_disconnect';

CREATE
OR REPLACE FUNCTION bytejack_save(IN dbname text, OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/bytejack', 'bytejack_save';