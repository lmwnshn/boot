CREATE
    OR REPLACE FUNCTION boot_cache_clear(OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/boot', 'boot_cache_clear';

CREATE
    OR REPLACE FUNCTION boot_connect(OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/boot', 'boot_connect';

CREATE
    OR REPLACE FUNCTION boot_disconnect(OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/boot', 'boot_disconnect';

CREATE
    OR REPLACE FUNCTION boot_save(IN dbname text, OUT status bool)
    RETURNS bool
    LANGUAGE C STRICT
AS '$libdir/boot', 'boot_save';
