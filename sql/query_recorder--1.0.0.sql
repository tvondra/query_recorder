
CREATE OR REPLACE FUNCTION query_recorder_flush()
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_buffer_flush'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION query_recorder_reset()
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_buffer_reset'
    LANGUAGE C IMMUTABLE;
