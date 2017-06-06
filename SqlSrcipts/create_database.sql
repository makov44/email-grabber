CREATE DATABASE email_grabber
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

GRANT TEMPORARY, CONNECT ON DATABASE email_grabber TO PUBLIC;

GRANT ALL ON DATABASE email_grabber TO postgres;