DROP TABLE public.us_places;
CREATE TABLE public.us_places
(
    id BIGINT NOT NULL DEFAULT nextval('us_places_id_seq'::regclass),
    factual_id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    address VARCHAR(255),
    address_extended VARCHAR(255),
    po_box VARCHAR(50),
    locality VARCHAR(50),
    region VARCHAR(25),
    post_town VARCHAR(50),
    admin_region VARCHAR(200),
    post_code VARCHAR(50),
    country VARCHAR(50),
    tel VARCHAR(50),
    fax VARCHAR(50),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    neighborhood TEXT,
    website VARCHAR(500),
    email VARCHAR(100),
    category_ids TEXT,
    category_lables TEXT,
    chaine_name VARCHAR(255),
    chain_id VARCHAR(100),
    hours Text,
    hours_display Text,
    existence VARCHAR(255)
);

DROP TABLE public.us_crosswalk;
CREATE TABLE public.us_crosswalk
(
    id BIGINT NOT NULL DEFAULT nextval('us_crosswalk_id_seq'::regclass),
    factual_id VARCHAR(50) NOT NULL,
    facebook_id VARCHAR(100),
    facebook_url VARCHAR(250),
    twitter_id VARCHAR(100),
    twitter_url VARCHAR(250)
);


DROP INDEX public.factualid_idx;
CREATE INDEX factualid_idx
    ON public.us_crosswalk USING btree
    (factual_id COLLATE pg_catalog."default")
    TABLESPACE pg_default;


DROP TABLE public.places_category;
CREATE TABLE public.places_category
(
    id BIGINT NOT NULL DEFAULT nextval('places_category_id_seq'::regclass),
    place_id  bigint,
    category_id bigint
);

DROP INDEX public.categoryid_idx;
CREATE INDEX categoryid_idx
    ON public.places_category USING btree
    (category_id)
    TABLESPACE pg_default;

DROP INDEX public.placeid_idx;
CREATE INDEX placeid_idx
    ON public.places_category USING btree
    (place_id)
    TABLESPACE pg_default;

DROP TABLE public.categories;
CREATE TABLE public.categories
(
    category_id BIGINT NOT NULL PRIMARY KEY,
    description TEXT
);

DROP TABLE public.zipcode;
CREATE TABLE public.zipcode
(
    id BIGINT NOT NULL DEFAULT nextval('zipcode_id_seq'::regclass),
    zipcode INT,
    population INT
);

DROP INDEX public.zipcode_idx;
CREATE INDEX zipcode_idx
    ON public.zipcode USING btree
    (zipcode)
    TABLESPACE pg_default;
