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

CREATE TABLE public.us_crosswalk
(
    id BIGINT NOT NULL DEFAULT nextval('us_crosswalk_id_seq'::regclass),
    factual_id VARCHAR(50) NOT NULL,
    facebook_id VARCHAR(100),
    facebook_url VARCHAR(250),
    twitter_id VARCHAR(100),
    twitter_url VARCHAR(250)
);

CREATE TABLE public.places_category
(
    id BIGINT NOT NULL DEFAULT nextval('places_category_id_seq'::regclass),
    place_id  bigint,
    category_id bigint
);

CREATE TABLE public.categories
(
    category_id BIGINT NOT NULL PRIMARY KEY,
    description TEXT
);

CREATE TABLE public.zipcode
(
    id BIGINT NOT NULL DEFAULT nextval('zipcode_id_seq'::regclass),
    zipcode INT,
    population INT
)