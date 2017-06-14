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
ALTER TABLE public.us_places ADD PRIMARY KEY (id);

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
ALTER TABLE public.us_crosswalk ADD PRIMARY KEY (id);


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

DROP TABLE public.emails;
CREATE TABLE public.emails
(
    id BIGINT NOT NULL DEFAULT nextval('emails_id_seq'::regclass),
    email VARCHAR(100) NOT NULL,
    domain VARCHAR(250),
    type  VARCHAR(100),
    confidence int,
    sources TEXT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    position VARCHAR(250),
    linkedin VARCHAR(250),
    twitter VARCHAR(250),
    phone_number VARCHAR(50),
    webmail BOOLEAN,
    pattern VARCHAR(250),
    organization VARCHAR(250),
    category_id INT
);

DROP INDEX public.zipcode_idx;
CREATE INDEX zipcode_idx
    ON public.zipcode USING btree
    (zipcode)
    TABLESPACE pg_default;

create table public.data_source_227 as
    select pl.id, pl.factual_id, pl.name, pl.address, pl.address_extended, pl.po_box, pl.locality, pl.region, pl.post_town, pl.admin_region, pl.post_code,
    pl.country, pl.tel, pl.fax, pl.latitude, pl.longitude, pl.neighborhood, pl.website, pl.email, pl.category_ids, pl.category_lables, pl.chaine_name, pl.chain_id,
    pl.hours, pl.hours_display, pl.existence, zip.population
	from public.us_places as pl
    inner join public.zipcode as zip on nullif(pl.post_code, '')::int = zip.zipcode
    inner join public.places_category as pc on pl.id = pc.place_id
    where (pl.website = '') is false and pc.category_id = 227
    order by zip.population desc;

ALTER TABLE public.data_source_227 ADD COLUMN processed BOOLEAN DEFAULT(FALSE);
ALTER TABLE public.data_source_227 ADD COLUMN emails_number int;

select _inner.website
from (SELECT distinct on (website) id, factual_id, name, address, address_extended,
 po_box, locality, region, post_town, admin_region, post_code, country, tel, fax, latitude, longitude, neighborhood,
 substring(website from '^(?:https?:\/\/)?(?:[^@\n]+@)?(?:www\.)?([^:\/\n\?]+\.com|[^:\/\n\?]+\.net|[^:\/\n\?]+\.org)(?:[^\.]+)?$') as website, email, category_ids,
        category_lables, chaine_name, chain_id, hours, hours_display, existence, population, processed
      FROM public.data_source_227
      where processed = FALSE
      order by website, population desc) as _inner
  order by _inner.population desc
LIMIT 2500



