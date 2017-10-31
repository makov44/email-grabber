Copy (select first_name, last_name, phone_number, position, organization, email
 from public.emails where first_name = '' is false and last_name = '' is false
 and emails_number<= 60 and confidence >= 10 and category_id = 299) To '/tmp/emails_299.csv' With CSV DELIMITER ',';

Copy (
    select first_name, last_name, phone_number, position, organization, email, ct.description
	 from public.emails as e
     left join public.categories as ct on e.category_id = ct.category_id
    where e.category_id in (219, 221, 226, 227) ORDER BY email LIMIT 90000
) To '/tmp/emails_finance_1.csv' With CSV DELIMITER ',' HEADER;

pg_dump --file "/tmp/emails_433911" --host "198.199.97.248" --port "5432" --username "postgres" --no-password --verbose --format=c --blobs --table "public.emails" "email_grabber"

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
ALTER TABLE public.data_source_227 ADD COLUMN domain varchar(150);
UPDATE public.data_source_277 SET domain = substring(website from
'^(?:https?:\/\/)?(?:www\.)?(?:[-0-9A-Za-z_]{1,}\.)*([-0-9A-Za-z_]{1,}\.com|[-0-9A-Za-z_]{1,}\.net|[-0-9A-Za-z_]{1,}\.org|[-0-9A-Za-z_]{1,}\.edu|[-0-9A-Za-z_]{1,}\.gov)(?:.+)?$');

select _inner.domain, _inner.population
from (SELECT distinct on (domain) id, factual_id, name, address, address_extended, po_box, locality, region,
 post_town, admin_region, post_code, country, tel, fax, latitude, longitude, neighborhood, website, email,
 category_ids, category_lables, chaine_name, chain_id, hours, hours_display, existence, population, processed,
  emails_number, domain
 FROM public.data_source_299
 where processed = true and domain is not NULL
    order by domain, population desc) as _inner
    order by _inner.population desc;

select c.description, c.category_id, v._count from
  (select t.category_id , count(*) as _count from
  (select * from public.emails where
   first_name = '' is false and last_name = '' is false and emails_number<= 60 and confidence >= 10 and position  = '' is false) as t
  group by t.category_id order by t.category_id) as v
  left join categories as c on c.category_id = v.category_id order by v.category_id;

 select first_name, last_name, phone_number, position, organization, email
 from public.emails where first_name = '' is false and last_name = '' is false
 and emails_number<= 60 and confidence >= 10 and category_id = 26;

 select * from public.emails where first_name = '' is false and last_name = '' is false and emails_number >= 1 and emails_number< 60 and confidence >10
 UNION all
 select count(*) from public.emails where first_name = '' is false and last_name = '' is false and emails_number >= 60 and emails_number< 100
 UNION all
 select count(*) from public.emails where first_name = '' is false and last_name = '' is false and emails_number >= 100 and emails_number< 200
 UNION all
 select count(*) from public.emails where first_name = '' is false and last_name = '' is false and emails_number >= 200;


SELECT * FROM public.emails
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY email ORDER BY id) AS rnum
                     FROM public.emails) t
              WHERE t.rnum > 1);


CREATE OR REPLACE FUNCTION public.get_processed_data(category_limit integer
	)
    RETURNS  TABLE (
    data_source   VARCHAR(100)
  , category text
  , emails_number bigint
  , processed   bigint
  , not_processed bigint)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$

DECLARE counter integer := 0;
		my_table_name VARCHAR(100) := '';
		processed bigint;
		not_processed bigint;
BEGIN
    DROP TABLE IF EXISTS temp_result;
 		CREATE TEMP TABLE temp_result
        (
             data_source VARCHAR(100),
             proccesed bigint,
             not_processed bigint,
             category_id int
        );
        LOOP
            counter := counter + 1;
            my_table_name := 'data_source_' || counter::text;
            EXIT WHEN counter > category_limit;
            CONTINUE WHEN NOT EXISTS (
                SELECT 1
                FROM   information_schema.tables
                WHERE  table_schema = 'public'
                AND    table_name = my_table_name
            );
            EXECUTE format('select count(*) from (select  distinct on (domain) domain from %s where  processed = TRUE and domain is not NULL) as _inner' , 'public.' || my_table_name) INTO processed;
            EXECUTE format('select count(*) from (select  distinct on (domain) domain from %s where  processed = FALSE and domain is not NULL) as _inner', 'public.' || my_table_name) INTO not_processed;
            INSERT INTO temp_result VALUES (my_table_name, processed, not_processed, counter);
        END LOOP;
        return query select tm.data_source, cat.description, em.emails_number, tm.proccesed, tm.not_processed
        	from temp_result as tm
            inner join  public.categories as cat on tm.category_id = cat.category_id
            inner join (SELECT category_id, count(*) as emails_number
                        FROM public.emails
                        group by category_id) as em on tm.category_id = em.category_id;
 END;

$BODY$;


CREATE OR REPLACE FUNCTION public.create_csv_file_by_category(category_limit integer
	)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE

AS $BODY$

DECLARE counter integer := 0;
		file_name VARCHAR(100) := '';
BEGIN
		counter:= 0;
        LOOP
            counter := counter + 1;
            EXIT WHEN counter > category_limit;
            CONTINUE WHEN NOT EXISTS (
                SELECT 1
                FROM   public.emails
                WHERE  category_id = counter
            );
           file_name := '/tmp/emails_' || counter::text || '.csv';
           EXECUTE format('Copy (select id, email, domain, type, confidence, sources, first_name, last_name, position,
                 linkedin, twitter, phone_number, webmail, pattern, organization, category_id, emails_number
 			from public.emails where category_id = %s) To ''%s'' With CSV HEADER DELIMITER '',''', counter, file_name);
        END LOOP;
        RETURN counter;
 END;

$BODY$;


select * from (
SELECT email, category_id FROM public.emails
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY email, category_id ORDER BY id asc) AS rnum
                     FROM public.emails) t
              WHERE t.rnum > 1)

UNION

SELECT email, category_id FROM public.emails
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY email, category_id ORDER BY id desc) AS rnum
                     FROM public.emails) t
              WHERE t.rnum > 1) ) as int_query
    order by int_query.email