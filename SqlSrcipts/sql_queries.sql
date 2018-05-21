Copy (select first_name, last_name, phone_number, position, organization, email
 from public.emails where first_name = '' is false and last_name = '' is false
 and emails_number<= 60 and confidence >= 10 and category_id = 299) To '/tmp/emails_299.csv' With CSV DELIMITER ',';



Copy (
    select first_name, last_name, phone_number, position, organization, email, ct.description
	 from public.emails as e
     left join public.categories as ct on e.category_id = ct.category_id
    where e.category_id in (219, 221, 226, 227) ORDER BY email LIMIT 90000
) To '/tmp/emails_finance_1.csv' With CSV DELIMITER ',' HEADER;

pg_restore --username "postgres" --no-password --dbname "email_grabber" --verbose "email_grabber_05_10_2018.backup"

pg_dump --file "/Users/dyl/Downloads/email_grabber_11_01_2017" --host "198.199.97.248" --port "5432" --username "postgres" --no-password --verbose --format=c --blobs "email_grabber"

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



CREATE OR REPLACE FUNCTION public.get_processed_data(
	category_limit integer)
RETURNS TABLE(data_source character varying, category text, processed bigint, not_processed bigint)
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
            EXECUTE format('SELECT count(distinct tb1.domain)
             FROM %s as  tb1
             inner join (SELECT domain_name
	                    FROM public.domains_categories  where active = True) as tb2 on tb1.domain = tb2.domain_name
             where tb1.processed is TRUE
                   and tb1.domain is not NULL' , 'public.' || my_table_name) INTO processed;
            EXECUTE format('SELECT count(distinct tb1.domain)
             FROM %s as  tb1
             inner join (SELECT domain_name
	                    FROM public.domains_categories  where active = True) as tb2 on tb1.domain = tb2.domain_name
             where tb1.processed is not TRUE
                   and tb1.domain is not NULL', 'public.' || my_table_name) INTO not_processed;
            INSERT INTO temp_result VALUES (my_table_name, processed, not_processed, counter);
        END LOOP;
        return query select tm.data_source, cat.description, tm.proccesed, tm.not_processed
        	from temp_result as tm
            inner join  public.categories as cat on tm.category_id = cat.category_id;
 END;

$BODY$;




CREATE OR REPLACE FUNCTION public.get_processed_1000_data(
	category_limit integer)
RETURNS TABLE(data_source character varying, category text, processed bigint, not_processed bigint)
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
            EXECUTE format('SELECT count(distinct tb1.domain)
             FROM %s as  tb1
             inner join (SELECT domain_name
	                    FROM public.domains_categories  where active = True) as tb2 on tb1.domain = tb2.domain_name
             where tb1.processed_1000 is TRUE
                   and tb1.domain is not NULL
                   and tb1.emails_number > 10
                   and tb1.emails_number < 1000' , 'public.' || my_table_name) INTO processed;
            EXECUTE format('SELECT count(distinct tb1.domain)
             FROM %s as  tb1
             inner join (SELECT domain_name
	                    FROM public.domains_categories  where active = True) as tb2 on tb1.domain = tb2.domain_name
             where tb1.processed_1000 is not TRUE
                   and tb1.domain is not NULL
                   and tb1.emails_number > 10
                   and tb1.emails_number < 1000', 'public.' || my_table_name) INTO not_processed;
            INSERT INTO temp_result VALUES (my_table_name, processed, not_processed, counter);
        END LOOP;
        return query select tm.data_source, cat.description, tm.proccesed, tm.not_processed
        	from temp_result as tm
            inner join  public.categories as cat on tm.category_id = cat.category_id;
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


INSERT INTO public.domains(name)
SELECT distinct substring(lower(website) from '^(?:https?:\/\/)?(?:www\.)?(?:[-0-9A-Za-z_]{1,}\.)*([-0-9A-Za-z_]{1,}\.com|[-0-9A-Za-z_]{1,}\.net|[-0-9A-Za-z_]{1,}\.org|[-0-9A-Za-z_]{1,}\.edu|[-0-9A-Za-z_]{1,}\.gov|[-0-9A-Za-z_]{1,}\.info|[-0-9A-Za-z_]{1,}\.us|[-0-9A-Za-z_]{1,}\.biz)(?:.+)?$')
from public.us_places where nullif(website, '') is not Null




CREATE OR REPLACE FUNCTION public.updated_processed_domains(
	_offset integer,
	_limit integer)
RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE
    ROWS 0
AS $BODY$

DECLARE counter integer := _offset;
		my_table_name VARCHAR(100) := '';
BEGIN
      LOOP
          counter := counter + 1;
          my_table_name := 'data_source_' || counter::text;

          EXIT WHEN counter > _limit;
          CONTINUE WHEN NOT EXISTS (
              SELECT 1
              FROM   information_schema.tables
              WHERE  table_schema = 'public'
              AND    table_name = my_table_name
          );

          EXECUTE format(
              'UPDATE %s SET processed=TRUE where domain in (SELECT domain from processed_domains where category_id = ''%s'')', 'public.' || my_table_name, counter);
          RAISE NOTICE 'processed table: (%)', 'public.' || my_table_name;
      END LOOP;
 END;

$BODY$;

ALTER FUNCTION public.updated_processed_domains(integer, integer)
    OWNER TO postgres;



CREATE OR REPLACE FUNCTION public.alter_source_tables(
	_offset integer,
	_limit integer)
RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE
AS $BODY$

DECLARE counter integer := _offset;
		my_table_name VARCHAR(100) := '';
BEGIN
      LOOP
          counter := counter + 1;
          my_table_name := 'data_source_' || counter::text;

          EXIT WHEN counter > _limit;
          CONTINUE WHEN NOT EXISTS (
              SELECT 1
              FROM   information_schema.tables
              WHERE  table_schema = 'public'
              AND    table_name = my_table_name
          );

          EXECUTE format(
              'ALTER TABLE %s ADD COLUMN processed_1000 boolean;', 'public.' || my_table_name);
          RAISE NOTICE 'altered table: (%)', 'public.' || my_table_name;
      END LOOP;
 END;

$BODY$;


UPDATE data_source_229
SET processed_1000 = true
where processed is  true and emails_number<=10 and processed_1000 is not true



Copy (
    SELECT distinct e.email, e.first_name, e.last_name, "position",  phone_number, organization, s.website, linkedin, twitter
	FROM public.emails as e
    inner join domains_categories as d on e.domain=domain_name
    inner join public.data_source_221 as s on s.domain = e.domain
    where d.category_id = 221
    and (e.first_name <> '')
    and (e.last_name <> '')
    and (e.position <> '')
    and (e.phone_number <> '')
    and e.organization <> ''
    and s.region = 'CA'
    and d.active = True
    and e.emails_number < 200
    limit 100
) To '/tmp/emails_221_banking.csv' With CSV DELIMITER ',' HEADER;



SELECT dom_cat.category_id, count(*) as emails_number
      FROM public.emails as email
      inner join domains_categories as dom_cat on email.domain=dom_cat.domain_name
      group by dom_cat.category_id;


SELECT domain_name, count(category_id)
	FROM public.domains_categories
    group by domain_name
    having count(category_id) > 10
    order by count(category_id);

Copy (
    SELECT distinct name, address, address_extended, po_box, locality, region, post_town, admin_region, post_code, country, tel, fax, latitude, longitude, neighborhood, website, email, category_ids, category_lables, chaine_name,  population
	FROM  public.data_source_227 as s
    left join domains_categories as d on s.domain = d.domain_name
    where  d.active = True and s.region = 'CA'
) To '/tmp/info_227_loans_ca_filtered.csv' With CSV DELIMITER ',' HEADER;


﻿
SELECT count(DISTINCT tb1.email)
	FROM public.mail_mass_mailing_contact as tb1
INNER JOIN (
   SELECT tb2._domain
   FROM   public.dblink('dbname=email_grabber','SELECT DISTINCT domain_name FROM public.domains_categories  where active = False')
   AS     tb2(_domain text)
) AS tb2 ON tb2._domain = substring(tb1.email from '@(.*)$')
where tb1.opt_out is not True

