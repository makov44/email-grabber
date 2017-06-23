Copy (select first_name, last_name, phone_number, position, organization, email
 from public.emails where first_name = '' is false and last_name = '' is false
 and emails_number<= 60 and confidence >= 10 and category_id = 299) To '/tmp/emails_299.csv' With CSV DELIMITER ',';

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
UPDATE public.data_source_299 SET domain = substring(website from
'^(?:https?:\/\/)?(?:www\.)?(?:[-0-9A-Za-z_]{1,}\.)*([-0-9A-Za-z_]{1,}\.com|[-0-9A-Za-z_]{1,}\.net|[-0-9A-Za-z_]{1,}\.org|[-0-9A-Za-z_]{1,}\.edu|[-0-9A-Za-z_]{1,}\.gov)(?:.+)?$');

select _inner.website
from (SELECT distinct on (website) id, factual_id, name, address, address_extended,
 po_box, locality, region, post_town, admin_region, post_code, country, tel, fax, latitude, longitude, neighborhood,
 substring(website from '^(?:https?:\/\/)?(?:www\.)?(?:[-0-9A-Za-z_]{1,}\.)*([-0-9A-Za-z_]{1,}\.com|[-0-9A-Za-z_]{1,}\.net|[-0-9A-Za-z_]{1,}\.org)(?:.+)?$') as website,
          email, category_ids, category_lables, chaine_name, chain_id, hours, hours_display, existence, population, processed
      FROM public.data_source_227
      where processed = FALSE
      order by website, population desc) as _inner
 where _inner.website is not null
 order by _inner.population desc
LIMIT 2500;


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