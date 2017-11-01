CREATE OR REPLACE FUNCTION public.create_data_source_tables(_offset integer, _limit integer)
    RETURNS  VOID
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$

DECLARE counter integer := _offset;
		my_table_name VARCHAR(100) := '';
    reg_exp VARCHAR(250):= '^(?:https?:\/\/)?(?:www\.)?(?:[-0-9A-Za-z_]{1,}\.)*([-0-9A-Za-z_]{1,}\.com|[-0-9A-Za-z_]{1,}\.net|[-0-9A-Za-z_]{1,}\.org|[-0-9A-Za-z_]{1,}\.edu|[-0-9A-Za-z_]{1,}\.gov|[-0-9A-Za-z_]{1,}\.info|[-0-9A-Za-z_]{1,}\.us|[-0-9A-Za-z_]{1,}\.biz)(?:.+)?$';
BEGIN
      LOOP
          counter := counter + 1;
          my_table_name := 'data_source_' || counter::text;

          EXIT WHEN counter > _limit;
          CONTINUE WHEN EXISTS (
              SELECT 1
              FROM   information_schema.tables
              WHERE  table_schema = 'public'
              AND    table_name = my_table_name
          );

          EXECUTE format(
             'CREATE TABLE %s as
              SELECT pl.id, pl.factual_id, pl.name, pl.address, pl.address_extended, pl.po_box, pl.locality, pl.region, pl.post_town, pl.admin_region, pl.post_code,
              pl.country, pl.tel, pl.fax, pl.latitude, pl.longitude, pl.neighborhood, pl.website, pl.email, pl.category_ids, pl.category_lables, pl.chaine_name, pl.chain_id,
              pl.hours, pl.hours_display, pl.existence, zip.population
              FROM public.us_places as pl
                  INNER JOIN public.zipcode as zip on nullif(pl.post_code, '''')::int = zip.zipcode
                  INNER JOIN public.places_category as pc on pl.id = pc.place_id
                  WHERE (pl.website = '''') is false and pc.category_id = %s
                  ORDER BY zip.population desc', 'public.' || my_table_name, counter);
          RAISE NOTICE 'Created table: (%)', 'public.' || my_table_name;
          EXECUTE format('ALTER TABLE %s ADD COLUMN processed BOOLEAN DEFAULT(FALSE)', 'public.' || my_table_name);
          EXECUTE format('ALTER TABLE %s ADD COLUMN emails_number int', 'public.' || my_table_name);
          EXECUTE format('ALTER TABLE %s ADD COLUMN domain varchar(150)', 'public.' || my_table_name);
          EXECUTE format('UPDATE %s SET domain = substring(website from ''%s'')', 'public.' || my_table_name, reg_exp);
      END LOOP;
 END;

$BODY$;