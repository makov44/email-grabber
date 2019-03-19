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

