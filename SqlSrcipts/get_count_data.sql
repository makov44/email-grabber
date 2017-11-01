CREATE OR REPLACE FUNCTION public.get_count_data(category_limit integer
	)
    RETURNS  TABLE (
    data_source   VARCHAR(100)
  , total bigint
  , domain_is_null bigint
  )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$

DECLARE counter integer := 0;
		my_table_name VARCHAR(100) := '';
BEGIN
    DROP TABLE IF EXISTS temp_result;
 		CREATE TEMP TABLE temp_result
        (
             data_source VARCHAR(100),
             total bigint,
             domain_is_null bigint
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
            EXECUTE format('select count(*) from  %s' , 'public.' || my_table_name) INTO total;
            EXECUTE format('select count(*) from %s where domain is NULL', 'public.' || my_table_name) INTO domain_is_null;
            INSERT INTO temp_result VALUES (my_table_name, total, domain_is_null);
        END LOOP;
        return query select * from temp_result;
 END;

$BODY$;