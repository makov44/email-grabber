SELECT tb2.recipient, tb1.email
FROM public.emails as tb1
inner JOIN (
   SELECT *
   FROM   public.dblink('dbname=postfix','SELECT distinct recipient
	FROM public.sisimai_output
	where softbounce= 0 ')
   AS  tb2( recipient text)
) AS tb2 ON tb2.recipient = tb1.email;

delete
FROM public.emails as tb1
where tb1.email in (SELECT recipient
   FROM   public.dblink('dbname=postfix','SELECT distinct recipient
	FROM public.sisimai_output
	where softbounce= 0 ')  AS  tb2(recipient text))

select *
FROM public.emails as tb1
where tb1.email in (SELECT recipient
   FROM   public.dblink('dbname=postfix','SELECT distinct recipient
	FROM public.sisimai_output
	where softbounce= 0 ')  AS  tb2(recipient text))

-- Create new extension dblink to connect to local or remote database
CREATE EXTENSION dblink

-- verify extension
SELECT pg_namespace.nspname, pg_proc.proname
FROM pg_proc, pg_namespace
WHERE pg_proc.pronamespace=pg_namespace.oid
   AND pg_proc.proname LIKE '%dblink%';