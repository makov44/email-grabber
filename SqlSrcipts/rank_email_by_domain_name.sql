CREATE OR REPLACE FUNCTION public.rang_emails()
RETURNS double precision
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE
AS $BODY$

DECLARE time_ double precision := 0;
        ranking RECORD;
        rec RECORD;
BEGIN

		FOR ranking IN select tb2.domain_, tb2.time_
        FROM (select tb.domain_, (select count(*)FROM public.mail_mass_mailing_contact)::double precision/tb.count_ as time_
             FROM  (SELECT lower(substring(email from '(?<=@)[^.]+(?=\.)')) as domain_, (count(*)) as count_
                    FROM public.mail_mass_mailing_contact
                    group by domain_
                    order by count(*) desc) as tb) AS tb2
		LOOP
            time_:= ranking.time_;
			FOR rec IN select id
                          from public.mail_mass_mailing_contact
                          where lower(substring(email from '(?<=@)[^.]+(?=\.)')) = ranking.domain_
			LOOP
				Update public.mail_mass_mailing_contact SET x_rank = time_ where id = rec.id;
                time_:=time_+ranking.time_;
			END LOOP;
		END LOOP;
        return time_;
 END;

$BODY$;