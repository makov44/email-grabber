WITH insurance_emails  AS (select * from public.emails where category_id = 272  and
                           (domain like '%health%' or organization like '%health%' or sources like '%healthcare%')),
 first_ids AS (select t.id
  from (SELECT id,  ROW_NUMBER() OVER (partition BY domain ORDER BY id) AS rnum
                     FROM insurance_emails ORDER By CASE
    											WHEN (first_name = '') is false or (last_name = '') is false or (organization = '') is false
    											THEN 1 ELSE 0
												END DESC) t
            where t.rnum=1),
 my_mails as (
  select * from public.emails
  where id in (select t1.id from first_ids as t1)
     )

 select email, type, first_name, last_name, position, phone_number, organization
 from my_mails