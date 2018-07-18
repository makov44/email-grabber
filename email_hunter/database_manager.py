from data_access.connector import connector
from functools import reduce
import logging
import traceback

logger = logging.getLogger('database_manager')


@connector
def get_domains(categoty_id, cur=None):
    sql = """SELECT distinct tb1.domain
             FROM public.data_source_{0} as  tb1
             inner join (SELECT domain_name
	                    FROM public.domains_categories  
	                    where active = True) as tb2 on tb1.domain = tb2.domain_name
             where tb1.processed is not TRUE  and tb1.domain is not NULL                
                """.format(categoty_id)
    cur.execute(sql)
    rows = cur.fetchall()
    print('Executed script: {0}'.format(sql))
    return rows


def insert_emails(data, category_id, emails_number, cur):
    lines = data['emails']

    lines = [(x['value'], x['type'], x["confidence"], reduce_sources(x['sources']), x['first_name'], x['last_name'],
              x['position'], x['linkedin'], x['twitter'], x['phone_number'], data['domain'],
              data['webmail'], data['pattern'], data['organization'], category_id, emails_number) for x in lines]

    for i, line in enumerate(lines):
        lines[i] = tuple(['' if x is None else x for x in line])

    sql = """INSERT INTO public.emails(email, type, confidence, sources, first_name, last_name, position, 
    linkedin, twitter, phone_number, domain, webmail, pattern, organization, category_id, emails_number) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cur.executemany(sql, lines)
    print('Executed script {0}'.format(sql))


def reduce_sources(sources):
    def reducer(value, element):
        value.append('{domain: ' + element['domain'] + ', uri: ' + element['uri'] + ', extracted_on: ' + element['extracted_on'] + '}')

        return value

    return ', '.join(reduce(reducer, sources, []))


def update_domain(domain, emails_number,  cur):
    sql = "select category_id from public.domains_categories where domain_name = '" + domain + "'"
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        sql = "UPDATE public.data_source_" + str(row[0]) + " SET processed=TRUE, emails_number=" + str(emails_number) + \
              " WHERE  domain = '" + domain + "'"
        try:
            cur.execute(sql)
            print('Executed script {0}'.format(sql))
        except:
            logger.error(traceback.format_exc())
