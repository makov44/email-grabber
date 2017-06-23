from data_access.connector import connector
from functools import reduce


@connector
def get_domains(categoty_id, cur=None):
    sql = """select _inner.domain 
from (SELECT distinct on (domain) id, factual_id, name, address, address_extended, po_box, locality, region,
 post_town, admin_region, post_code, country, tel, fax, latitude, longitude, neighborhood, website, email, 
 category_ids, category_lables, chaine_name, chain_id, hours, hours_display, existence, population, processed,
  emails_number, domain
 FROM public.data_source_{0} 
 ﻿  where processed = FALSE and domain is not NULL 
    order by domain, population desc) as _inner   
    order by _inner.population desc
    limit 5000""".format(categoty_id)
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


def update_domain(domain, category_id, emails_number,  cur):
    sql = "UPDATE public.data_source_" + str(category_id) + " SET processed=TRUE, emails_number=" + str(emails_number) + \
          " WHERE  domain = '" + domain + "'"
    cur.execute(sql)
    print('Executed script {0}'.format(sql))