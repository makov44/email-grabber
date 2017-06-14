from data_access.connector import connector
from functools import reduce


@connector
def get_domains(categoty_id, cur=None):
    sql = """select _inner.website 
from (SELECT distinct on (website) id, factual_id, name, address, address_extended, 
 po_box, locality, region, post_town, admin_region, post_code, country, tel, fax, latitude, longitude, neighborhood, 
 substring(website from '^(?:https?:\/\/)?(?:[^@\n]+@)?(?:www\.)?([^:\/\n\?]+\.com|[^:\/\n\?]+\.net|[^:\/\n\?]+\.org)(?:[^\.]+)?$') 
 as website, email, category_ids, category_lables, chaine_name, chain_id, hours, hours_display, existence, population, processed
 FROM public.data_source_{0} 
 ﻿  where processed = FALSE
    order by website, population desc) as _inner
    where _inner.website is not null
    order by _inner.population desc
    limit 2500""".format(categoty_id)
    cur.execute(sql)
    rows = cur.fetchall()
    print('Executed script: {0}'.format(sql))
    return rows


@connector
def insert_emails(data, category_id,  cur=None):
    lines = data['emails']

    lines = [(x['value'], x['type'], x["confidence"], reduce_sources(x['sources']), x['first_name'], x['last_name'],
              x['position'], x['linkedin'], x['twitter'], x['phone_number'], data['domain'],
              data['webmail'], data['pattern'], data['organization'], category_id) for x in lines]

    for i, line in enumerate(lines):
        lines[i] = tuple(['' if x is None else x for x in line])

    sql = """INSERT INTO public.emails(email, type, confidence, sources, first_name, last_name, position, 
    linkedin, twitter, phone_number, domain, webmail, pattern, organization, category_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cur.executemany(sql, lines)
    print('Executed script {0}'.format(sql))


def reduce_sources(sources):
    def reducer(value, element):
        value.append('{domain: ' + element['domain'] + ', uri: ' + element['uri'] + ', extracted_on: ' + element['extracted_on'] + '}')

        return value

    return ', '.join(reduce(reducer, sources, []))


@connector
def update_domain(domain, category_id, emails_number,  cur=None):
    sql = "UPDATE public.data_source_" + str(category_id) + " SET processed=TRUE, emails_number=" + \
          str(emails_number) + " WHERE website LIKE '%" + domain + "%'"
    cur.execute(sql)
    print('Executed script {0}'.format(sql))