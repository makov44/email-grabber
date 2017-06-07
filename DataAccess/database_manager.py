from DataAccess.connector import connector


def insert_crosswalk(lines, conn):
    lines = [tuple(x) for x in lines if(len(x) == 5)]
    sql = """INSERT INTO public.us_crosswalk(factual_id, facebook_id, facebook_url, twitter_id, twitter_url) 
    VALUES(%s,%s,%s,%s,%s)"""
    cur = conn.cursor()
    cur.executemany(sql, lines)
    conn.commit()
    cur.close()
    print('Executed script: {0}'.format(sql))


def insert_places(lines, conn):
    lines = [tuple(x) for x in lines if(len(x) == 25)]
    sql = """INSERT INTO public.us_places(factual_id, name, address, address_extended, po_box, 
    locality, region, post_town, admin_region, post_code, country, tel, fax, latitude, longitude,
    neighborhood, website, email, category_ids, category_lables, chaine_name, chain_id, hours, 
    hours_display, existence) 
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cur = conn.cursor()
    cur.executemany(sql, lines)
    conn.commit()
    result = get_currseq('us_places_id_seq', conn)
    cur.close()
    print('Executed script: {0}'.format(sql))
    return result


def insert_zipcodes(lines, conn):
    lines = [(x[-1], x[-12]) for x in lines]
    sql = """INSERT INTO public.zipcode(zipcode, population) 
    VALUES(%s,%s)"""
    cur = conn.cursor()
    cur.executemany(sql, lines)
    conn.commit()
    cur.close()
    print('Executed script: {0}'.format(sql))


def insert_categories(lines, conn):
    sql = """INSERT INTO public.categories(category_id, description) 
    VALUES(%s,%s)"""
    cur = conn.cursor()
    cur.executemany(sql, lines)
    conn.commit()
    cur.close()
    print('Executed script: {0}'.format(sql))


def insert_places_category(lines, conn):
    sql = """INSERT INTO public.places_category(place_id, category_id) 
    VALUES(%s,%s)"""
    cur = conn.cursor()
    cur.executemany(sql, lines)
    conn.commit()
    cur.close()
    print('Executed script: {0}'.format(sql))


def get_currseq(class_name, conn):
    sql = "select currval('{0}'::regclass)".format(class_name)
    cur = conn.cursor()
    cur.execute(sql)
    curr_sequence = cur.fetchone()
    conn.commit()
    print('Executed script: {0}'.format(sql))
    return curr_sequence[0]


def clean_data(table_names, conn):
    for name in table_names:
        sql = 'TRUNCATE public.' + name + ' RESTART IDENTITY'
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print('Executed script: {0}'.format(sql))
