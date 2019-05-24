import logging.config
import os
import sys
sys.path.append("..")
from handlers import error_handler
from email_hunter.client import EmailHunterClient
from email_hunter.database_manager import *
import traceback

script_path = os.path.dirname(__file__)
path = os.path.join(script_path, 'logging_config.ini')
logging.config.fileConfig(path)
logger = logging.getLogger('root')
category_ids = ["181", "182", "183", "184", "185", "186", "187", "188", "189", "190", "191", "192"]
#category_ids = ["341", "340", "339", "338", "337", "336", "335", "334", "332", "331"]
#category_ids = ["241", "242", "243"]
_client = EmailHunterClient('***********')


@error_handler(logger)
def main():
    for category_id in category_ids:
        rows = get_domains(category_id)
        i = 1
        size = 1
        for chunk in gen_chunk(rows, size):
            process(chunk, category_id)
            print("Processed domains:{0}, category id: {1}".format(i * size, category_id))
            i += 1


@connector
def process(chunk, category_id, cur=None):
    for row in chunk:
        try:
            offset = 0
            get_emails(category_id, cur, offset, row)
        except:
            logger.error(traceback.format_exc())
            break


def get_emails(category_id, cur, offset, row):
    #response = _client.search(row[0], offset=offset, type_="personal",  limit=100, department='executive,finance,management,sales,hr,marketing,communication,it,legal,support')
    response = _client.search(row[0], offset=offset, type_="personal", limit=100)
    emails_number = response['meta']['results']
    try:
        update_domain(row[0], emails_number, cur)
    except:
        logger.error(traceback.format_exc())
    if emails_number > 0:
        insert_emails(response['data'], category_id, emails_number, cur)

    return
    offset += 100
    if emails_number > offset:
        get_emails(category_id, cur, offset, row)


def gen_chunk(iterable, size):
    chunk = []
    for i, line in enumerate(iterable):
        if i % size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(line)
    yield chunk


if __name__ == "__main__":
    main()
