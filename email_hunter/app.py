import logging.config
from logging_config import LOGGING
from handlers import error_handler
from email_hunter.client import EmailHunterClient
from email_hunter.database_manager import *
from time import sleep
import traceback

_i = 0

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('main')
category_ids = ["4", "26", "62", "193", "219", "221", "227", "235", "269", "272"]
_client = EmailHunterClient('4e3c330e5d7e6f1da8f7191b2be57e2fa8724208')


@error_handler(logger)
def main():
    for _id in category_ids:
        rows = get_domains(_id)
        for chunk in gen_chunk(rows, 20):
            process(chunk, _id)
            sleep(0.5)


def process(chunk, _id):
    for row in chunk:
        try:
            response = _client.search(row[0])
            emails_number = response['meta']['results']
            update_domain(row[0], _id, emails_number)
            if emails_number > 0:
                insert_emails(response['data'], _id)
        except:
            logger.error(traceback.format_exc())


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