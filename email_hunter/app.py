import logging.config
from logging_config import LOGGING
from handlers import error_handler
from email_hunter.client import EmailHunterClient
from email_hunter.database_manager import get_domains, insert_emails
from time import sleep
import traceback

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('main')
category_ids = ["4", "26", "62", "193", "219", "221", "227", "235", "269", "272"]
_client = EmailHunterClient('8ed878188f5d409dd037bbbe08499c2e1b156e55')


@error_handler(logger)
def main():
    for id in category_ids:
        rows = get_domains(id)
        try:
            response = _client.search(rows[12])

            if response['meta']['results'] > 0:
                insert_emails(response['data'])
        except:
            logger.error(traceback.format_exc())

        sleep(1)

if __name__ == "__main__":
    main()