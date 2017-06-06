import sys
import logging.config
from logging_config import LOGGING
from exception import error_handler
from file_parser import *
from helper import *
from DataAccess.database_manager import *

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('main')


@error_handler(logger)
@connector
def main(conn=None):
    args = sys.argv[1:]
    if args is None or len(args) < 3:
        raise Exception('Failed to run script. Please provide file names as arguments')

    us_crosswalk = args[0]
    us_places = args[1]
    zipinfo = args[2]

    clean_data(['us_places', 'us_crosswalk', 'zipcode', 'categories', 'places_category'], conn)

    #process_data(us_crosswalk, insert_crosswalk, conn)

    process_data(us_places, process_places, conn)

    process_data(zipinfo, insert_zipcodes, conn)

    insert_categories(categories)


def process_places(lines, conn):
    curr_sequence = insert_places(lines, conn)
    places_categories = create_categories(lines, curr_sequence)
    insert_places_category(places_categories, conn)


if __name__ == "__main__":
    main()