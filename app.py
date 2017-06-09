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
def main():
    args = sys.argv[1:]
    if args is None or len(args) < 3:
        raise Exception('Failed to run script. Please provide file names as arguments')

    us_crosswalk = args[0]
    us_places = args[1]
    zipinfo = args[2]

    clean_data(['us_places', 'us_crosswalk', 'zipcode', 'categories', 'places_category'], cur=None)

    process_data(zipinfo, insert_zipcodes)

    process_data(us_crosswalk, insert_crosswalk)

    process_data(us_places, process_places)

    insert_categories(cur=None)


def process_places(lines, cur):
    curr_sequence = insert_places(lines, cur)
    places_categories = create_categories(lines, curr_sequence)
    insert_places_category(places_categories, cur)


if __name__ == "__main__":
    main()