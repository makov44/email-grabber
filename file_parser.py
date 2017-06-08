import csv
from DataAccess.connector import connector


@connector
def process_data(file_path, f, cur=None):
    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file, dialect='excel-tab')
            next(reader)  # read head
            size = 10000
            i = 1
            for chunk in gen_chunk(reader, size):
                f(chunk, cur)
                print("Processed {0} lines".format(i*size))
                i += 1

    except StopIteration:
        print("StopIteration , file name: {0}".format(file_path))


def gen_chunk(reader, size=1000):
    chunk = []
    for i, line in enumerate(reader):
        if i % size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(line)
    yield chunk
