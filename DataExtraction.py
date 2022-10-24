import gzip
import unicodecsv as ucsv
from settings import DATA_LOCATION, FIELDNAMES, HEADERS, DELIMITER


@staticmethod
def extract_data(data_path, fieldnames):
    """
    Reads and stores data into a list.
    """
    data = []
    try:
        with gzip.open(data_path, 'rb') as r_file:
            # Need to be imported from settings.py as a variable. Probably list?
            rowlist = ucsv.DictReader(r_file, fieldnames=fieldnames)
            for row in rowlist:
                data.append(row)
    except:
        with open(data_path, 'rb') as r_file:
            # Need to be imported from settings.py as a variable. Probably list?
            rowlist = ucsv.DictReader(r_file, delimiter=DELIMITER, fieldnames=fieldnames)
            for row in rowlist:
                data.append(row)
    yield data

# data_path = "D:\\BigDataCourse\\Data-practicals\\ml-100k\\u.data"
data_raw = extract_data(DATA_LOCATION, FIELDNAMES)
DATA = data_raw[1:] if HEADERS else data_raw
# print(len(DATA))
# if len(DATA) > 1000000:
#     DATA = DATA[:1000000]
