import gzip
import unicodecsv as ucsv
from settings import DATA_LOCATION, FIELDNAMES



def extract_data(data_path, fieldnames):
    data = []
    with gzip.open(data_path, 'rb') as r_file:
        # Need to be imported from settings.py as a variable. Probably list?
        rowlist = ucsv.DictReader(r_file, fieldnames=fieldnames)
        for row in rowlist:
            data.append(row)
    return data

data_raw = extract_data(DATA_LOCATION, FIELDNAMES)
DATA = data_raw[1:]