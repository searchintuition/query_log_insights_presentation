import pandas

def extract(file=None, delimiter='\t'):
    return pandas.read_csv(file, delimiter)
