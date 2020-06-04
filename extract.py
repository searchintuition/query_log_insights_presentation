import pandas
import pprint
import json
from jsonpath_ng import jsonpath, parse

def extract(file=None, delimiter='\t'):
    return pandas.read_csv(file, delimiter)


def parse_json(json_data, jsonpath=None):
    jsonpath_expression = parse(jsonpath)
    parsed = [match.value for match in jsonpath_expression.find(json_data)]
    return parsed
