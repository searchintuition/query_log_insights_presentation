from elasticsearch import Elasticsearch
import json

es = Elasticsearch(
    'localhost',
    # sniff before doing anything
    sniff_on_start=True,
    # refresh nodes after a node fails to respond
    sniff_on_connection_fail=True,
    # and also every 60 seconds
    sniffer_timeout=60
)


def create_index(index=None, body=None, mapping_file=None):
    if es.indices.exists(index):
        es.indices.delete(index)
    
    if (mapping_file is not None):
        with open(mapping_file, 'r') as f:
            body = f.read()
    
    es.indices.create(index=index, body=body)


def populate_index(body=None):
    response = es.bulk(body=body)
    return response


def get(id, index=None):
    return es.get(index=index, id=id)


def query_index(index=None, body=None, field=None):
    res = es.search(index=index, body=body)
    for hit in res['hits']['hits']:
        print(hit["_source"][field])


def query_template(index=None, template_file=None, template=None, params=None):
    if template_file is not None:
        with open(template_file, 'r') as f:
            template = f.read()

    escaped_template = template.replace('"', '\"').replace('\n','')
    body = { "source": escaped_template, "params": params }
    res = es.search_template(index=index, body=body)
    return res


def parse_json(json_data, jsonpath=None):
    jsonpath_expression = parse('employees[*].id')
    [print(v) for v in jsonpath_expression.find(json.loads(json_data))]


def index_mapping(index=None):
    res = es.indices.get_mapping(index)
    print("Mapping for ", index)
    print(json.dumps(res, indent=2))


def peek(filename):
    with open(filename, 'r') as f:
        contents = f.read()

    print(contents)
    return
