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


def query_index(index=None, body=None, field=None):
    res = es.search(index=index, body=body)
    for hit in res['hits']['hits']:
        print(hit["_source"][field])


def query_template(index=None, template_file=None, template=None, params=None, field=None):
    if template_file is not None:
        with open(template_file, 'r') as f:
            template = f.read()

    body = { "source": template, "params": params }
    res = es.search_template(index=index, body=body)
    for hit in res['hits']['hits']:
        print(hit["_source"][field])


def index_mapping(index=None):
    res = es.indices.get_mapping(index)
    print("Mapping for ", index)
    print(json.dumps(res, indent=2))


def peek(filename):
    with open(filename, 'r') as f:
        print(f.read())
