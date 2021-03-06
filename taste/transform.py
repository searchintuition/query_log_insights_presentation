import pandas
import json
from slugify import slugify


def reshape(df):
    return df.groupby('user') \
        .agg({ 'query': list, 'document': list }) \
        .reset_index()


def preload(df):
    bulk_ready = []
    jsonl = df.to_json(orient='records', lines=True, index=True)
    [add(j, bulk_ready) for j in jsonl.split('\n')]

    return bulk_ready


def add(jsonstring, bulk):
    record = json.loads(jsonstring)
    leader = {
      "index": {
        "_index": "taste",
        "_id": record["user"]
      }
    }
    bulk.append(leader)
    bulk.append(record)
