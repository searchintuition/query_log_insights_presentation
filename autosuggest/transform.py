import pandas
import json
from slugify import slugify


def reshape(df):
    return df.groupby('query') \
      .agg(count=('user', 'nunique'), timestamp=('timestamp', 'max')) \
      .sort_values(by='count', ascending=False) \
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
        "_index": "autosuggest",
        "_id": slugify(record["query"])
      }
    }
    bulk.append(leader)
    bulk.append(record)
