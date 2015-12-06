from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch


class EsDataStore():
    def __init__(self, es):
        self.es = es
        pass

    def index(self, index, doc_type, body):
        self.es.index(index=index, doc_type=doc_type, refresh=True, body=body)

    def store(self, index, doc_type, docs):
        bulk_data = []
        for doc in docs:
            bulk_data.append(doc)

        bulk(self.es, bulk_data, index=index, doc_type=doc_type, refresh=True)

    def load(self, index, doc_type, key, value):
        query = {
            "size": 1000000,
            "query": {
                "filtered": {
                    "query": {
                        "match": {key: value}
                    }
                }
            }
        }
        results = self.es.search(index=index, body=query, doc_type=doc_type)
        return results['hits']['hits']

    def has_ids(self, index, doc_type, ids):
        query = {
            "size": 1,
            "query": {
                "ids": {
                    "type": doc_type,
                    "values": ids
                }
            }
        }
        results = self.es.search(index=index, body=query)
        return len(results['hits']['hits']) > 0

    def put_mapping(self, index, doc_type, mapping):
        self.es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)

    def initialize_index(self, index, erase):
        exists = self.es.indices.exists(index=index)
        if exists and erase is True:
            self.es.indices.delete(index=index, ignore=[404])
            exists = False

        if not exists:
            self.es.indices.create(index=index)