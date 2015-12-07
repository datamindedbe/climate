from elasticsearch import Elasticsearch
from es_data_store import EsDataStore
from config import Config
from xco2 import xco2_to_dict, get_mapping
import glob
import sys


def main():
    cfg = Config()
    index = cfg.get('index')
    hosts = cfg.get('hosts')
    type = "xco2"
    es = Elasticsearch(hosts=hosts)
    data_store = EsDataStore(es)
    data_store.initialize_index(index, erase=True)
    data_store.put_mapping(index, type, get_mapping())

    folder = 'data/GOSAT_TANSO_Level2/ACOS_L2S.3.3'
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    pattern = '%s/*/*/*.h5' %folder
    print 'searching %s' %pattern
    for filename in glob.iglob(pattern):
        try:
            print "extracting %s" %filename
            extract = xco2_to_dict(filename)
            data_store.store(index, type, extract)
            print "success."
        except:
            print "failed."



if __name__ == "__main__":
    main()