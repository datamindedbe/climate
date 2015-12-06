from elasticsearch import Elasticsearch
from es_data_store import EsDataStore
from config import Config
from xco2 import xco2_to_dict, get_mapping
import glob


def main():
    cfg = Config()
    index = cfg.get('index')
    hosts = cfg.get('hosts')
    type = "xco2"
    es = Elasticsearch(hosts=hosts)
    data_store = EsDataStore(es)
    data_store.initialize_index(index, erase=True)
    data_store.put_mapping(index, type, get_mapping())

    for filename in glob.iglob('data/GOSAT_TANSO_Level2/ACOS_L2S.3.3/2013/*/*.h5'):
        #try:
            print "extracting %s" %filename
            extract = xco2_to_dict(filename)
            data_store.store(index, type, extract)
            print "success."
        #except:
        #    print "failed."



if __name__ == "__main__":
    main()