import json
import csv
import logging
import os

from elasticsearch import Elasticsearch


logging.basicConfig(level=logging.INFO)


class Config:
    def __init__(self, filename='config.json'):
        with open(filename, 'r') as infile:
            self.config = json.load(infile)

    def get(self, key):
        return self.config[key]