import requests
import geojson
import json
from elasticsearch import helpers,Elasticsearch                                                         # imports

TYPE = 'multilinestring'

data_url = 'https://opendata.arcgis.com/datasets/70512b03fe994c6393107cc9946e5c22_0.geojson'    # sets up variables for future use
mappingFile = 'power_lines_mapping'
index_name = 'powerlines'


def prepare_config_indexname_url():                                                             # Allows user to specify variables
    global data_url, mappingFile, index_name

    possible_url = input(
        'If you would like to load data from a custom URL, please specify. Otherwise, default URL will be used:')
    data_url = possible_url if possible_url else data_url  

    possible_mapping = input(
        'If you would like to use a custom index mapping, please specify the text/json file to pull the mapping from. Otherwise, default URL will be used:')
    mappingFile = possible_mapping if possible_mapping else mappingFile

    possible_name = input('Please type an all lowercase name for the power line data index. Defaults to "powerline":')
    index_name = possible_name if possible_name else index_name

      
def process_lines(feature_list):                                                                # processes dictionary of power lines
    processed = []
    for feature in feature_list:
        geometry = feature['geometry']
        info = feature['properties']
        line_object = {'shape': {'type': TYPE, 'coordinates': geometry['coordinates']}, 'type': TYPE}       # adds geoshape data to the line object
        for line_property in info:
            line_object[line_property] = info[line_property]                                    # adds all non-geoshape data to the object as well
        processed.append(line_object)                                                           # adds new object to list of processed objects
    return processed


def prepare_elasticsearch():
    mapping = json.loads(open(mappingFile).read())                                              # pulls mapping from a file
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}],
                       http_auth=('elastic', 'changeme'))                                       # initializes elasticsearch with authentication
    if not es.indices.exists(index=index_name):                                                 # creates the index if it doesn't exist yet
        es.indices.create(index=index_name, body=mapping)
    return es


def get_data():
   return geojson.loads(requests.get(data_url, allow_redirects=True).content)["features"]       # gets data from url, then converts the geojson file into a dictionary 
                                                                                                # of all the powerlines, then returns said dictionary

def index_data(line_data_list, es):
   bulk_lines_list = ({
        '_index': index_name,
        '_ID': line['ID'],                                                                      # id is the ID of the power line, unique to each power line
        '_source': line} for line in line_data_list
    )

   helpers.bulk(es, bulk_lines_list)                                                            # bulk indexes data                        

prepare_config_indexname_url()
elastic = prepare_elasticsearch()                                                               # runs everything
loaded_data = get_data()
print('retrieved and loaded')
processed_data = process_lines(loaded_data)
index_data(processed_data, elastic)
print('indexed')




