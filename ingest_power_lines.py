import requests
import geojson
import sys
from elasticsearch import Elasticsearch                                                         # imports


data_url = 'https://opendata.arcgis.com/datasets/70512b03fe994c6393107cc9946e5c22_0.geojson'    # sets up variables for future use
if len(sys.argv) > 1:
   data_url = sys.argv[1]                                                                       # option for user to choose their own data url
index_name = 'powerlines'
type = 'multilinestring'


def process_features(feature_list):                                                             # processes dictionary of power lines
   processed = []
   idx = 0
   for feature in feature_list:
      geometry = feature['geometry']
      info = feature['properties']
      object = {}
      object['shape'] = {'type':type, 'coordinates':geometry['coordinates']}                    # prepares the line into a multiline format acceptable by elasticsearch
      object['type'] = type
      for property in info:
         object[property] = info[property]                                                      # adds all non-geoshape data to the object as well
      processed.append(object)                                                                  # adds new object to list of processed objects
   return processed


def prepare_elasticsearch():
   mapping = {                                                                                  # sets up types for all the information in the power line document
      'mappings': {
         "properties": {

            'location':{'type':'geo_shape'},
            'OBJECTID':{'type':'integer'},
            'ID':{'type':'integer'},
            'TYPE':{'type':'text'},
            'STATUS':{'type':'text'},
            'NAICS_CODE':{'type':'integer'},
            'NAICS_DESC':{'type':'text'},
            'SOURCE':{'type':'text'},
            'SOURCEDATE':{'type':'date',"format": "yyyy/MM/dd HH:mm:ss"},
            'VAL_METHOD':{'type':'text'},
            'VAL_DATE':{'type':'date',"format": "yyyy/MM/dd HH:mm:ss"},
            'OWNER':{'type':'text'},
            'VOLTAGE':{'type':'float'},
            'VOLT_CLASS':{'type':'text'},
            'INFERRED':{'type':'text'},
            'SUB_1':{'type':'text'},
            'SUB_2':{'type':'text'},
            'SHAPE_Length':{'type':'float'}

               }
      }
   }
   es = Elasticsearch([{'host': 'localhost', 'port': 9200}],http_auth=('elastic','changeme'))   # initializes elasticsearch with authentication
   if not es.indices.exists(index=index_name):                                                  # creates the index if it doesn't exist yet
     es.indices.create(index=index_name,body=mapping)
   return es


def get_data():
   return geojson.loads(requests.get(data_url, allow_redirects=True).content)["features"]       # gets data from url, then converts the geojson file into a dictionary 
                                                                                                # of all the powerlines, then returns said dictionary

def index_data(data_list, es):
   for i in range(len(data_list)):                                                              #add data to index
      es.index(index=index_name, id=data_list[i]['ID'], body=data_list[i])                      # id is the ID of the power line, unique to each power line


elastic = prepare_elasticsearch()                                                               # runs everything
loaded_data = get_data()
print('retrieved and loaded')
processed_data = process_features(loaded_data)
index_data(processed_data, elastic)
print('indexed')




