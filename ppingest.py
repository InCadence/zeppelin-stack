from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import io
import sys
import csv
import os
import pandas as pd
from geojson.geometry import Point
from elasticsearch import helpers, Elasticsearch
from elasticsearch.connection import create_ssl_context           #imports


def SaveElasticData(toIndex, df, chunksize=500):
   """
   Utility function to save a dataframe to an Elasticsearch index.  The function will
   attempt to create the index if it does not exist.  An index template should already
   exist that maps the index name to the appropriate mapping.

   Parameters:
       toIndex (String): The name of the Elasticsearch index to which to write the dataframe.
       df (pd.dataframe): Pandas dataframe to save to Elasticsearch.
   """
   from elasticsearch import Elasticsearch
   from elasticsearch import helpers
   import time
   # TODO:  The function should return a status
   # TODO:  Add a parameter to specify the mapping to use in creating the index.
   start_time = time.time()
   #es = Elasticsearch(['http://elastic:changeme@elk.afpefp.incadencecorp.com:9200/'],
   #                   requestTimeout='Infinity')
   # es = Elasticsearch([{'host':'elk.afpefp.incadencecorp.com', 'user': 'elastic',
   #        'pwd': 'changeme','requestTimeout': 'Infinity'}])
   es = Elasticsearch([{'host': 'localhost', 'port': 9200}],
                 http_auth=('elastic', 'changeme'))  # initializes elasticsearch with authentication
   # Note this assumes an index template with the mappings has been created
   if es.indices.exists(toIndex):
      print("Index %s Exists" % (toIndex))
   else:
      print("Creating Index %s" % (toIndex))
      res = es.indices.create(index=toIndex, ignore=[400, 404], request_timeout=30)

   # Internal helper function used the the Elasticsearch bulk save
   def merged_doc_generator(df):
      df_iter = df.iterrows()
      for index, document in df_iter:
         yield {
            "_index": toIndex,
            "_type": "_doc",
            "_source": document.dropna().to_dict(),
         }

   helpers.bulk(es, merged_doc_generator(df), chunk_size=chunksize)
   # print the elapsed time
   print("TOTAL TIME:", time.time() - start_time, "seconds.")

path = os.path.realpath(__file__)
path = path[:len(path)-path[::-1].index('\\')]     #saves the data to the same file as this script

zipurl = 'http://datasets.wri.org/dataset/540dcf46-f287-47ac-985d-269b04bea4c6/resource/c240ed2e-1190-4d7e-b1da-c66b72e08858/download/globalpowerplantdatabasev120'
if len(sys.argv) > 1:
   zipurl = sys.argv[1]
   
with urlopen(zipurl) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(path+'powerplantdata/powerPlants')                      #extracts and downloads data from above url

allStations = []                 #dictionary of stations, by name
integer = -1                     
headings = []                    #headings of columns
numColumns = 0

def makePoint(lat, long):     #creates a point in longitude-latitude array format, given the coordinates
   point = [long,lat]
   return point

file = 'powerplantdata/powerPlants/global_power_plant_database.csv'          #path to data file
with open(file,'r', encoding='utf-8') as newFile:               
   newCSV = csv.reader(newFile,delimiter=',')                           #read the CSV
   
   for row in newCSV:       
                                            
      if integer == -1:        #first row, which contains headers
         headings = row[0:5] + row[7:]
         numColumns = len(headings)                      #for future use
         integer+=1
      
      else:
         point = makePoint(float(row[5]),float(row[6]))              #makes point
         converted = [row[0],row[1],row[2],row[3],row[4],*row[7:]]   #cuts lat and long out of data
         elasticReady = {headings[column]:converted[column] for column in range(numColumns)}    #instantiates a dictionary ready to add to elastic
         if elasticReady['commissioning_year'] == '':          #blank years are unacceptable
            elasticReady['commissioning_year'] = '0000'        #set them as year 0000
         elif '.' in elasticReady['commissioning_year']:       #some years have a decimal point, which elasticsarch assumes to be a separator between year and month fields
            elasticReady['commissioning_year'] = elasticReady['commissioning_year'].split('.')[0]     #gets rid of . to 
         elasticReady['location'] = point                            #adds the geo_point data
         allStations.append(elasticReady)                            #adds the power plant's data to a list of all power plants

name = 'powerplants'

mapping = {                               #sets up data types
   'mappings': {
      "properties": {
      'country':{'type':'text'},
      'country_long':{'type':'text'},	
      'name':{'type':'text'},
      'gppd_idnr':{'type':'text'},
      'capacity_mw':{'type':'float'},
      'primary_fuel':{'type':'text'},
      'other_fuel1':{'type':'text'},
      'other_fuel2':{'type':'text'},
      'other_fuel3':{'type':'text'},
      'commissioning_year':{'type':'date', 'format':'year'},
      'owner':{'type':'text'},
      'source':{'type':'text'},
      'url':{'type':'text'},
      'geolocation_source':{'type':'text'},
      'wepp_id':{'type':'text'},	
      'year_of_capacity_data':{'type':'integer'},
      'generation_gwh_2013':{'type':'float'},
      'generation_gwh_2014':{'type':'float'},
      'generation_gwh_2015':{'type':'float'},
      'generation_gwh_2016':{'type':'float'},
      'generation_gwh_2017':{'type':'float'},
      'estimated_generation_gwh':{'type':'float'},
      'location':{'type':'geo_point'}
      }
   }
} 
 
es = Elasticsearch([{'host': 'localhost', 'port': 9200}],http_auth=('elastic','changeme'))     #initializes elasticsearch with authentication


if not es.indices.exists(index=name):                #creates the index if it doesn't exist yet
  es.indices.create(index=name,body=mapping)
 
SaveElasticData(name,pd.DataFrame(allStations))

#for i in range(len(allStations)):                  #add data to index
#   es.index(index=name, id=i, body=allStations[i])
   
