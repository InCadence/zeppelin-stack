import json
from math import pi, sin, cos, asin, sqrt
from elasticsearch import Elasticsearch,helpers  # imports

CONNECTIONS_TUPLE = tuple()         #tuple that will be used in the helpers.bulk method
RADIUS = 6378135  # radius of the earth in meters, from NOAA: https://www.ngs.noaa.gov/PUBS_LIB/Geodesy4Layman/TR80003E.HTM
DIGITS = {*'1234567890'}

plants_index = 'powerplants'
lines_index = 'powerlines'
host = 'localhost'
port = 9200
elastic_pass = 'changeme'
elastic_uname = 'elastic'
connections_index_name = 'lineplantconnections'
mappingFile = 'connections_mapping'


def is_number(test):  # tests if a string is an whole number
    global DIGITS
    for char in str(test):
        if char not in DIGITS:
            return False
    return True


def prepare_variables():
    global mappingFile, plants_index, lines_index, connections_index_name, host, port, elastic_pass, elastic_uname

    possible_mapping = input(
        'If you would like to use a custom index mapping, please specify the text/json file to pull the mapping from. Otherwise, default file will be used:')
    mappingFile = possible_mapping if possible_mapping else mappingFile

    possible_name = input('Please type the name of the power line data index (defaults to "powerlines"):')
    lines_index = possible_name if possible_name else lines_index

    possible_name = input('Please type the name of the power plant data index (defaults to "powerplants"):')
    plants_index = possible_name if possible_name else plants_index

    possible_name = input(
        'Please type an all lowercase name for the connection data index (defaults to "lineplantconnections"):')
    connections_index_name = possible_name if possible_name else connections_index_name

    possible_host = input('Please type the host for elasticsearch (defaults to "localhost"):')
    host = possible_host if possible_host else host

    possible_port = input('Please type the port for elasticsearch (defaults to 9200):')
    port = possible_port if (possible_port and is_number(possible_port)) else port

    possible_pass = input('Please enter the password for elasticsearch (defaults to "elastic"):')
    elastic_pass = possible_pass if possible_pass else elastic_pass

    possible_uname = input('Please enter the password for elasticsearch (defaults to "changeme"):')
    elastic_uname = possible_uname if possible_uname else elastic_uname
  
def create_connection():
    elastic = Elasticsearch([{'host': host, 'port': port}],
                            http_auth=(elastic_uname,
                                       elastic_pass))

    return elastic


def prepare_elasticsearch():
    esearch = create_connection()
    if not esearch.indices.exists(index=connections_index_name):  # creates the index if it doesn't exist yet
        mapping = json.loads(open(mappingFile).read())  # pulls mapping from a file
        esearch.indices.create(index=connections_index_name, body=mapping)
    return esearch


def calc_distance(long1, lat1, long2,
                  lat2):  # calculates distance between two lat/long points using the Haversine formula
    radius = 6371 * (10 ** 3)  # radius of the earth in meters

    lat1_radians = lat1 * pi / 180

    lat2_radians = lat2 * pi / 180

    lat_difference = (lat2 - lat1) * pi / 180

    long_difference = (long2 - long1) * pi / 180

    first_part = sin(lat_difference / 2) ** 2 + cos(lat1_radians) * cos(lat2_radians) * sin(long_difference / 2) ** 2
    distance = 2 * RADIUS * asin(sqrt(first_part))
    return distance  # distance is in meters


def get_pline_data(elastic):  # gets power line data from elasticsearch, using scroll to get all power lines
    all_power_lines = []
    resp = elastic.search(index=lines_index, body={"size": 10000, "query": {"match_all": {}}}, scroll='30s')
    all_power_lines += resp['hits']['hits']

    old_scroll_id = resp['_scroll_id']
    while len(resp['hits']['hits']):
        resp = elastic.scroll(
            scroll_id=old_scroll_id,
            scroll='30s'
        )

        all_power_lines += resp['hits']['hits']  #adds all power lines to the same list

        if old_scroll_id != resp['_scroll_id']:
            print("NEW SCROLL ID:", resp['_scroll_id'])
        old_scroll_id = resp['_scroll_id']

    return all_power_lines


def process_lines(feature_list):  # processes list of power lines
    processed = {}
    for feature in feature_list:
        geometry = feature['_source']['shape']['coordinates']
        if len(geometry) > 1:
            if geometry[0][0] == geometry[1][0]:
                geometry = [geometry[1][::-1] + geometry[0]] + geometry[
                                                               2:]  # merges lines together if one section is an extension of another one
            elif geometry[0][-1] == geometry[1][0]:
                geometry = [geometry[0] + geometry[1]] + geometry[2:]
            elif geometry[0][0] == geometry[1][-1]:
                geometry = [geometry[1] + geometry[0]] + geometry[2:]
            elif geometry[0][-1] == geometry[1][-1]:
                geometry = [geometry[0] + geometry[1][::-1]] + geometry[2:]

        feature_id = feature['_source']['ID']
        processed[feature_id] = [geometry[-1][-1], geometry[0][0]]  # adds new power line to list of processed objects
    return processed


def find_matching_plants(point, line_id, es):             # matches power plants and lines
    global CONNECTIONS_TUPLE
    results = es.search(index="powerplants", body={"query": {
        "bool": {
            "must": {
                "match_all": {}
            },
            "should": {
                "distance_feature": {
                    "field": "location",
                    "pivot": "5000km",                   # large distance to guarantee at least one match, closest points first on return
                    "origin": point                      # point is a long-lat array
                }
            }
        }
    }})

    if len(results['hits']['hits']):
        plant_info = results['hits']['hits'][0]['_source']['_doc_source']
        plant_id = plant_info['gppd_idnr']
        plant_location = plant_info['location']
        distance1 = calc_distance(plant_location[0], plant_location[1], point[0], point[1])
        CONNECTIONS_TUPLE = CONNECTIONS_TUPLE + tuple([{'_index': connections_index_name, '_ID': len(CONNECTIONS_TUPLE),
                                                      '_source': {'line_id': line_id, 'plant_id': plant_id,
                                                                  'distance': distance1}}])        
        return (plant_id, plant_location)


def make_line_plant_connections(power_line_data, elastic_search):
    connections = {}

    for id in power_line_data:
        start_of_pline = power_line_data[id][0]
        end_of_pline = power_line_data[id][0]
        connections[id] = {}
        connections[id]['first'] = find_matching_plants(start_of_pline, id, elastic_search)
        connections[id]['second'] = find_matching_plants(end_of_pline, id, elastic_search)

    return connections

prepare_variables()
elastic_connection = prepare_elasticsearch()
loaded_data = get_pline_data(elastic_connection)
processed_data = process_lines(loaded_data)
connections_dictionary = make_line_plant_connections(processed_data, elastic_connection)

helpers.bulk(elastic_connection, CONNECTIONS_TUPLE)
