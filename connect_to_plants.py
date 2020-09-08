import json
from math import pi, sin, cos, asin, sqrt
from elasticsearch import Elasticsearch, helpers  # imports

CONNECTIONS_TUPLE = tuple()  # tuple that will be used in the helpers.bulk method
DIGITS = {*'1234567890'}

host = 'localhost'
port = 9200
elastic_pass = 'changeme'
elastic_uname = 'elastic'
distances_index_name = 'lineplantdistances'
connections_index_name = 'lineplantconnections'
mappingFile = 'connections_mapping'


def is_number(test):  # tests if a string is an whole number
    global DIGITS
    for char in str(test):
        if char not in DIGITS:
            return False
    return True


def prepare_variables():
    global mappingFile, distances_index_name, connections_index_name, host, port, elastic_pass, elastic_uname

    possible_mapping = input(
        'If you would like to use a custom index mapping, please specify the text/json file to pull the mapping from. Otherwise, default file will be used:')
    mappingFile = possible_mapping if possible_mapping else mappingFile

    possible_distances = input(
        'Please type an all lowercase name for the connection data index (defaults to "lineplantdistances"):')
    distances_index_name = possible_distances if possible_distances else distances_index_name

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

def get_distance_data(elastic):  # gets data for all power line endpoints where the distance to the power plant <= 500m
    all_power_lines = []
    resp = elastic.search(index=distances_index_name, body={"size": 1000, "query": {"range": {"distance":{"lte":500}}}}, scroll='30s')
    all_power_lines += resp['hits']['hits']

    old_scroll_id = resp['_scroll_id']
    while len(resp['hits']['hits']):
        resp = elastic.scroll(
            scroll_id=old_scroll_id,
            scroll='30s'
        )

        all_power_lines += resp['hits']['hits']  # adds all power lines to the same list

        if old_scroll_id != resp['_scroll_id']:
            print("NEW SCROLL ID:", resp['_scroll_id'])
        old_scroll_id = resp['_scroll_id']

    return all_power_lines


def process_distances(feature_list):  # processes list probably connected power lines and power plants
    global CONNECTIONS_TUPLE
    processed = {}
    for idx,feature in enumerate(feature_list):
        info = feature['_source']
        CONNECTIONS_TUPLE = CONNECTIONS_TUPLE + tuple([{'_index':connections_index_name , '_ID':idx, '_source':info}])
    return processed



prepare_variables()
elastic_connection = prepare_elasticsearch()
loaded_data = get_distance_data(elastic_connection)
processed_data = process_distances(loaded_data)
helpers.bulk(elastic_connection, CONNECTIONS_TUPLE)