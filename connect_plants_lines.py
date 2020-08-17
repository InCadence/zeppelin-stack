import geojson
import requests
import json
from math import pi, sin, cos, asin, sqrt
from elasticsearch import Elasticsearch,helpers  # imports

DISTANCES = []

plants_index = 'powerplants'
data_url = 'https://opendata.arcgis.com/datasets/70512b03fe994c6393107cc9946e5c22_0.geojson'
host = 'localhost'
port = 9200
elastic_pass = 'changeme'
elastic_uname = 'elastic'
connections_index_name = 'lineplantconnections'
mappingFile = 'connections_mapping'


def create_connection():
    elastic = Elasticsearch([{'host': host, 'port': port}],
                            http_auth=(elastic_uname,
                                       elastic_pass))

    return elastic


def prepare_elasticsearch():
    mapping = json.loads(open(mappingFile).read())  # pulls mapping from a file
    esearch = create_connection()
    if not esearch.indices.exists(index=connections_index_name):  # creates the index if it doesn't exist yet
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
    distance = 2 * radius * asin(sqrt(first_part))
    return distance  # distance is in meters


def get_pline_data():
    print('starting load')
    return geojson.loads(requests.get(data_url, allow_redirects=True).content)[
        "features"]  # gets data from url, then converts the geojson file into a dictionary of all the powerlines,
    # then returns said dictionary


def process_lines(feature_list):  # processes dictionary of power lines
    processed = {}
    for feature in feature_list:
        geometry = feature['geometry']['coordinates']
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

        info = feature['properties']
        processed[info['ID']] = [geometry[-1][-1], geometry[0][0]]  # adds new power line to list of processed objects
    return processed


def find_matching_plants(point, line_id, es):             # matches power plants and lines
    global DISTANCES
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
        plant_id = results['hits']['hits'][0]['_source']['gppd_idnr']
        plant_location = results['hits']['hits'][0]['_source']['location']
        distance1 = calc_distance(plant_location[0], plant_location[1], point[0], point[1])
        DISTANCES.append({'line_id': line_id, 'plant_id': plant_id, 'distance': distance1})   #global distances dictionary
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


loaded_data = get_pline_data()
elastic_connection = prepare_elasticsearch()
print('retrieved and loaded')
processed_data = process_lines(loaded_data)
connections_dictionary = make_line_plant_connections(processed_data, elastic_connection)

bulk_connections_list = ({
    '_index': connections_index_name,
    '_ID': position,
    '_source': connection} for position, connection in enumerate(DISTANCES)
)

helpers.bulk(elastic_connection,bulk_connections_list)
