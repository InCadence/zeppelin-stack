import json
from math import pi, sin, cos, asin, sqrt, atan2, acos
from elasticsearch import Elasticsearch, helpers  # imports

POINTS_OF_INTERSECTION = {}     
BULK_TUPLE = ()
DIGITS = {*'1234567890'}
RADIUS = 6378135        # radius of the earth in meters, from NOAA: https://www.ngs.noaa.gov/PUBS_LIB/Geodesy4Layman/TR80003E.HTM
TYPE = 'multilinestring'  # constant type for the multiline strings which represent the power lines

lines_index = 'powerlines'
host = 'localhost'          #host and port for elasticsearch
port = 9200
elastic_pass = 'changeme'
elastic_uname = 'elastic'
substations_index_name = 'substations'
mappingFile = 'substation_mapping'


def is_number(test):  # tests if a string is an whole number
    global DIGITS
    for char in str(test):
        if char not in DIGITS:
            return False
    return True

def prepare_variables():
    global mappingFile, substations_index_name, host, port, elastic_pass, elastic_uname, lines_index            #optional inpujts for elasticsearch connection details and index names

    possible_mapping = input(
        'If you would like to use a custom index mapping, please specify the text/json file to pull the mapping from. Otherwise, default file will be used:')
    mappingFile = possible_mapping if possible_mapping else mappingFile

    possible_lines = input(
        'Please type the name for the power line data index (defaults to "powerlines"):')
    lines_index = possible_lines if possible_lines else lines_index

    possible_name = input(
        'Please type an all lowercase name for the substations data index (defaults to "probable_substations"):')
    substations_index_name = possible_name if possible_name else substations_index_name

    possible_host = input('Please type the host for elasticsearch (defaults to "localhost"):')
    host = possible_host if possible_host else host

    possible_port = input('Please type the port for elasticsearch (defaults to 9200):')
    port = possible_port if (possible_port and is_number(possible_port)) else port

    possible_pass = input('Please enter the password for elasticsearch (defaults to "elastic"):')
    elastic_pass = possible_pass if possible_pass else elastic_pass

    possible_uname = input('Please enter the password for elasticsearch (defaults to "changeme"):')
    elastic_uname = possible_uname if possible_uname else elastic_uname

def create_connection():
    global host,port,elastic_pass,elastic_uname
    elastic = Elasticsearch([{'host': host, 'port': port}],                 #creates elesticsearch connection with either inputted information or default
                            http_auth=(elastic_uname,
                                       elastic_pass))

    return elastic


def prepare_elasticsearch():
    esearch = create_connection()                                                       #creares connection
    if not esearch.indices.exists(index=substations_index_name):                        # creates the index if it doesn't exist yet
        mapping = json.loads(open(mappingFile).read())                                                  # pulls mapping from a file
        esearch.indices.create(index=substations_index_name, body=mapping)
    return esearch


def get_pline_data(elastic):
    all_power_lines = []                                                                                #scrolls through all power lines
    resp = elastic.search(index=lines_index, body={"size": 10000, "query": {"match_all": {}}}, scroll='30s')
    all_power_lines += resp['hits']['hits']                                                             #to collect all power line data into one list
    old_scroll_id = resp['_scroll_id']
    while len(resp['hits']['hits']):
        resp = elastic.scroll(
            scroll_id=old_scroll_id,
            scroll='30s'  # length of time to keep search context
        )

        all_power_lines += resp['hits']['hits']

        if old_scroll_id != resp['_scroll_id']:
            print("NEW SCROLL ID:", resp['_scroll_id'])
        old_scroll_id = resp['_scroll_id']
    return all_power_lines                                          #list of all power lines


def magnitude(vector):                                          #finds the magnitude of a vector with parts stored in either a list or tuple
    return sqrt(sum(vector_part ** 2 for vector_part in vector))


def crossProduct(vector1, vector2):                             #finds the cross product of vector1 with vector 2
    return (vector1[1] * vector2[2] - vector1[2] * vector2[1], (vector1[0] * vector2[2] - vector1[2] * vector2[0]),
            vector1[0] * vector2[1] - vector1[1] * vector2[0])


def dotProduct(vector1, vector2):           #dot product of two vectors
    return sum(vector1[i] * vector2[i] for i in range(len(vector1)))


def calc_point_intersection(point1, point2, point11, point12):      #finds point of intersection between two lines, if there is any. Based off of
    if point1 == point11 or point1 == point12:                      #https://blog.mbedded.ninja/mathematics/geometry/spherical-geometry/finding-the-intersection-of-two-arcs-that-lie-on-a-sphere/
        return tuple(point1)
    if point2 == point11 or point2 == point12:          #if either line is attached to the end of the other, return that point
        return tuple(point2)

    point1 = [val * pi / 180 for val in point1]         #converts lat/long to radians
    point2 = [val * pi / 180 for val in point2]
    point11 = [val * pi / 180 for val in point11]
    point12 = [val * pi / 180 for val in point12]

    xyz1 = (RADIUS * cos(point1[0]) * cos(point1[1]), RADIUS * cos(point1[0]) * sin(point1[1]), RADIUS * sin(point1[0]))        #finds the cartesian coordinate representation of the lat/long point
    xyz2 = (RADIUS * cos(point2[0]) * cos(point2[1]), RADIUS * cos(point2[0]) * sin(point2[1]), RADIUS * sin(point2[0]))
    xyz11 = (RADIUS * cos(point11[0]) * cos(point11[1]), RADIUS * cos(point11[0]) * sin(point11[1]), RADIUS * sin(point11[0]))
    xyz12 = (RADIUS * cos(point12[0]) * cos(point12[1]), RADIUS * cos(point12[0]) * sin(point12[1]), RADIUS * sin(point12[0]))

    crossProd1 = crossProduct(xyz1, xyz2)           #finds the cross product of the vectors of the points of the first two arc
    crossProd2 = crossProduct(xyz11, xyz12)

    crossProd_of_crossProds = crossProduct(crossProd1, crossProd2)
    magnitude_of_cross = magnitude(crossProd_of_crossProds)
    if magnitude_of_cross == 0:
        return ()
    crossProd_of_crossProds = [val / magnitude_of_cross for val in crossProd_of_crossProds]
    negative_cross_prod = [val * -1 for val in crossProd_of_crossProds]

    for possibility in [negative_cross_prod, crossProd_of_crossProds]:
        value1 = acos(max(min(dotProduct(xyz1, possibility) / (magnitude(xyz1) * magnitude(possibility)), 1),-1))
        value2 = acos(max(min(dotProduct(xyz2, possibility) / (magnitude(xyz2) * magnitude(possibility)), 1),-1))
        value3 = acos(max(min(dotProduct(xyz1, xyz2) / (magnitude(xyz1) * magnitude(xyz2)), 1),-1))
        if not (value3 - 0.0001 <= value1 + value2 <= value3 + 0.0001):
            continue
        value1 = acos(max(min(dotProduct(xyz11, possibility) / (magnitude(xyz11) * magnitude(possibility)), 1),-1))
        value2 = acos(max(min(dotProduct(xyz12, possibility) / (magnitude(xyz12) * magnitude(possibility)), 1),-1))
        value3 = acos(max(min(dotProduct(xyz11, xyz12) / (magnitude(xyz11) * magnitude(xyz12)), 1),-1))
        if value3 - 0.0001 <= value1 + value2 <= value3 + 0.0001:
            expectedlong = atan2(possibility[1], possibility[0]) * 180 / pi
            expectedlat = asin(possibility[2]) * 180 / pi
            if expectedlat < 0 or expectedlong > 0:         #some points end up in the middle of the pacific (not at hawaii).
                return ()       #I still haven't figured out what causes this, so omitted those points for now
            return (expectedlong, expectedlat)

    return (0, 0)


def process_lines(feature_list):  # processes dictionary of power lines
    processed = {}
    for feature in feature_list:
        geometry = feature['_source']['shape']['coordinates']
        if len(geometry) > 1:
            if geometry[0][0] == geometry[1][0]:
                geometry = [geometry[1][::-1] + geometry[0]] + geometry[2:]  # merges lines together if one section is an extension of another one
            elif geometry[0][-1] == geometry[1][0]:
                geometry = [geometry[0] + geometry[1]] + geometry[2:]
            elif geometry[0][0] == geometry[1][-1]:
                geometry = [geometry[1] + geometry[0]] + geometry[2:]
            elif geometry[0][-1] == geometry[1][-1]:
                geometry = [geometry[0] + geometry[1][::-1]] + geometry[2:]

        feature_id = feature['_source']['ID']
        processed[feature_id] = geometry  # adds new power line to list of processed objects
    return processed


def find_intersections(line, line_id, es):
    global POINTS_OF_INTERSECTION
    results = es.search(index=lines_index, body={"query": {         #finds all power lines that intersect the current one
        "bool": {
            "must": {
                "match_all": {}                 #finds all lines that intersect
            },
            "filter": {
                "geo_shape": {
                    "shape": {
                        "shape": {
                            "type": TYPE,
                            "coordinates": line
                        },
                        "relation": "intersects"
                    }
                }
            }
        }
    }})

    for hit in results['hits']['hits']:
        result_line_id = hit['_source']['ID']
        if result_line_id == line_id:                   #skips if the intersecting line is the current line
            continue
        line_shape = hit['_source']['shape']['coordinates']
        for segment in line:        #for each segment in the first power line
            for segment2 in line_shape:     #for each segment in the second as well
                for i in range(len(segment) - 1):       #for each section of the first segment
                    for j in range(len(segment2) - 1):      #for each section of the second segment
                        point_of_intersect = calc_point_intersection(segment[i], segment[i + 1], segment2[j],  #check if the two sections intersect
                                                                     segment2[j + 1])
                        if not point_of_intersect: #if the intersection method returns an empty tuple, the segments do not intersect
                            continue
                        if point_of_intersect not in POINTS_OF_INTERSECTION:        #instantiate set if not added yet
                            POINTS_OF_INTERSECTION[point_of_intersect] = set()
                        POINTS_OF_INTERSECTION[point_of_intersect].add((line_id))                   #stores the ids of the two lines that connect in the set for the point
                        POINTS_OF_INTERSECTION[point_of_intersect].add((result_line_id))

        return result_line_id, line_shape


def make_line_connections(power_line_data, elastic_search):         #goes through all the power lines and finds connections
    connections = {}
    for id in power_line_data:
        line = power_line_data[id]
        connections[id] = {}
        connections[id]['first'] = find_intersections(line, id, elastic_search)


    return connections


def process_connections():
    global POINTS_OF_INTERSECTION, BULK_TUPLE
    idx = 0
    for point in POINTS_OF_INTERSECTION:
        if len(POINTS_OF_INTERSECTION[point]) < 3:              #only stores intersections with more than three power lines crossing
            continue                                            #I havem't accounted for multiple intersections with less than three crossing possibly being a substation, this could probably be done by
        info = {"point_of_intersection": [point[0], point[1]]}  #grouping together intersections which are within x meters of each other
        BULK_TUPLE = BULK_TUPLE + tuple([{'_index': substations_index_name, '_ID': idx, '_source': info}])
        idx += 1


prepare_variables()
elastic_connection = prepare_elasticsearch()
loaded_data = get_pline_data(elastic_connection)
processed_data = process_lines(loaded_data)
connections_dictionary = make_line_connections(processed_data, elastic_connection)
process_connections()
print(BULK_TUPLE)
helpers.bulk(elastic_connection, BULK_TUPLE)
