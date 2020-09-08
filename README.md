# Zeppelin-Stack
I really don't listen to Led Zeppelin that much.
`- Jimmy Page`

Description: Includes a docker-compose file and other neccesary files to prepare and start docker containers containing elasticsearch, geoserver, zeppelin, kibana, and logstash, as well as python scripts to ingest and index power plant and power line data into elasticsearch and connect the ends of power lines to the closest power plants and index the distances. Also contains kibana dashboards for all the data.

Usage:

Download docker (https://www.docker.com/get-started) and Python.
Run "docker-compose up -d" in the command prompt from the zeppelin-stack directory.
Wait a couple of minutes
Check if kibana is running at localhost port 5601. You will need to sign in with username "elastic" and password "changeme"
Once kibana is running, run "python ppingest.py" from command prompt in a directory of your choice. This will download the power plant data to the same directory that the python file is in.
Run "python ingest_power_lines.py" from command prompt in a directory of your choice. This can also be done before running ppingest.py.
With both power plant and power line data indexed, run "python plant_line_distances.py" in command prompt to index the data regarding distances between power line endpoints and closest power plants.
After the distance data is indexed, run "python plant_line_connections.py" to create an index with connections where the power line endpoint is expected connected to a power plant (distance <= 500m)
Open up kibana and create index patterns for the 4 new indexes created.
Import the energy infrastructure dashboard and connections dashboard into Kibana. The default index patterns (powerplants, powerlines, lineplantconnections) may not match what you named the indices/index patterns, so you may need to change some of the visualizations.
