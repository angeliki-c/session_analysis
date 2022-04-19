# assign the desired permission to the folder of the project for accessing code + data
sudo chmod 700 -R the/location/of/your/project/session_analysis/
# start the ssh client and server
sudo service ssh --full-restart
# start hadoop
start-dfs.sh
# copy the data to hadoop file system
hdfs dfs -put the/location/of/your/project/session_analysis/data/ hdfs://localhost:9000/user/
# start pyspark including registering the 'apache sedona' and 'geotools' packages for processing geospatial data
pyspark pyspark --packages org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating,org.apache.sedona:sedona-viz-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2