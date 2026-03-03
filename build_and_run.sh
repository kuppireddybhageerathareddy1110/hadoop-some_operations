#!/bin/bash
# Compile Java code
mkdir -p build
javac -cp $(hadoop classpath) -d build DegreeCounter.java
jar -cvf degreecounter.jar -C build .

# Initialize HDFS (run once or on reset)
hdfs namenode -format

# Start HDFS
start-dfs.sh

# Create HDFS directory and upload data
hdfs dfs -mkdir -p /user/$USER/input
hdfs dfs -put social_network.txt /user/$USER/input/

# Run MapReduce job
hadoop jar degreecounter.jar DegreeCounter \
    /user/$USER/input/social_network.txt \
    /user/$USER/output

# View results
echo "Job results:"
hdfs dfs -cat /user/$USER/output/part-r-00000

# Stop Hadoop
stop-dfs.sh
