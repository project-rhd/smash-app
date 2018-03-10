#! /bin/sh

#======= Primary Settings Start ==============================

# Ip address of spark master.
spark_master_address=smash-1-master
# Ip address of hadoop master. (the name node)
hadoop_master_address=smash-1-master
# Directory in hdfs for placing jar. e.g. /scats
jar_hdfs_dir=/scats

# Local Spark home directory e.g. /usr/local/spark
client_spark_home=/home/darcular/Applications/spark-2.1.1-bin-hadoop2.6
# Local Hadoop home e.g. /usrl/local/hadoop
client_hadoop_home=/home/darcular/Applications/hadoop-2.6.0

accumulo_instance_id=smash
# smash-1-master:2181,smash-1-slave:2181,smash-2-slave:2181
zookeepers=smash-1-master:2181,smash-1-slave:2181,smash-2-slave:2181,smash-3-slave:2181,smash-4-slave:2181,smash-5-slave:2181,smash-6-slave:2181,smash-7-slave:2181
accumulo_user_name=root
accumulo_user_pwd=smash
accumulo_table_name=scats_2017

# Path of raw data file in hdfs. e.g. /scats/sample700M.csv  VolumeData.CSV  volume_1G.csv volume.csv
INPUT_VOLUME_FILE=/scats/2017/VSDATA_2017.csv
INPUT_LAYOUT_FILE=/scats/SiteLayouts.csv
INPUT_SHAPE_FILE=/scats/traffic_lights_2016.7.csv  #tlights_vic_4326.csv
INPUT_SHAPE_FILE2=/scats/uniq-roads.csv

jar_name=smash-scats-app-0.1.0.jar

#======= Primary Settings End ================================

app_base=$(dirname $(readlink -f $0))/../
hdfs_root=hdfs://${hadoop_master_address}:9000
input_volume_url=${hdfs_root}${INPUT_VOLUME_FILE}
input_layouts_url=${hdfs_root}${INPUT_LAYOUT_FILE}
input_shp_url=${hdfs_root}${INPUT_SHAPE_FILE}
input_shp_url2=${hdfs_root}${INPUT_SHAPE_FILE2}

echo "Task 1: Build application jar locally"
mvn -f ${app_base}/../../pom.xml -pl smash-scats/smash-scats-app clean package -am -DskipTests
if [ $? -eq 0 ]; then
    echo "Task 1 Finished"
else
    echo "Error. Failed in task 1"
    exit 1
fi

echo "Task 2: Upload jar to remote HDFS:" ${hdfs_root}${jar_hdfs_dir}
${client_hadoop_home}/bin/hdfs dfs -put -f ${app_base}/target/${jar_name} ${hdfs_root}${jar_hdfs_dir}
if [ $? -eq 0 ]; then
    echo "Task 2 Finished"
else
    echo "Error. Failed in Task 2"
    exit 1
fi


# Option: Use "--conf spark.hadoop.validateOutputSpecs=false" for overwriting existing files.
# But better to delete the whole output directory if exists before saving.
echo "Task 3: Submit task in cluster mode to the spark master:" spark://${spark_master_address}:6066
${client_spark_home}/bin/spark-submit \
--master spark://${spark_master_address}:6066 \
--driver-memory 3G \
--executor-memory 3G \
--class "smash.app.scats.importer.ScatsImporter" \
--deploy-mode cluster \
--conf spark.eventLog.enabled=true \
${hdfs_root}${jar_hdfs_dir}/${jar_name} \
--instanceId ${accumulo_instance_id} \
--zookeepers ${zookeepers} \
--user ${accumulo_user_name} \
--password ${accumulo_user_pwd} \
--tableName ${accumulo_table_name} \
--overwrite \
--inputVolumeCSV ${input_volume_url} \
--inputLayoutCSV ${input_layouts_url} \
--inputPointShapeFile ${input_shp_url} \
--inputLineShapeFile ${input_shp_url2}

if [ $? -eq 0 ]; then
    echo "Task 3 Finished"
else
    echo "Error. Failed in Task 3"
    exit 1
fi

echo "Task submitted. Check" http://${spark_master_address}:8080 "for details."
