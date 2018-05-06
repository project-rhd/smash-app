#!/usr/bin/env bash

#======= Primary Settings Start ==============================

# Ip address of spark master.
spark_master_address=smash-1-master
# Ip address of hadoop master. (the name node)
hadoop_master_address=smash-1-master
# Directory in hdfs for placing jar. e.g. /scats
jar_hdfs_dir=/tweets

# Local Spark home directory e.g. /usr/local/spark
client_spark_home=/home/darcular/Applications/spark-2.1.1-bin-hadoop2.6
# Local Hadoop home e.g. /usrl/local/hadoop
client_hadoop_home=/home/darcular/Applications/hadoop-2.6.0

accumulo_instance_id=smash
# smash-1-master:2181,smash-1-slave:2181,smash-2-slave:2181
zookeepers=smash-1-master:2181,smash-1-slave:2181,smash-2-slave:2181,smash-3-slave:2181,smash-4-slave:2181,smash-5-slave:2181,smash-6-slave:2181,smash-7-slave:2181
accumulo_user_name=root
accumulo_user_pwd=smash
accumulo_table_name=tweets

jar_name=smash-tweets-app-0.1.0.jar


#======= Primary Settings End ================================
app_base=$(dirname $(readlink -f $0))/../
hdfs_root=hdfs://${hadoop_master_address}:9000

echo "Task 1: Build application jar locally"
mvn -f ${app_base}/../../pom.xml -pl smash-tweets/smash-tweets-app clean install -am -DskipTests
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

echo "Task 3: Submit task in cluster mode to the spark master:" spark://${spark_master_address}:6066
${client_spark_home}/bin/spark-submit \
--deploy-mode cluster \
--master spark://${spark_master_address}:6066 \
--executor-cores 3 \
--total-executor-cores 18 \
--driver-memory 8G \
--executor-memory 8G \
--conf spark.eventLog.enabled=true \
--class "smash.app.tweets.classifier.OnStreetTweetsDetector" \
${hdfs_root}${jar_hdfs_dir}/${jar_name} \
--instanceId ${accumulo_instance_id} \
--zookeepers ${zookeepers} \
--user ${accumulo_user_name} \
--password ${accumulo_user_pwd} \
--tableName ${accumulo_table_name} \
--overwrite


if [ $? -eq 0 ]; then
    echo "Task 3 Finished"
else
    echo "Error. Failed in Task 3"
    exit 1
fi

echo "Task submitted. Check" http://${spark_master_address}:8080 "for details."


