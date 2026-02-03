#!/bin/bash
set -euo pipefail
set -x

export PATH=$PATH:/opt/hadoop/bin
export SPARK_HOME=/opt/spark

# O $1 captura o parâmetro {env} enviado pelo Airflow
ENV_ARG=${1:-"default"} 


docker exec pyspark-stack-spark-master-1 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf "spark.ui.enabled=false" \
  --conf "spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000" \
  --conf "HADOOP_USER_NAME=hdfs" \
  /opt/spark-apps/script/save_parquet.py "$ENV_ARG"

#spark-submit \
#  --master local[*] \
#  --conf "spark.driver.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
#  --conf "spark.executor.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
#  --conf "spark.ui.enabled=false" \
#  --conf "spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000" \
#  --conf "HADOOP_USER_NAME=hdfs" \
#  /opt/spark-apps/script/save_parquet.py "$ENV_ARG"





  
  
  
 

