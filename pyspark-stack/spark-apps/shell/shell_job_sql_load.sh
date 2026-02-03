#!/bin/bash
set -euo pipefail
set -x

export PATH=$PATH:/opt/hadoop/bin
export SPARK_HOME=/opt/spark
#!/bin/bash
# ... (manter exports e configurações de conf)

ENV_ARG=${1:-"default"} 
EXEC_DATE=${2:-""} # Captura a data vinda da DAG



spark-submit \
  --master spark://pyspark-stack-spark-master-1:7077 \
  --deploy-mode client \
  --packages com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11 \
  --conf "spark.ui.enabled=false" \
  --conf "spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000" \
  --conf "HADOOP_USER_NAME=hdfs" \
  /opt/spark-apps/script/spark_to_sql.py "$EXEC_DATE"

  #--master local[*] \
  #--packages com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11 \
  #--packages com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11 \
  #--conf "spark.driver.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
  #--conf "spark.executor.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
  #--conf "spark.ui.enabled=false" \
  #--conf "spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000" \
  #--conf "HADOOP_USER_NAME=hdfs" \
  #/opt/spark-apps/script/spark_to_sql.py "$EXEC_DATE"





  
  
  
 

