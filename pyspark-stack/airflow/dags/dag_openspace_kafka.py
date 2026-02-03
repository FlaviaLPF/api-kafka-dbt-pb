from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import sys
from airflow.models import Variable
import os

# Adiciona o diretório de scripts ao path do Python para importar consumer_kafka
sys.path.append("/opt/spark-apps/script")
import consumer_kafka

# Variáveis e Configurações
env = Variable.get("environment", default_var="dev")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 8),
    "retries": 0,
}

dag = DAG(
    dag_id="opensky_sp",
    description="Pipeline de ingestão e processamento de voos SBGR",
    max_active_runs=1,
    #schedule_interval="*/5 * * * *",  # <--- 5 em 5 min
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
)

# --- DEFINIÇÃO DAS TASKS ---

# 1. Producer Kafka
task_producer = BashOperator(
    task_id="producer_flights",
    bash_command="python /opt/spark-apps/script/producer_kafka.py",
    env={"EXECUTION_DATE": "{{ ds }}"},
    dag=dag,
)

# 2. Consumer Kafka (gera o arquivo JSON e retorna contagem)
task_consume = PythonOperator(
    task_id="consume_flights",
    python_callable=consumer_kafka.consume_flights,
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

# 3. Branch: decide se segue para o Spark ou encerra
def branch_consumer(**context):
    ti = context["ti"]
    count = ti.xcom_pull(task_ids="consume_flights")
    try:
        count = int(count) if count is not None else 0
    except Exception:
        count = 0
    if count > 0:
        return "task_parquet"
    return "task_end"

branch = BranchPythonOperator(
    task_id="branch_consumer",
    python_callable=branch_consumer,
    provide_context=True,
    dag=dag,
)

# 4. Save to Parquet (Lê o JSON específico da rodada no staging)
task_parquet = SparkSubmitOperator(
    task_id="task_parquet",
    application="/opt/spark-apps/script/save_parquet.py",
    conn_id="spark_default",
    application_args=[
        "dev", 
        "/opt/spark-apps/staging/flights_{{ ts_nodash }}.json"
    ],
    env_vars={"HADOOP_USER_NAME": "flavia"},
    dag=dag  
)

# 5. Parquet to SQL Server
task_parquet_to_sql = SparkSubmitOperator(
    task_id="task_parquet_to_sql",
    conn_id="spark_default",
    application="/opt/spark-apps/script/spark_to_sql.py",
    application_args=["dev", "{{ ds }}"],
    jars="/opt/spark/jars/mssql-jdbc-12.4.2.jre11.jar", 
    env_vars={"HADOOP_USER_NAME": "flavia"},
    conf={
        "spark.hadoop.fs.defaultFS": "hdfs://hdfs-namenode:9000",
        "spark.driver.extraClassPath": "/opt/spark/jars/mssql-jdbc-12.4.2.jre11.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/mssql-jdbc-12.4.2.jre11.jar"
    },
    dag=dag
)

# 5.5 Arquivamento do arquivo JSON (Move do staging para o processed)
task_archive = BashOperator(
    task_id="archive_json",
    bash_command="mkdir -p /opt/spark-apps/processed && mv /opt/spark-apps/staging/flights_{{ ts_nodash }}.json /opt/spark-apps/processed/",
    dag=dag,
)


# 6. DBT Run (Transformação Bronze -> Silver -> Gold)
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
    dag=dag,
)

# 7. DBT Test (Garantia de Qualidade)
task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
    dag=dag,
)

# 8. Fim
task_end = DummyOperator(
    task_id="task_end", 
    trigger_rule="none_failed_min_one_success",
    dag=dag
)
# --- ORDENAÇÃO DO FLUXO ---

task_producer >> task_consume >> branch

# Fluxo A: Só acontece se o branch decidir que tem dados
# O dbt AGORA faz parte obrigatória deste caminho
branch >> task_parquet >> task_parquet_to_sql >> task_archive >> task_dbt_run >> task_dbt_test >> task_end

# Fluxo B: Se não houver dados, pula tudo e vai direto para o fim
branch >> task_end