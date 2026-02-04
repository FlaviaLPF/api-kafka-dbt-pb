import os
import sys
import logging
from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession


# ==============================================================================
# CONFIGURAÇÃO DE LOGS (Arquivo Exclusivo para o Spark)
# ==============================================================================
LOG_FILE = "/opt/spark-apps/logs/parquet_to_sql.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger = logging.getLogger("flights_parquet_save_sql")
logger.setLevel(logging.INFO)

if not logger.handlers:
    # 1. Handler para o arquivo físico exclusivo
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)

    # 2. Handler para o console (Para visualização nos logs do Airflow)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

##logging.basicConfig(filename=LOG_FILE, level=logging.INFO, 
#                    format="%(asctime)s %(levelname)s: %(message)s")

# Busca a conexão na UI do Airflow
conn = BaseHook.get_connection("mssql_default")

def main():
    # 1. Verifica argumentos antes de tudo
    if len(sys.argv) < 2:
        logger.error("Data de execução não fornecida.")
        sys.exit(1)

    
    execution_date = sys.argv[2] if len(sys.argv) > 2 else sys.argv[1]
    logger.info(f"Iniciando carga Parquet -> SQL Server Raw para data: {execution_date}")

    spark = SparkSession.builder \
        .appName(f"ParquetToSQLServer-{execution_date}") \
        .getOrCreate()

    # Configurações do SQL Server
    jdbc_url = f"jdbc:sqlserver://{conn.host}:{conn.port};databaseName=dwFlights;encrypt=false;trustServerCertificate=true"
    jdbc_properties = {
        "user": conn.login,
        "password": conn.password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    

    try:
        
        path_parquet = f"hdfs://hdfs-namenode:9000/data/flights/parquet/processing_date={execution_date}"
        
        logging.info(f"Lendo dados de {path_parquet}")
        df = spark.read.parquet(path_parquet)           

        count_records = df.count()
        logger.info(f"Quantidade de registros para carregar: {count_records}")

        if count_records > 0:
            logger.info("Gravando na tabela raw.flights_raw...")
            df.write.jdbc(
                url=jdbc_url, 
                table="raw.flights_raw", 
                mode="append", 
                properties=jdbc_properties
            )
            logger.info("Carga finalizada com sucesso!")
        else:
            logger.warning(f"Nenhum registro encontrado para a data {execution_date}")

    except Exception as e:
        logger.error(f"Erro na carga para o SQL Server: {str(e)}")
        raise
    finally:
        logger.info("Encerrando Spark Session.")
        spark.stop()

if __name__ == "__main__":
    main()
