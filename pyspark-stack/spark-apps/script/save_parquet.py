import os
import sys
import json
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, ArrayType
)
from pyspark.sql.functions import (
    explode, from_json, col, lit, size)

# ==============================================================================
# CONFIGURAÇÃO DE LOGS
# ==============================================================================
LOG_FILE = "/opt/spark-apps/logs/flights_save_parquet.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger = logging.getLogger("flights_spark_save_parquet")
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

# ==============================================================================
# CAPTURA DE ARGUMENTOS
# ==============================================================================
ENV_ARG = sys.argv[1] if len(sys.argv) > 1 else "dev"
INPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else None
DATE_ARG = datetime.now().strftime('%Y-%m-%d')

if not INPUT_PATH:
    logger.error("ERRO: O caminho do arquivo JSON de entrada não foi fornecido.")
    sys.exit(1)

OUTPUT_PATH = "hdfs://hdfs-namenode:9000/data/flights/parquet"
SCHEMA_PATH = "/opt/spark-apps/config/schema.json"

def main():
    logger.info("-" * 60)
    logger.info(f"AMBIENTE: {ENV_ARG} | ARQUIVO DE ENTRADA: {INPUT_PATH}")

    spark = SparkSession.builder \
        .appName("save_parquet_job") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    try:
        # 1. VOLTAMOS PARA A LEITURA COMO TEXTO (Mais seguro para listas de listas)
        # O Spark lê o conteúdo do arquivo inteiro como uma string na coluna 'value'
        df_text = spark.read.text(INPUT_PATH)

        # 2. CONVERSÃO DO JSON (Usando a sua lógica original que funciona)
        # Como o arquivo é "[ [...], [...] ]", o schema é Array de Arrays de Strings
        df_json = df_text.select(
            from_json(col("value"), ArrayType(ArrayType(StringType()))).alias("rows")
        )

        # 3. EXPLODE PARA LINHAS
        df_exploded = df_json.select(explode(col("rows")).alias("row"))

        # --- VALIDAÇÃO DE ENTRADA ---
        count_input = df_exploded.count()
        logger.info(f"REGISTROS LIDOS: {count_input}")

        # 4. Carrega o Schema JSON para o mapeamento
        with open(SCHEMA_PATH) as f:
            schema_config = json.load(f)

        # 5. Validação de Contrato 
        actual_columns_count = df_exploded.select(size(col("row"))).first()[0]
        expected_columns_count = len(schema_config)
        
        if expected_columns_count != actual_columns_count:
            raise ValueError(f"CONTRATO QUEBRADO: Esperado {expected_columns_count}, recebido {actual_columns_count}")

        # 6. Aplica conversão de tipos conforme o schema.json
        select_expr = [
            col("row")[i].cast(dtype).alias(col_name)
            for i, (col_name, dtype) in enumerate(schema_config.items())
        ]
        
        df_final = df_exploded.select(*select_expr)

        # 7. Adiciona data de processamento
        df_final = df_final.withColumn("processing_date", lit(DATE_ARG))

        # 8. Escreve em Parquet no HDFS
        logger.info(f"GRAVANDO EM PARQUET NO HDFS: {OUTPUT_PATH}")
        (
            df_final.write
            .mode("overwrite")
            .partitionBy("processing_date")
            .parquet(OUTPUT_PATH)
        )

        count_output = df_final.count()
        logger.info(f"SUCESSO: {count_output} registros processados e salvos no HDFS.")

    except Exception as e:
        logger.error(f"FALHA CRÍTICA: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()