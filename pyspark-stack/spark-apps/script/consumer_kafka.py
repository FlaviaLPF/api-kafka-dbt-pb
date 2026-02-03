import json
import logging
import os
from kafka import KafkaConsumer
from datetime import datetime

# =========================
# CONFIGURAÇÕES
# =========================
TOPIC = "flights_sp"
LOG_FILE = "/opt/spark-apps/logs/flights_consumer.log"
STAGING_PATH = "/opt/spark-apps/staging"

# Cria as pastas necessárias se não existirem
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(STAGING_PATH, exist_ok=True)

# Configuração do Logger
logger = logging.getLogger("flights_consumer")
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

logging.getLogger("kafka").setLevel(logging.WARNING)

def consume_flights(**context):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["broker:29092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True, 
        group_id="flights-consumer-test2",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=5000
    )
    
    logger.info(f"Iniciando consumo do tópico: {TOPIC}")
    messages_list = [] 
    count = 0
    
    try:
        # Loop para ler mensagens do Kafka
        for i, message in enumerate(consumer):
            try:
                # Mantemos o log para você conferir se os dados bateram
                logger.info(f"Mensagem recebida #{i+1}: {message.value}")
                messages_list.append(message.value) 
                count += 1
            except Exception as e:
                logger.error(f"Erro ao processar mensagem #{i+1}: {e}")
                raise
        
        # --- SALVANDO O ARQUIVO JSON (O SEU SEGURO) ---
        if count > 0:
            # Pega o 'ts_nodash' do Airflow (ex: 20260117T180000)
            # Se rodar manual no terminal, ele gera um timestamp atual
            ts = context.get('ts_nodash') or datetime.now().strftime('%Y%m%dT%H%M%S')
            
            file_name = f"flights_{ts}.json"
            full_path = os.path.join(STAGING_PATH, file_name)
            
            # Grava a lista de dicionários como um arquivo JSON único
            with open(full_path, 'w') as f:
                json.dump(messages_list, f)
            
            logger.info(f"ARQUIVO GERADO: {full_path}")
            logger.info(f"Total de {count} mensagens salvas com sucesso.")
        else:
            logger.info("Nenhuma mensagem nova no Kafka nesta rodada.")

    except Exception as e:
        logger.critical(f"Erro crítico no consumidor: {e}")
        raise
    finally:
        consumer.close()

    # O count vai para o XCom para o Branch decidir se chama o Spark
    return count

if __name__ == "__main__":
    # Permite rodar 'python consumer_kafka.py' para teste manual
    print(f"Resultado da execução manual: {consume_flights(**{})}")