import os
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import redis

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

# Conexões com MongoDB (Bronze) e Redis (Gold)
mongo_client = MongoClient(MONGO_URI)
bronze_collection = mongo_client["shopvibe"]["raw_orders"]
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def process_order_event(raw_msg: str):
    event = json.loads(raw_msg)
    
    # Debezium sem schema envia os dados diretamente ou dentro de "payload"
    payload = event.get("payload", event)
    if not payload:
        return

    op = payload.get("op") # 'c' = create (INSERT), 'r' = read (snapshot inicial), 'u' = update
    after = payload.get("after")

    if op in ("c", "r", "u") and after:
        order_id = after["order_id"]
        customer_id = after["customer_id"]
        total = float(after["total"])
        status = after["status"]
        
        # 1. CAMADA BRONZE (RF02): Salva o evento bruto completo no MongoDB para reprocessamento
        bronze_doc = {
            "_id": order_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "total": total,
            "status": status,
            "raw_cdc_event": event,
            "ingested_at": datetime.utcnow().isoformat()
        }
        bronze_collection.replace_one({"_id": order_id}, bronze_doc, upsert=True)

        # Processa métricas e antifraude apenas para novos pedidos (evita duplicar caso haja reentrega)
        today = datetime.now().strftime("%Y-%m-%d")
        processed_set_key = f"sales:{today}:processed_orders"
        
        # SADD retorna 1 se o pedido ainda não havia sido contabilizado hoje (Idempotência)
        if r.sadd(processed_set_key, order_id) == 1:
            # 2. CAMADA GOLD (RF03): Contadores em tempo real no Redis
            r.incr(f"sales:{today}:count")
            r.incrbyfloat(f"sales:{today}:revenue", total)

            # 3. DIFERENCIAL ANTIFRAUDE (RF05): Mais de 5 pedidos em 2 minutos (120 segundos)
            fraud_key = f"fraud:window:{customer_id}"
            tx_count = r.incr(fraud_key)
            if tx_count == 1:
                r.expire(fraud_key, 120) # Abre janela de 120s no primeiro pedido

            if tx_count > 5:
                alert = {
                    "customer_id": customer_id,
                    "order_id": order_id,
                    "orders_in_2min": tx_count,
                    "last_order_total": total,
                    "detected_at": datetime.now().isoformat(),
                    "rule": "RF05: Mais de 5 pedidos em 2 minutos"
                }
                r.lpush("fraud:alerts", json.dumps(alert))
                print(f"🚨 [ALERTA DE FRAUDE] Cliente {customer_id} fez {tx_count} pedidos em <2 min! (Pedido #{order_id})")

        print(f"✅ [PROCESSADO] Pedido #{order_id} | Cliente: {customer_id} | Valor: R$ {total:.2f}")

def main():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "shopvibe-realtime-cg",
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(conf)
    # Assina o tópico criado pelo Debezium para a tabela orders
    consumer.subscribe(["shopvibe.public.orders"])

    print("🎧 Consumidor ShopVibe iniciado! Aguardando eventos em 'shopvibe.public.orders'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"⚠️ Aguardando tópico/broker: {msg.error()}")
                time.sleep(2)
                continue

            if msg.value():
                process_order_event(msg.value().decode("utf-8"))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()