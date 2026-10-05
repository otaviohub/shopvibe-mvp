import os
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import redis

app = FastAPI(
    title="ShopVibe Real-Time Data API",
    description="API de Vendas ao Vivo, Consulta de Pedidos (Bronze) e Alertas de Fraude (Gold)",
    version="1.0.0"
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

mongo_client = MongoClient(MONGO_URI)
bronze_collection = mongo_client["shopvibe"]["raw_orders"]
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

@app.get("/vendas/hoje", summary="RF03 & RF04 - Painel de Vendas do Dia em Tempo Real")
def get_vendas_hoje():
    today = datetime.now().strftime("%Y-%m-%d")
    count = int(r.get(f"sales:{today}:count") or 0)
    revenue = float(r.get(f"sales:{today}:revenue") or 0.0)
    
    return {
        "data": today,
        "quantidade_pedidos": count,
        "faturamento_total_brl": round(revenue, 2),
        "ticket_medio_brl": round(revenue / count, 2) if count > 0 else 0.0,
        "camada": "Gold (Redis In-Memory)"
    }

@app.get("/pedidos/{order_id}", summary="RF02 & RF04 - Consulta de Pedido e Evento Bruto")
def get_pedido(order_id: int):
    doc = bronze_collection.find_one({"_id": order_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Pedido não encontrado na camada Bronze")
    return doc

@app.get("/alertas/fraude", summary="RF05 (Diferencial) - Alertas de Fraude (>5 pedidos em 2 min)")
def get_alertas_fraude():
    raw_alerts = r.lrange("fraud:alerts", 0, 19)
    alerts = [json.loads(a) for a in raw_alerts]
    return {
        "total_alertas_recentes": len(alerts),
        "alertas": alerts
    }