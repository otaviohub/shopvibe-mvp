import os
import sys
import time
import random
import psycopg2

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": 5432,
    "dbname": "shopvibe_db",
    "user": "shopvibe",
    "password": "shopvibe_password"
}

def insert_order(conn, customer_id: str, total: float):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer_id, total, status) VALUES (%s, %s, 'APPROVED') RETURNING order_id;",
            (customer_id, total)
        )
        order_id = cur.fetchone()[0]
        conn.commit()
        print(f"🛒 [POSTGRES] Pedido #{order_id} gravado | Cliente: {customer_id} | R$ {total:.2f}")

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"
    conn = psycopg2.connect(**DB_CONFIG)

    if mode == "fraude":
        print("🕵️‍♂️ [SIMULAÇÃO DE ATAQUE] Disparando 7 compras seguidas do mesmo cliente em poucos segundos...")
        suspect = "CUST-FRAUDE-999"
        for _ in range(7):
            valor = round(random.uniform(450.0, 1500.0), 2)
            insert_order(conn, suspect, valor)
            time.sleep(0.4)
        print("✅ Ataque simulado! Verifique os logs do consumer ou GET /alertas/fraude.")
    else:
        print("🛍️ [SIMULAÇÃO NORMAL] Gerando 5 pedidos de clientes variados...")
        for _ in range(5):
            cliente = f"CUST-{random.randint(100, 500)}"
            valor = round(random.uniform(59.90, 399.90), 2)
            insert_order(conn, cliente, valor)
            time.sleep(1.0)

    conn.close()

if __name__ == "__main__":
    main()