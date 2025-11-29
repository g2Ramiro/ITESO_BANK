import json
import os
from datetime import datetime
from pymongo import ASCENDING, DESCENDING

# --- Helpers ---

def parse_mongo_date(value):
    if not value:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    
    # Limpieza básica
    v = value.strip().replace("Z", "")
    
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        try:
            return datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            return None

# --- Funciones Internas de Carga ---

def _load_users(db, filepath):
    collection = db["users"]
    
    if not os.path.exists(filepath):
        print(f"⚠️ Archivo no encontrado: {filepath}")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        users_data = json.load(f)
    
    processed_docs = []
    for doc in users_data:
        if "fecha_nacimiento" in doc:
            doc["fecha_nacimiento"] = parse_mongo_date(doc["fecha_nacimiento"])
        if "fecha_creacion" in doc:
            doc["fecha_creacion"] = parse_mongo_date(doc["fecha_creacion"])
            
        if "logins" in doc and isinstance(doc["logins"], list):
            for login in doc["logins"]:
                # Corrección: El JSON suele traer 'ts', tu código buscaba 'timestamp'
                if "ts" in login: 
                    login["ts"] = parse_mongo_date(login["ts"])
                elif "timestamp" in login:
                    login["timestamp"] = parse_mongo_date(login["timestamp"])
                    
        processed_docs.append(doc)

    if processed_docs:
        collection.insert_many(processed_docs)
        print(f"✅ Insertados {len(processed_docs)} documentos en 'users'.")
    
    # Índices [cite: 346, 348]
    collection.create_index([("user_id", ASCENDING)], unique=True)
    collection.create_index([("email", ASCENDING)], unique=True)
    collection.create_index([("logins.ip", ASCENDING)])
    print("   Índices de 'users' creados.")

def _load_accounts(db, filepath):
    collection = db["accounts"]
    
    if not os.path.exists(filepath):
        print(f"⚠️ Archivo no encontrado: {filepath}")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        accounts_data = json.load(f)
        
    processed_docs = []
    for doc in accounts_data:
        if "fecha_creacion" in doc:
            doc["fecha_creacion"] = parse_mongo_date(doc["fecha_creacion"])
        if "fecha_actualizacion" in doc:
            doc["fecha_actualizacion"] = parse_mongo_date(doc["fecha_actualizacion"])
        if "flagged_at" in doc:
            doc["flagged_at"] = parse_mongo_date(doc["flagged_at"])
            
        if "status_history" in doc and isinstance(doc["status_history"], list):
            for status in doc["status_history"]:
                if "changed_at" in status:
                    status["changed_at"] = parse_mongo_date(status["changed_at"])
        
        if "saldo_actual" in doc:
            doc["saldo_actual"] = float(doc["saldo_actual"])

        processed_docs.append(doc)

    if processed_docs:
        collection.insert_many(processed_docs)
        print(f"✅ Insertados {len(processed_docs)} documentos en 'accounts'.")

    # Índices [cite: 379, 380]
    collection.create_index([("account_id", ASCENDING)], unique=True)
    collection.create_index([("user_id", ASCENDING)])
    collection.create_index([("numero_cuenta", ASCENDING)], unique=True)
    print("   Índices de 'accounts' creados.")

def _load_transactions(db, filepath):
    collection = db["transactions_meta"] 
    
    if not os.path.exists(filepath):
        print(f"⚠️ Archivo no encontrado: {filepath}")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        tx_data = json.load(f)
        
    processed_docs = []
    for doc in tx_data:
        if "timestamp" in doc:
            doc["timestamp"] = parse_mongo_date(doc["timestamp"])
            
        if "amount_details" in doc and "total" in doc["amount_details"]:
            doc["amount_details"]["total"] = float(doc["amount_details"]["total"])
            
        processed_docs.append(doc)
        
    if processed_docs:
        collection.insert_many(processed_docs)
        print(f"✅ Insertados {len(processed_docs)} documentos en 'transactions_meta'.")
    
    # Índices [cite: 415, 419]
    collection.create_index([("transaction_id", ASCENDING)], unique=True)
    collection.create_index([("timestamp", DESCENDING)]) 
    collection.create_index([("flow.account_origen", ASCENDING)])
    collection.create_index([("flow.account_destino", ASCENDING)])
    collection.create_index([("digital_fingerprint.ip", ASCENDING)])
    collection.create_index([("digital_fingerprint.device_model", ASCENDING)])
    collection.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])

    print("   Índices de 'transactions_meta' creados.")

# --- FUNCIÓN PRINCIPAL EXPORTABLE ---

def populate_database(db, data_dir="data/mongo"):
    print(f"📂 Buscando archivos en: {data_dir}")
    
    # 1. Limpieza (Idempotencia)
    print("🧹 Limpiando colecciones existentes...")
    db.users.drop()
    db.accounts.drop()
    db.transactions_meta.drop()
    
    # 2. Carga
    # Usamos os.path.join para evitar errores de rutas en Windows/Mac/Linux
    _load_users(db, os.path.join(data_dir, "users.json"))
    _load_accounts(db, os.path.join(data_dir, "accounts.json"))
    _load_transactions(db, os.path.join(data_dir, "transactions_meta.json"))
    
    print("\n✨ Población de MongoDB finalizada.")