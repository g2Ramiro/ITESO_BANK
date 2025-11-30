from pymongo import DESCENDING
from datetime import datetime, timedelta

# Req 5. Vistas de usuario
def get_user_financial_view(db, user_id):
    """
    Obtiene el perfil completo del usuario, sumando saldos de todas sus cuentas.
    """
    try:
        uid = int(user_id) # Conversión forzada a INT
    except ValueError:
        return None

    pipeline = [
        {"$match": {"user_id": uid}},
        {
            "$lookup": {
                "from": "accounts",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "accounts_info"
            }
        },
        {
            "$addFields": {
                "saldo_total_global": {"$sum": "$accounts_info.saldo_actual"},
                "cantidad_cuentas": {"$size": "$accounts_info"}
            }
        },
        {"$project": {
            "_id": 0,
            "id_usuario": "$user_id",
            "nombre_completo": {"$concat": ["$first_name", " ", "$last_name"]},
            "email": "$email",
            "resumen_bancario": {
                "total_en_banco": "$saldo_total_global",
                "num_productos": "$cantidad_cuentas"
            },
            "detalle_cuentas": {
                "$map": {
                    "input": "$accounts_info",
                    "as": "cuenta",
                    "in": {
                        "numero": "$$cuenta.numero_cuenta",
                        "tipo": "$$cuenta.account_type",
                        "saldo": "$$cuenta.saldo_actual",
                        "estado": "$$cuenta.estatusCuenta",
                        "alerta": "$$cuenta.flagged"
                    }
                }
            }
        }}
    ]
    
    result = list(db.users.aggregate(pipeline))
    return result[0] if result else None


# Req 6: Monitoreo de Cuentas Flageadas

def get_flagged_accounts(db):
    """
    Lista cuentas sospechosas y los datos de contacto de su dueño.
    """
    pipeline = [
        {"$match": {"flagged": True}},
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "owner"
            }
        },
        {"$unwind": "$owner"},
        {"$project": {
            "_id": 0,
            "cuenta": "$numero_cuenta",
            "tipo": "$account_type",
            "saldo": "$saldo_actual",
            "alerta": {
                "motivo": "$flagged_reason",
                "fecha": "$flagged_at",
                "estado_actual": "$estatusCuenta"
            },
            "propietario": {
                "id": "$owner.user_id",
                "nombre": {"$concat": ["$owner.first_name", " ", "$owner.last_name"]},
                "email": "$owner.email"
            }
        }},
        {"$sort": {"alerta.fecha": -1}}
    ]
    return list(db.accounts.aggregate(pipeline))

# Req 7: Cuentas con cambios frecuentes de estatus 
def get_erratic_accounts(db, min_changes=1):
    """
    Detecta inestabilidad en cuentas basada en su historial de estados.
    """
    pipeline = [
        # Filtrar cuentas que tengan historial
        {"$match": {"status_history": {"$exists": True, "$not": {"$size": 0}}}},
        {"$project": {
            "_id": 0,
            "cuenta": "$numero_cuenta",
            "total_cambios": {"$size": "$status_history"},
            "historial_cambios": {
                "$map": {
                    "input": "$status_history",
                    "as": "cambio",
                    "in": {
                        "de": "$$cambio.old_status",
                        "a": "$$cambio.new_status",
                        "razon": "$$cambio.reason",
                        "fecha": "$$cambio.changed_at"
                    }
                }
            }
        }},
        {"$match": {"total_cambios": {"$gt": min_changes}}},
        {"$sort": {"total_cambios": -1}}
    ]
    return list(db.accounts.aggregate(pipeline))


# Req 8: Dispositivos por Usuario
def get_user_devices(db, user_id):
    """
    Extrae la huella digital (Dispositivos e IPs) de los logins.
    """
    try:
        uid = int(user_id) # Conversión forzada a INT
    except ValueError:
        return None

    pipeline = [
        {"$match": {"user_id": uid}},
        {"$unwind": "$logins"},
        {"$group": {
            "_id": "$user_id",
            "nombre": {"$first": "$first_name"},
            "apellido": {"$first": "$last_name"},
            "lista_dispositivos": {"$addToSet": "$logins.device"},
            "lista_ips": {"$addToSet": "$logins.ip"},
            "conteo_accesos": {"$sum": 1}
        }},
        {"$project": {
            # Usamos $toString para evitar el error de concatenar Int con String
            "usuario": {
                "$concat": [
                    {"$toString": "$_id"}, " - ", 
                    "$nombre", " ", "$apellido"
                ]
            },
            "resumen_seguridad": {
                "total_dispositivos_unicos": {"$size": "$lista_dispositivos"},
                "dispositivos": "$lista_dispositivos",
                "ips_usadas": "$lista_ips"
            }
        }}
    ]
    result = list(db.users.aggregate(pipeline))
    return result[0] if result else None


#Req 9: Busqueda flexible de cuentas
def find_users_by_name(db, name_input):
    """
    Busca usuarios por nombre o apellido (case insensitive).
    Retorna una lista de candidatos con su ID.
    """
    # Creamos un patrón regex para buscar coincidencias parciales
    # 'i' significa case-insensitive (ignora mayúsculas)
    regex_pattern = {"$regex": name_input, "$options": "i"}
    
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"first_name": regex_pattern},
                    {"last_name": regex_pattern}
                ]
            }
        },
        {
            "$project": {
                "_id": 0,
                "user_id": 1,
                "nombre_completo": {"$concat": ["$first_name", " ", "$last_name"]},
                "email": 1
            }
        },
        {"$limit": 5} # Limitamos a 5 para no llenar la pantalla
    ]
    
    return list(db.users.aggregate(pipeline))

# Req 10: Cuentas Nuevas de Alto Riesgo
def get_high_risk_new_accounts(db, days_threshold=1000, amount_threshold=1000):
    """
    Cruza cuentas recientes con transacciones de alto valor.
    """
    # Nota: days_threshold alto para capturar datos de ejemplo antiguos
    date_limit = datetime.utcnow() - timedelta(days=days_threshold)
    
    pipeline = [
        # 1. Filtrar cuentas nuevas
        {"$match": {"fecha_creacion": {"$gte": date_limit}}},
        
        # 2. Buscar transacciones grandes asociadas
        # Nota: account_id (string) vs flow.account_origen (string)
        {
            "$lookup": {
                "from": "transactions_meta",
                "let": {"acc_id": "$account_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$flow.account_origen", "$$acc_id"]},
                                    {"$gte": ["$amount_details.total", amount_threshold]}
                                ]
                            }
                        }
                    },
                    { 
                        "$project": { 
                            "_id": 0, 
                            "tx_id": "$transaction_id", 
                            "monto": "$amount_details.total", 
                            "destino": "$flow.account_destino",
                            "fecha": "$timestamp"
                        } 
                    }
                ],
                "as": "transacciones_sospechosas"
            }
        },
        # 3. Filtrar las que tengan coincidencias
        {"$match": {"transacciones_sospechosas.0": {"$exists": True}}},
        {"$project": {
            "_id": 0,
            "cuenta_riesgo": "$numero_cuenta",
            "fecha_apertura": "$fecha_creacion",
            "saldo_actual": "$saldo_actual",
            "alerta": {
                "total_txs_grandes": {"$size": "$transacciones_sospechosas"},
                "detalle_txs": "$transacciones_sospechosas"
            }
        }}
    ]
    return list(db.accounts.aggregate(pipeline))

# Req 11: Detección Global de Cambio IP/Dispositivo
def detect_suspicious_ip_changes(db):
    """
    Detecta IPs compartidas por múltiples usuarios (posible Botnet).
    """
    pipeline = [
        {"$group": {
            "_id": "$digital_fingerprint.ip",
            "usuarios_afectados": {"$addToSet": "$user_id"},
            "total_operaciones": {"$sum": 1},
            "ubicaciones_detectadas": {"$addToSet": "$digital_fingerprint.location_approx"}
        }},
        # Filtro: Más de 1 usuario distinto usando la misma IP
        {"$match": {
            "$expr": {"$gt": [{"$size": "$usuarios_afectados"}, 1]}
        }},
        {"$project": {
            "ip_sospechosa": "$_id",
            "_id": 0,
            "analisis": {
                "usuarios_involucrados": "$usuarios_afectados",
                "volumen_txs": "$total_operaciones",
                "geolocalizacion": "$ubicaciones_detectadas"
            }
        }},
        {"$sort": {"analisis.volumen_txs": -1}}
    ]
    return list(db.transactions_meta.aggregate(pipeline))

# Req 12: Score de Riesgo 
def calculate_risk_score(db, user_id):
    """
    Calcula un puntaje de riesgo (0-100) basado en reglas de negocio.
    """
    try:
        uid = int(user_id) # Conversión forzada a INT
    except ValueError:
        return None

    # 1. Obtener datos de cuentas (Flagged)
    # Buscamos cuántas cuentas tiene marcadas
    flagged_count = db.accounts.count_documents({"user_id": uid, "flagged": True})
    
    # 2. Obtener dispositivos (reutilizando query simple para no depender de func externa)
    dev_pipeline = [
        {"$match": {"user_id": uid}},
        {"$unwind": "$logins"},
        {"$group": {
            "_id": "$user_id",
            "unique_devices": {"$addToSet": "$logins.device"},
            "unique_ips": {"$addToSet": "$logins.ip"}
        }}
    ]
    dev_res = list(db.users.aggregate(dev_pipeline))
    
    unique_dev_count = 0
    devs_list = []
    ips_list = []
    
    if dev_res:
        unique_dev_count = len(dev_res[0].get("unique_devices", []))
        devs_list = dev_res[0].get("unique_devices", [])
        ips_list = dev_res[0].get("unique_ips", [])
    
    # 3. CÁLCULO DEL SCORE (Lógica Python equivalente a lo probado en Mongosh)
    score = 0
    reasons = []

    # Regla A: Cuentas castigadas
    if flagged_count > 0:
        pts = 40 * flagged_count
        score += pts
        reasons.append(f"🚩 +{pts} pts: Tiene {flagged_count} cuenta(s) marcada(s).")

    # Regla B: Granja de dispositivos
    if unique_dev_count >= 3:
        pts = 20
        score += pts
        reasons.append(f"📱 +{pts} pts: Uso excesivo de dispositivos ({unique_dev_count}).")
        
    # Regla C: Software Sospechoso (Keywords)
    # Convertimos a string mayúsculas para buscar fácil
    devs_str = str(devs_list).upper()
    ips_str = str(ips_list).upper()
    
    if "TOR" in devs_str or "VPN" in devs_str or "BOT" in devs_str or "10.0.0" in ips_str: 
        pts = 25
        score += pts
        reasons.append(f"🕵️ +{pts} pts: ALERTA TÉCNICA - Uso de TOR, VPN o Bots detectado.")

    return {
        "user_id": uid,
        "risk_score": min(score, 100),
        "risk_level": "CRITICO 🔴" if score > 70 else ("ALTO 🟠" if score > 40 else "BAJO 🟢"),
        "factors": reasons
    }