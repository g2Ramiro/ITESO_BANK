import json
import pydgraph


# CONSULTAS DE DETECCIÓN DE FRAUDE
def query_fraud_ring(client, device_id):
    """
    Requerimiento: Detección de Colaboración Fraudulenta / Anillos.
    (Menu Opción 4 - Monitor de Amenazas)

    Busca: Usuarios conectados a un mismo dispositivo sospechoso.
    Usa: Relación inversa ~uses_device.
    """
    query = """query fraud_ring($dev_id: string) {
      fraud_ring(func: eq(device_id, $dev_id)) {
        device_id
        device_location
        used_by: ~uses_device {
          user_id
          name
          risk_score
          is_flagged
          # Ver si también comparten IPs
          known_ips {
            ip_addr
            ip_reputation
          }
        }
      }
    }"""
    variables = {'$dev_id': device_id}

    print(f"\n--- 🕸️ Query: Anillo de Fraude en Dispositivo {device_id} ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        print(json.dumps(json.loads(res.json), indent=2, ensure_ascii=False))
    finally:
        txn.discard()


def query_money_laundering_pattern(client, min_amount):
    """
    Requerimiento: Detección de Lavado de Dinero.
    (Menu Opción 5 - Monitor de Amenazas)

    Busca: Patrones de 'Layering' donde una cuenta recibe múltiples transferencias
    grandes en poco tiempo.
    Usa: Filtros en aristas (facets) o nodos de transacción.
    """
    query = """query laundering($min_amt: float) {
      suspicious_transfers(func: type(Transaction)) @filter(ge(amount, $min_amt)) {
        tx_id
        amount
        tx_ts
        # Rastrear origen y destino
        source: from_account {
          account_id
          owner: ~owns_account { name }
        }
        target: to_account {
          account_id
          risk_score
          owner: ~owns_account { name }
        }
      }
    }"""
    # Nota: Dgraph json espera strings para variables int/float en el mapa
    variables = {'$min_amt': str(min_amount)}

    print(f"\n--- 💸 Query: Patrones de Lavado (Montos > {min_amount}) ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        print(json.dumps(json.loads(res.json), indent=2, ensure_ascii=False))
    finally:
        txn.discard()


def query_ghost_accounts(client, max_balance, min_txs):
    """
    Requerimiento: Detección de Cuentas Fantasma.
    (Menu Opción 6 - Monitor de Amenazas)

    Busca: Cuentas con saldo bajo pero mucha actividad (cuentas puente).
    Usa: Índices numéricos y conteo de aristas inversas.
    """
    query = """query ghost_acc($max_bal: float, $min_tx: int) {
      ghost_accounts(func: le(balance, $max_bal)) @filter(type(Account)) {
        account_id
        balance
        risk_score
        # Contamos transacciones entrantes y salientes
        incoming_count: count(~to_account)
        outgoing_count: count(~from_account)
      }
    }"""

    variables = {'$max_bal': str(max_balance)}

    print(f"\n--- 👻 Query: Cuentas Fantasma (Saldo < {max_balance}) ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)

        # Filtrado simple en Python para cumplir el requerimiento de 'min_txs'
        filtered = [
            acc for acc in data.get('ghost_accounts', [])
            if (acc.get('incoming_count', 0) + acc.get('outgoing_count', 0)) >= min_txs
        ]

        print(json.dumps({'ghost_accounts_filtered': filtered}, indent=2, ensure_ascii=False))
    finally:
        txn.discard()


def query_identity_theft(client):
    """
    Requerimiento: Suplantación de Identidad / Datos Compartidos.
    (Menu Opción 7 - Monitor de Amenazas)

    Busca: Documentos o teléfonos asociados a MÁS de 1 usuario (duplicidad prohibida).
    """
    query = """query id_theft {
      shared_documents(func: type(Document)) @filter(gt(count(~has_document), 1)) {
        document_id
        doc_type
        linked_users: ~has_document {
          user_id
          name
          risk_score
        }
      }
    }"""

    print(f"\n--- 🎭 Query: Suplantación de Identidad (Documentos Duplicados) ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2, ensure_ascii=False))
    finally:
        txn.discard()


def query_suspicious_path(client, start_account_id):
    """
    Requerimiento: Rastreo de rutas de dinero (Trace Flow).
    (Menu Opción 8 - Monitor de Amenazas)

    Busca: A dónde fue el dinero desde una cuenta comprometida (Recursividad).
    """
    query = """query money_trail($acc_id: string) {
      path_analysis(func: eq(account_id, $acc_id)) @recurse(depth: 3) {

        account_id
        balance
        tx_id
        amount
        is_flagged
        dgraph.type

        ~from_account
        to_account
      }
    }"""
    variables = {'$acc_id': start_account_id}

    print(f"\n--- 🕸️ Query: Ruta del Dinero desde {start_account_id} (Profundidad 3) ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        print(json.dumps(json.loads(res.json), indent=2, ensure_ascii=False))
    finally:
        txn.discard()


def query_risk_scoring(client, user_id):
    """
    Requerimiento: Scoring de riesgo basado en conexiones.
    (Menu Opción 9 - Target Individual)

    Busca: Analiza el riesgo de los vecinos (IPs, Dispositivos, Cuentas) de un usuario.
    """
    query = """query user_risk_context($uid: string) {
      risk_analysis(func: eq(user_id, $uid)) {
        user_id
        name
        my_risk: risk_score

        # 1. Riesgo por Dispositivos compartidos
        uses_device {
          device_id
          used_by_others: ~uses_device @filter(NOT eq(user_id, $uid)) {
            user_id
            name
            risk_score # Si el vecino tiene riesgo alto, yo tengo riesgo
          }
        }

        # 2. Riesgo por IPs compartidas
        known_ips {
          ip_addr
          ip_reputation # Si la reputación es mala (ej > 50)
        }
      }
    }"""
    variables = {'$uid': str(user_id)}

    print(f"\n--- ⚠️ Query: Análisis de Riesgo Contextual para User {user_id} ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)
        nodes = data.get('risk_analysis', [])
        
        if not nodes:
            return None
            
        return nodes[0]
    finally:
        txn.discard()


def query_geo_heatmap(client, lat, lon, radius_km):
    """
    Requerimiento: Mapa de calor geográfico / Ubicaciones raras.
    (Menu Opción 7 - Analítica Forense)

    Busca: Transacciones realizadas en un radio geográfico específico.
    """
    query = """query geo_tx($radius: float) {
      geo_transactions(func: near(associated_location, [%s, %s], $radius)) {
        tx_id
        amount
        associated_location
        used_device {
          device_id
        }
      }
    }""" % (lon, lat) # Inyectamos lat/lon directamente

    variables = {'$radius': str(radius_km * 1000)} 

    print(f"\n--- 🗺️ Query: Transacciones en radio de {radius_km}km de [{lat}, {lon}] ---")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        print(json.dumps(json.loads(res.json), indent=2, ensure_ascii=False))
    finally:
        txn.discard()
