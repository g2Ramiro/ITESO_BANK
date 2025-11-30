import json
import pydgraph

# UTILERÍAS DE VISUALIZACIÓN

def print_header(title):
    print(f"\n{'='*60}")
    print(f"🕵️  REPORTE: {title.upper()}")
    print(f"{'='*60}")

def print_separator():
    print(f"{'-'*60}")

# CONSULTAS DE DETECCIÓN DE FRAUDE

def query_fraud_ring(client, device_id):
    """
    Requerimiento: Detección de Colaboración Fraudulenta / Anillos.
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
          known_ips { ip_addr }
        }
      }
    }"""
    variables = {'$dev_id': device_id}

    print_header(f"ANILLO DE FRAUDE (Dispositivo: {device_id})")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json).get('fraud_ring', [])

        if not data:
            print("✅ No se encontró el dispositivo o no tiene usuarios asociados.")
            return

        device = data[0]
        users = device.get('used_by', [])

        print(f"📍 Ubicación Disp: {device.get('device_location', 'N/A')}")
        print(f"👥 Usuarios compartiendo este dispositivo: {len(users)}")
        print_separator()
        print(f"{'USER ID':<10} | {'NOMBRE':<20} | {'RISK':<5} | {'FLAG':<5} | {'IPS CONOCIDAS'}")
        print_separator()

        for u in users:
            ips = ", ".join([ip['ip_addr'] for ip in u.get('known_ips', [])])
            flag = "🚩" if u.get('is_flagged') else "OK"
            print(f"{u['user_id']:<10} | {u['name']:<20} | {u['risk_score']:<5} | {flag:<5} | {ips}")

    finally:
        txn.discard()


def query_money_laundering_pattern(client, min_amount):
    """
    Requerimiento: Detección de Lavado de Dinero.
    """
    query = """query laundering($min_amt: float) {
      suspicious_transfers(func: type(Transaction)) @filter(ge(amount, $min_amt)) {
        tx_id
        amount
        tx_ts
        source: from_account { account_id, owner: ~owns_account { name } }
        target: to_account { account_id, owner: ~owns_account { name } }
      }
    }"""
    variables = {'$min_amt': str(min_amount)}

    print_header(f"POSIBLE LAVADO (Montos >= ${min_amount})")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        txs = json.loads(res.json).get('suspicious_transfers', [])

        if not txs:
            print("✅ No se detectaron transacciones sospechosas por ese monto.")
            return

        print(f"{'FECHA':<12} | {'TX ID':<10} | {'MONTO ($)':<12} | {'ORIGEN (Cuenta/Dueño)':<30} | {'DESTINO (Cuenta/Dueño)'}")
        print_separator()

        for tx in txs:

            src_acc = tx.get('source', [{}])[0]
            tgt_acc = tx.get('target', [{}])[0]

            src_str = f"{src_acc.get('account_id', 'EXT')} ({src_acc.get('owner', [{'name':'Unknown'}])[0]['name']})"
            tgt_str = f"{tgt_acc.get('account_id', 'EXT')} ({tgt_acc.get('owner', [{'name':'Unknown'}])[0]['name']})"

            print(f"{tx['tx_ts'][:10]:<12} | {tx['tx_id']:<10} | ${tx['amount']:<11.2f} | {src_str[:29]:<30} | {tgt_str}")

    finally:
        txn.discard()


def query_ghost_accounts(client, max_balance, min_txs):
    """
    Requerimiento: Detección de Cuentas Fantasma.
    """
    query = """query ghost_acc($max_bal: float) {
      ghost_accounts(func: le(balance, $max_bal)) @filter(type(Account)) {
        account_id
        balance
        risk_score
        incoming_count: count(~to_account)
        outgoing_count: count(~from_account)
      }
    }"""
    variables = {'$max_bal': str(max_balance)}

    print_header(f"CUENTAS FANTASMA (Saldo < ${max_balance}, Txs >= {min_txs})")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json).get('ghost_accounts', [])

        # Filtrado lógico
        filtered = [acc for acc in data if (acc.get('incoming_count', 0) + acc.get('outgoing_count', 0)) >= min_txs]

        if not filtered:
            print("✅ Ninguna cuenta cumple con el criterio de 'Fantasma'.")
            return

        print(f"{'CUENTA':<15} | {'SALDO':<10} | {'RISK':<5} | {'ENTRADAS':<8} | {'SALIDAS':<8} | {'TOTAL FLUJO'}")
        print_separator()

        for acc in filtered:
            total = acc['incoming_count'] + acc['outgoing_count']
            print(f"{acc['account_id']:<15} | ${acc['balance']:<9.2f} | {acc['risk_score']:<5} | {acc['incoming_count']:<8} | {acc['outgoing_count']:<8} | {total}")

    finally:
        txn.discard()


def query_identity_theft(client):
    """
    Requerimiento: Suplantación de Identidad.
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

    print_header("SUPLANTACIÓN DE IDENTIDAD (Docs Duplicados)")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        docs = json.loads(res.json).get('shared_documents', [])

        if not docs:
            print("✅ Integridad de documentos correcta. No hay duplicados.")
            return

        for doc in docs:
            print(f"📄 DOCUMENTO COMPROMETIDO: {doc['document_id']} ({doc.get('doc_type','Unknown')})")
            print("   ⚠️  Usuarios vinculados:")
            for u in doc['linked_users']:
                print(f"      - ID: {u['user_id']} | Nombre: {u['name']} | Risk: {u['risk_score']}")
            print_separator()

    finally:
        txn.discard()


def query_suspicious_path(client, start_account_id):
    """
    Requerimiento: Rastreo de rutas de dinero
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

    print_header(f"RASTREO DE RUTA (TRACE FLOW) - Origen: {start_account_id}")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json).get('path_analysis', [])

        if not data:
            print("❌ Cuenta no encontrada o sin conexiones.")
            return

        def print_node(node, level=0, prefix=""):
            indent = "    " * level
            dtype = node.get('dgraph.type', [])
            if isinstance(dtype, list): dtype = dtype[0] if dtype else "Unknown"

            if dtype == 'Account':
                flag = "🚩" if node.get('is_flagged') else ""
                print(f"{indent}🏦 [{node.get('account_id')}] Saldo: ${node.get('balance',0)} {flag}")

                if 'to_account' in node: # Si fuera transacción
                    pass

            elif dtype == 'Transaction':
                print(f"{indent}💸 TX: {node.get('tx_id')} | Monto: ${node.get('amount')} {prefix}")
            for key, val in node.items():
                if isinstance(val, list) and isinstance(val[0], dict):
                    # Es una relación
                    for child in val:
                        arrow = "  ⬇️  " if key == 'to_account' else "  ⬆️  " # Simplificación
                        print_node(child, level + 1, arrow)

        # Imprimir raíz
        print_node(data[0])
        print("\n(Nota: La indentación muestra el flujo de pasos encontrados)")

    finally:
        txn.discard()


def query_risk_scoring(client, user_id):
    """
    Requerimiento: Scoring de riesgo basado en conexiones.
    """
    query = """query user_risk_context($uid: string) {
      risk_analysis(func: eq(user_id, $uid)) {
        user_id
        name
        my_risk: risk_score

        uses_device {
          device_id
          used_by_others: ~uses_device @filter(NOT eq(user_id, $uid)) {
            user_id
            name
            risk_score
          }
        }

        known_ips {
          ip_addr
          ip_reputation
        }
      }
    }"""
    variables = {'$uid': str(user_id)}

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json).get('risk_analysis', [])

        if not data:
            print("❌ Usuario no encontrado.")
            return None

        user = data[0]

        print_header(f"SCORING DE RIESGO: {user['name']} (ID: {user['user_id']})")
        print(f"🔥 RIESGO PROPIO: {user.get('my_risk', 0)}/100")
        print_separator()

        print("📱 ANÁLISIS DE DISPOSITIVOS:")
        devices = user.get('uses_device', [])
        if not devices: print("   (Sin dispositivos registrados)")

        for d in devices:
            others = d.get('used_by_others', [])
            print(f"   ► Disp ID: {d['device_id']}")
            if others:
                print("      ⚠️  USADO TAMBIÉN POR:")
                for o in others:
                    print(f"         - {o['name']} (Risk: {o['risk_score']})")
            else:
                print("      ✅ Uso exclusivo (Limpio)")

        print_separator()
        # IPs
        print("🌐 ANÁLISIS DE IPs:")
        ips = user.get('known_ips', [])
        if not ips: print("   (Sin IPs registradas)")

        for ip in ips:
            rep = ip.get('ip_reputation', 0)
            status = "PELIGROSA ⛔" if rep > 70 else "SOSPECHOSA ⚠️" if rep > 40 else "SEGURA ✅"
            print(f"   ► {ip['ip_addr']:<15} | Reputación: {rep:<3} | {status}")

        return user

    finally:
        txn.discard()


def query_geo_heatmap(client, lat, lon, radius_km):
    """
    Requerimiento: Mapa de calor geográfico
    """
    query = """query geo_tx($radius: float) {
      geo_transactions(func: near(associated_location, [%s, %s], $radius)) {
        tx_id
        amount
        associated_location
        used_device { device_id }
      }
    }""" % (lon, lat)
    variables = {'$radius': str(radius_km * 1000)}

    print_header(f"MAPA DE CALOR GEO (Radio: {radius_km}km)")
    print(f"📍 Centro: [{lat}, {lon}]")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, variables=variables)
        txs = json.loads(res.json).get('geo_transactions', [])

        if not txs:
            print("✅ No se encontraron transacciones en esta zona.")
            return

        print_separator()
        print(f"🔍 Se encontraron {len(txs)} transacciones:")
        print_separator()
        print(f"{'TX ID':<10} | {'MONTO':<10} | {'DISPOSITIVO':<20} | {'COORDENADAS'}")

        for tx in txs:
            dev = tx.get('used_device', [{'device_id': 'Unknown'}])[0]['device_id']
            coords = tx.get('associated_location', {}).get('coordinates', 'N/A')
            print(f"{tx['tx_id']:<10} | ${tx['amount']:<9.2f} | {dev:<20} | {coords}")

    finally:
        txn.discard()
