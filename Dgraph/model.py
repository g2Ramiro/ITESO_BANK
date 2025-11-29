import csv
import json
import sys
import os
import pydgraph

#Funcion para cargar schema a dgraph
def create_schema(client):
    schema = """
    # 1. Identificadores e Índices
    user_id: string @index(hash) .
    account_id: string @index(hash) .
    device_id: string @index(hash) .
    ip_addr: string @index(hash) .
    document_id: string @index(hash) .
    tx_id: string @index(hash) .

    # 2. Datos Numéricos y Fechas
    risk_score: float @index(float) .
    balance: float @index(float) .
    amount: float @index(float) .
    tx_ts: datetime @index(hour) .
    ip_reputation: int @index(int) .

    # 3. Datos Geográficos
    device_location: geo @index(geo) .
    associated_location: geo @index(geo) .
    ip_location: geo @index(geo) .

    # 4. Flags y Textos
    name: string @index(term) .
    is_flagged: bool @index(bool) .
    email: string .
    phone: string .
    tx_type: string .
    doc_type: string .
    address_hash: string @index(hash) .

    # 6. Relaciones (edges)
    owns_account: [uid] @reverse .
    has_document: [uid] @reverse .
    uses_device: [uid] @reverse .
    known_ips: [uid] @reverse .
    has_ip: [uid] @reverse .
    from_account: [uid] @reverse .
    to_account: [uid] @reverse .
    used_device: [uid] @reverse .
    used_ip: [uid] @reverse .

    # 5. Definición de Tipos
    type User {
        user_id
        name
        email
        phone
        address_hash
        risk_score
        is_flagged
        owns_account
        has_document
        uses_device
        known_ips
    }

    type Account {
        account_id
        balance
        risk_score
        is_flagged
    }

    type Device {
        device_id
        device_location
        has_ip
    }

    type IpAddress {
        ip_addr
        ip_reputation
        ip_location
    }

    type Transaction {
        tx_id
        amount
        tx_ts
        tx_type
        associated_location
        from_account
        to_account
        used_device
        used_ip
    }

    type Document {
        document_id
        doc_type
    }
    """
    client.alter(pydgraph.Operation(schema=schema))


# Detecta la ruta donde está este archivo (Dgraph/model.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Sube un nivel y entra a data/dgraph
DATA_PATH = os.path.join(BASE_DIR, '..', 'data', 'dgraph')

# Definición de rutas a los archivos CSV
FILES = {
    # Nodos
    'users':    os.path.join(DATA_PATH, 'nodes_users.csv'),
    'accounts': os.path.join(DATA_PATH, 'nodes_accounts.csv'),
    'devices':  os.path.join(DATA_PATH, 'nodes_devices.csv'),
    'ips':      os.path.join(DATA_PATH, 'nodes_ips.csv'),
    'docs':     os.path.join(DATA_PATH, 'nodes_documents.csv'),
    'txs':      os.path.join(DATA_PATH, 'nodes_transactions.csv'),

    # Relaciones
    'rel_acc':    os.path.join(DATA_PATH, 'edges_users_accounts.csv'),
    'rel_dev':    os.path.join(DATA_PATH, 'edges_users_devices.csv'),
    'rel_ip':     os.path.join(DATA_PATH, 'edges_users_ips.csv'),
    'rel_doc':    os.path.join(DATA_PATH, 'edges_users_documents.csv'),
    'rel_dev_ip': os.path.join(DATA_PATH, 'edges_devices_ips.csv'),
    'rel_tx':     os.path.join(DATA_PATH, 'edges_transactions_flow.csv')
}

#Funciones para leer los csv correctamente

def get_bnode_key(row_id):
    """Limpia el ID para usarlo como clave interna durante la carga"""
    return str(row_id).strip()

def safe_read_csv(file_path):
    """Lee CSV manejando caracteres BOM (utf-8-sig) y errores de ruta"""
    if not os.path.exists(file_path):
        print(f"❌ ERROR: No se encontró el archivo: {file_path}")
        print(f"   (Buscando en: {os.path.dirname(file_path)})")
        return [], None

    f = open(file_path, 'r', encoding='utf-8-sig')
    return csv.DictReader(f), f

#Funcionas para hacer la transformacion de csv a json y que lo reciba dgraph en la mutacion

def tf_user(row):
    return {
        'user_id': row['user_id'],
        'name': row['name'],
        'email': row['email'],
        'phone': row['phone'],
        'address_hash': row['address_hash'],
        'risk_score': float(row['risk_score']),
        'is_flagged': str(row['is_flagged']).lower() == 'true'
    }

def tf_account(row):
    return {
        'account_id': row['account_id'],
        'balance': float(row['balance']),
        'risk_score': float(row['risk_score']),
        'is_flagged': str(row['is_flagged']).lower() == 'true'
    }

def tf_device(row):
    loc = row.get('device_location')
    if loc and loc.startswith('{'):
        try: loc = json.loads(loc)
        except: pass
    return {
        'device_id': row['device_id'],
        'device_location': loc
    }

def tf_ip(row):
    return {
        'ip_addr': row['ip_addr'],
        'ip_reputation': int(row['ip_reputation'])
    }

def tf_doc(row):
    return {
        'document_id': row['document_id'],
        'doc_type': row['doc_type']
    }

def tf_tx(row):
    loc = row.get('associated_location')
    if loc and loc.startswith('{'):
        try: loc = json.loads(loc)
        except: pass
    return {
        'tx_id': row['tx_id'],
        'amount': float(row['amount']),
        'tx_ts': row['tx_ts'],
        'tx_type': row.get('type') or row.get('tx_type') or 'unknown',
        'associated_location': loc
    }

#Funciones de carga de nodos y aristas

def load_generic_nodes(client, file_path, node_type, id_col, transform_func):
    """Carga un CSV de nodos y devuelve un diccionario {ID_CSV: UID_DGRAPH}"""
    print(f"📂 Cargando {node_type}s desde {os.path.basename(file_path)}...")
    txn = client.txn()
    uids_map = {}

    reader, f = safe_read_csv(file_path)
    if f is None: return {}

    try:
        batch = []
        for row in reader:
            try:
                row_id = row[id_col]
                bnode = '_:' + get_bnode_key(row_id)

                obj = transform_func(row)
                obj['uid'] = bnode
                obj['dgraph.type'] = node_type
                batch.append(obj)
            except Exception as e:
                print(f"   ⚠️ Error transformando fila: {e}")

        if batch:
            resp = txn.mutate(set_obj=batch)

            # Crear mapa de UIDs reales devueltos por Dgraph
            for item in batch:
                original_key = item['uid'].replace('_:', '')
                # Obtener UID real (buscando por bnode key o string key)
                real_uid = resp.uids.get(item['uid']) or resp.uids.get(original_key)

                if real_uid:
                    uids_map[original_key] = real_uid

        txn.commit()
        print(f"   ✅ {len(uids_map)} nodos insertados.")
    except Exception as e:
        print(f"❌ Error crítico cargando nodos: {e}")
    finally:
        txn.discard()
        if f: f.close()

    return uids_map

def load_simple_edges(client, file_path, source_map, target_map, src_col, tgt_col, edge_name):
    """Carga relaciones simples (1-1, 1-N) leyendo IDs y buscando sus UIDs en los mapas"""
    print(f"🔗 Conectando '{edge_name}' desde {os.path.basename(file_path)}...")
    txn = client.txn()
    count = 0
    reader, f = safe_read_csv(file_path)
    if f is None: return

    try:
        batch = []
        for row in reader:
            s_key = get_bnode_key(row.get(src_col))
            t_key = get_bnode_key(row.get(tgt_col))

            s_uid = source_map.get(s_key)
            t_uid = target_map.get(t_key)

            if s_uid and t_uid:
                batch.append({
                    'uid': s_uid,
                    edge_name: {'uid': t_uid}
                })
                count += 1

        if batch:
            txn.mutate(set_obj=batch)
            txn.commit()
            print(f"   ✅ {count} relaciones creadas.")
        else:
            print(f"   ⚠️  No se encontraron coincidencias para '{edge_name}'.")

    except Exception as e:
        print(f"❌ Error cargando aristas: {e}")
    finally:
        txn.discard()
        if f: f.close()

def load_tx_flows(client, file_path, tx_map, acc_map, dev_map, ip_map):
    """Carga el grafo complejo de transacciones (Tx -> Account, Device, IP)"""
    print(f"🔀 Conectando Flujo de Transacciones desde {os.path.basename(file_path)}...")
    txn = client.txn()
    count = 0
    reader, f = safe_read_csv(file_path)
    if f is None: return

    try:
        batch = []
        for row in reader:
            tx_key = get_bnode_key(row['tx_id'])
            tx_uid = tx_map.get(tx_key)

            if not tx_uid: continue

            mu = {'uid': tx_uid}

            # Conectar si el UID existe en el mapa
            if row.get('from_account') in acc_map:
                mu['from_account'] = {'uid': acc_map[row['from_account']]}

            if row.get('to_account') in acc_map:
                mu['to_account'] = {'uid': acc_map[row['to_account']]}

            if row.get('used_device') in dev_map:
                mu['used_device'] = {'uid': dev_map[row['used_device']]}

            if row.get('used_ip') in ip_map:
                mu['used_ip'] = {'uid': ip_map[row['used_ip']]}

            batch.append(mu)
            count += 1

        if batch:
            txn.mutate(set_obj=batch)
            txn.commit()
            print(f"   ✅ {count} flujos conectados.")
    except Exception as e:
        print(f"❌ Error en flujos: {e}")
    finally:
        txn.discard()
        if f: f.close()

#Load data para usar los uids generados y cargarlos a dgraph con las respectivas relaciones

def load_data(client):
    """
    Función maestra que orquesta toda la carga.
    Recibe tu objeto 'client' de Dgraph ya conectado.
    """
    print(f"\n🚀 INICIANDO CARGA DE DATOS")
    print(f"📂 Ruta de datos detectada: {DATA_PATH}\n")

    # 1. LOAD NODES (y obtener mapas de UIDs)
    uids_users = load_generic_nodes(client, FILES['users'], 'User', 'user_id', tf_user)
    uids_accounts = load_generic_nodes(client, FILES['accounts'], 'Account', 'account_id', tf_account)
    uids_devices = load_generic_nodes(client, FILES['devices'], 'Device', 'device_id', tf_device)
    uids_ips = load_generic_nodes(client, FILES['ips'], 'IpAddress', 'ip_addr', tf_ip)
    uids_docs = load_generic_nodes(client, FILES['docs'], 'Document', 'document_id', tf_doc)
    uids_txs = load_generic_nodes(client, FILES['txs'], 'Transaction', 'tx_id', tf_tx)

    print("\n--- NODOS LISTOS. VINCULANDO RELACIONES ---\n")

    # 2. LOAD EDGES (usando los mapas de UIDs)
    load_simple_edges(client, FILES['rel_acc'], uids_users, uids_accounts, 'user_id', 'account_id', 'owns_account')
    load_simple_edges(client, FILES['rel_dev'], uids_users, uids_devices, 'user_id', 'device_id', 'uses_device')
    load_simple_edges(client, FILES['rel_ip'], uids_users, uids_ips, 'user_id', 'ip_addr', 'known_ips')
    load_simple_edges(client, FILES['rel_doc'], uids_users, uids_docs, 'user_id', 'document_id', 'has_document')
    load_simple_edges(client, FILES['rel_dev_ip'], uids_devices, uids_ips, 'device_id', 'ip_addr', 'has_ip')

    # 3. LOAD COMPLEX FLOWS
    load_tx_flows(client, FILES['rel_tx'], uids_txs, uids_accounts, uids_devices, uids_ips)

    print("\n🎉 CARGA COMPLETADA EXITOSAMENTE.")
