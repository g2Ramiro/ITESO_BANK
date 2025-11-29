# populate.py
from cassandra.cluster import Cluster
import connect
from Cassandra.loader import create_keyspace_and_tables, load_all_data
from Dgraph import model as mo

def populate_cassandra():
    ips = [ip.strip() for ip in connect.CLUSTER_IPS.split(",")]
    cluster = Cluster(ips)
    session = cluster.connect()

    rf = int(connect.REPLICATION_FACTOR)
    create_keyspace_and_tables(session, connect.KEYSPACE, rf)
    load_all_data(session, connect.KEYSPACE, base_path="data/Cassandra")

    print("✅ Cassandra poblada correctamente.")
    cluster.shutdown()

def populate_dgraph():
    print("🚀 Conectando a Dgraph...")
    client_stub = connect.create_client_stub()
    client = connect.create_client(client_stub)

    try:
        mo.create_schema(client)
        mo.load_data(client)
    finally:
        client_stub.close()

if __name__ == "__main__":
    populate_cassandra()
    populate_dgraph()
