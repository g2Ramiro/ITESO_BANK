# populate.py

from cassandra.cluster import Cluster
from connect import CLUSTER_IPS, KEYSPACE, REPLICATION_FACTOR
from Cassandra.loader import create_keyspace_and_tables, load_all_data

def populate_cassandra():
    ips = [ip.strip() for ip in CLUSTER_IPS.split(",")]
    cluster = Cluster(ips)
    session = cluster.connect()

    rf = int(REPLICATION_FACTOR)
    create_keyspace_and_tables(session, KEYSPACE, rf)

    # usa la carpeta data/Cassandra donde estan los CSV
    load_all_data(session, KEYSPACE, base_path="data/Cassandra")

    print("✅ Cassandra poblada correctamente.")
    cluster.shutdown()

if __name__ == "__main__":
    populate_cassandra()
