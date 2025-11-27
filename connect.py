import os
from cassandra.cluster import Cluster
from pymongo import MongoClient
import pydgraph

#Cassandra
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'itesobank_antifraude')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

#MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.ItesoBank

# Dgraph
DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080")

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

