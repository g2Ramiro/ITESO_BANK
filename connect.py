import os
from cassandra.cluster import Cluster
from pymongo import MongoClient

CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'logistics')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
client = MongoClient('mongodb://localhost:27017/')
db = client.ItesoBank

