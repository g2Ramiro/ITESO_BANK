# loader.py
from cassandra.query import BatchStatement
from datetime import datetime
import csv

from .model import (
    CREATE_KEYSPACE,
    CREATE_TRANSACTIONS_BY_USER_TABLE,
    CREATE_TOP_TRANSACTIONS_BY_USER_TABLE,
    CREATE_TRANSFERS_BY_USER_TABLE,
    CREATE_OUT_OF_RANGE_TRANSACTIONS_TABLE,
    CREATE_REJECTED_ATTEMPTS_BY_USER_TABLE,
    CREATE_ACCOUNTS_BY_TRANSACTIONS_TABLE,
    CREATE_REALTIME_TRANSACTIONS_TABLE,
    CREATE_ALERTS_BY_ACCOUNT_STATUS_TABLE,
    CREATE_RECEIVED_TRANSACTIONS_BY_USER_TABLE,
    CREATE_DUPLICATE_TRANSACTIONS_BY_USER_TABLE,
    CREATE_TRANSACTION_STATUS_CHANGES_TABLE,
)


# Helpers de tipos
def parse_date(value: str):
    """
    Convierte 'YYYY-MM-DD' a datetime.date.
    """
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_timestamp(value: str):
    """
    Convierte un string ISO a datetime (TIMESTAMP Cassandra).
    Soporta formato con 'Z' al final.
    """
    if not value:
        return None
    v = value.strip()
    # Ej: '2024-07-20T07:00:00Z'
    if v.endswith("Z"):
        v = v[:-1]
    # Si no tiene 'T', intenta con formato simple
    if "T" in v:
        return datetime.fromisoformat(v)
    # Ej: '2024-07-20 07:00:00'
    try:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Último intento: solo fecha
        return datetime.strptime(v, "%Y-%m-%d")


def default_convert(value: str):
    """
    Conversión por defecto: deja el string tal cual o None.
    """
    if value == "" or value is None:
        return None
    return value

# Creación de keyspace y tablas
def create_keyspace_and_tables(session, keyspace_name: str, replication_factor: int = 1):
    """
    Crea el keyspace (si no existe) y todas las tablas definidas en model.py
    """
    # Crear keyspace
    session.execute(CREATE_KEYSPACE.format(keyspace_name, replication_factor))
    session.set_keyspace(keyspace_name)

    # Crear tablas
    session.execute(CREATE_TRANSACTIONS_BY_USER_TABLE)
    session.execute(CREATE_TOP_TRANSACTIONS_BY_USER_TABLE)
    session.execute(CREATE_TRANSFERS_BY_USER_TABLE)
    session.execute(CREATE_OUT_OF_RANGE_TRANSACTIONS_TABLE)
    session.execute(CREATE_REJECTED_ATTEMPTS_BY_USER_TABLE)
    session.execute(CREATE_ACCOUNTS_BY_TRANSACTIONS_TABLE)
    session.execute(CREATE_REALTIME_TRANSACTIONS_TABLE)
    session.execute(CREATE_ALERTS_BY_ACCOUNT_STATUS_TABLE)
    session.execute(CREATE_RECEIVED_TRANSACTIONS_BY_USER_TABLE)
    session.execute(CREATE_DUPLICATE_TRANSACTIONS_BY_USER_TABLE)
    session.execute(CREATE_TRANSACTION_STATUS_CHANGES_TABLE)

# Carga genérica desde CSV
def load_csv_into_table(session,
                        keyspace_name: str,
                        table_name: str,
                        csv_path: str,
                        columns: list,
                        converters: dict | None = None,
                        batch_size: int = 50):
    """
    Carga un CSV en una tabla Cassandra usando BatchStatement.

    :param session: sesión de Cassandra
    :param keyspace_name: nombre del keyspace
    :param table_name: nombre de la tabla destino
    :param csv_path: ruta al archivo CSV
    :param columns: lista de columnas en el mismo orden que el CSV
    :param converters: dict opcional {col_name: func_conversion}
    :param batch_size: tamaño del batch para inserts
    """
    session.set_keyspace(keyspace_name)

    placeholders = ", ".join(["?"] * len(columns))
    cols_str = ", ".join(columns)
    insert_cql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    prepared = session.prepare(insert_cql)

    if converters is None:
        converters = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = BatchStatement()
        count = 0

        for row in reader:
            values = []
            for col in columns:
                raw_val = row[col]

                if raw_val == "" or raw_val is None:
                    values.append(None)
                    continue

                if col in converters:
                    values.append(converters[col](raw_val))
                else:
                    values.append(default_convert(raw_val))

            batch.add(prepared, values)
            count += 1

            if count % batch_size == 0:
                session.execute(batch)
                batch.clear()

        # Ejecutar lo que quede
        if len(batch) > 0:
            session.execute(batch)


# Carga info de las tablas

def load_all_data(session, keyspace_name: str, base_path: str = "data"):
    """
    Carga los datos de TODOS los CSV en sus tablas correspondientes.
    Se asume que los archivos CSV existen en la carpeta 'base_path'.
    """

    # 1) transactions_by_user.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "transactions_by_user",
        f"{base_path}/transactions_by_user.csv",
        ["user_id", "account_id", "tx_id", "amount", "type_tx", "state", "account_qty", "user_qty", "tx_date"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "account_qty": int,
            "user_qty": int,
            "tx_date": parse_date,
        },
    )

    # 2) top_transactions_by_user.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "top_transactions_by_user",
        f"{base_path}/top_transactions_by_user.csv",
        ["user_id", "account_id", "tx_id", "amount", "type_tx", "state", "account_qty", "user_qty", "tx_date"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "account_qty": int,
            "user_qty": int,
            "tx_date": parse_date,
        },
    )

    # 3) transfers_by_user.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "transfers_by_user",
        f"{base_path}/transfers_by_user.csv",
        ["user_id", "account_id", "tx_id", "amount", "type_tx", "state", "account_qty", "user_qty", "tx_date"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "account_qty": int,
            "user_qty": int,
            "tx_date": parse_date,
        },
    )

    # 4) out_of_range_transactions.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "out_of_range_transactions",
        f"{base_path}/out_of_range_transactions.csv",
        ["user_id", "account_id", "tx_id", "amount", "type_tx", "state", "account_qty", "user_qty", "tx_date"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "account_qty": int,
            "user_qty": int,
            "tx_date": parse_date,
        },
    )

    # 5) rejected_attempts_by_user.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "rejected_attempts_by_user",
        f"{base_path}/rejected_attempts_by_user.csv",
        ["user_id", "account_id", "tx_id", "amount", "type_tx", "state", "account_qty", "user_qty", "tx_date"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "account_qty": int,
            "user_qty": int,
            "tx_date": parse_date,
        },
    )

    # 6) accounts_by_transactions.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "accounts_by_transactions",
        f"{base_path}/accounts_by_transactions.csv",
        ["user_id", "account_id", "total_transacciones", "account_balance"],
        converters={
            "user_id": int,
            "total_transacciones": int,
            "account_balance": float,
        },
    )

    # 7) realtime_transactions.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "realtime_transactions",
        f"{base_path}/realtime_transactions.csv",
        ["tx_day", "user_id", "account_id", "tx_id", "amount", "type_tx", "state", "account_qty", "user_qty", "tx_date"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "account_qty": int,
            "user_qty": int,
            "tx_date": parse_date,
        },
    )

    # 8) alerts_by_account_status.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "alerts_by_account_status",
        f"{base_path}/alerts_by_account_status.csv",
        ["account_id", "status", "alert_id", "date_detected", "user_id", "trs_id", "alert_type", "riskscore", "descrip"],
        converters={
            "alert_id": int,
            "date_detected": parse_timestamp,
            "user_id": int,
            "trs_id": int,
            "riskscore": int,
        },
    )

    # 9) received_transactions_by_user.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "received_transactions_by_user",
        f"{base_path}/received_transactions_by_user.csv",
        ["user_id", "date", "tx_id", "account_id", "sender_acc_id", "amount", "status", "tx_type"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "date": parse_date,
        },
    )

    # 10) duplicate_transactions_by_user.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "duplicate_transactions_by_user",
        f"{base_path}/duplicate_transactions_by_user.csv",
        ["user_id", "date", "tx_id", "account_id", "sender_acc_id", "amount", "status", "tx_type"],
        converters={
            "user_id": int,
            "tx_id": int,
            "amount": float,
            "date": parse_date,
        },
    )

    # 11) transaction_status_changes.csv
    load_csv_into_table(
        session,
        keyspace_name,
        "transaction_status_changes",
        f"{base_path}/transaction_status_changes.csv",
        ["trs_id", "account_id", "user_id", "old_status", "new_status", "change_date", "change_reason"],
        converters={
            "trs_id": int,
            "user_id": int,
            "change_date": parse_timestamp,
        },
    )
