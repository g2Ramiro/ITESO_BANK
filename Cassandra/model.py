#!/usr/bin/env python3
from datetime import datetime, timedelta
import random
import uuid
import time_uuid
from .utils import print_table
from cassandra.query import BatchStatement

#Crear keyspace
CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

# Requerimiento 1
CREATE_TRANSACTIONS_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS transactions_by_user (
        user_id INT,
        account_id TEXT,
        tx_id INT,
        amount DECIMAL,
        type_tx TEXT,
        state TEXT,
        account_dty TEXT,
        user_dty INT,
        tx_date DATE,
        PRIMARY KEY ((user_id), tx_date, account_id)
    ) WITH CLUSTERING ORDER BY (tx_date DESC, account_id ASC)
"""

# Requerimiento 2
CREATE_TOP_TRANSACTIONS_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS top_transactions_by_user (
        user_id INT,
        account_id TEXT,
        tx_id INT,
        amount DECIMAL,
        type_tx TEXT,
        state TEXT,
        account_dty TEXT,
        user_dty INT,
        tx_date DATE,
        PRIMARY KEY ((user_id), tx_date, amount)
    ) WITH CLUSTERING ORDER BY (tx_date ASC, amount DESC)
"""

# Requerimiento 4
CREATE_TRANSFERS_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS transfers_by_user (
        user_id INT,
        account_id TEXT,
        tx_id INT,
        amount DECIMAL,
        type_tx TEXT,
        state TEXT,
        account_dty TEXT,
        user_dty INT,
        tx_date DATE,
        PRIMARY KEY ((user_id), user_dty)
    ) WITH CLUSTERING ORDER BY (user_dty ASC)
"""

# Requerimiento 8
CREATE_OUT_OF_RANGE_TRANSACTIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS out_of_range_transactions (
        user_id INT,
        account_id TEXT,
        tx_id INT,
        amount DECIMAL,
        type_tx TEXT,
        state TEXT,
        account_dty TEXT,
        user_dty INT,
        tx_date DATE,
        PRIMARY KEY ((user_id), tx_date, amount, tx_id)
    ) WITH CLUSTERING ORDER BY (tx_date ASC, amount DESC, tx_id DESC)
"""

# Requerimiento 9
CREATE_REJECTED_ATTEMPTS_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS rejected_attempts_by_user (
        user_id INT,
        account_id TEXT,
        tx_id INT,
        amount DECIMAL,
        type_tx TEXT,
        state TEXT,
        account_dty TEXT,
        user_dty INT,
        tx_date DATE,
        PRIMARY KEY ((user_id), state, tx_date)
    ) WITH CLUSTERING ORDER BY (state ASC, tx_date ASC)
"""

# Requerimiento 6
CREATE_REALTIME_TRANSACTIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS realtime_transactions (
        tx_day TEXT,
        user_id INT,
        account_id TEXT,
        tx_id INT,
        amount DECIMAL,
        type_tx TEXT,
        state TEXT,
        account_dty TEXT,
        user_dty INT,
        tx_date DATE,
        PRIMARY KEY ((tx_day), tx_id)
    ) WITH CLUSTERING ORDER BY (tx_id DESC)
"""

#Requerimiento 3:
CREATE_ACCOUNTS_BY_TRANSACTIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_transactions (
        user_id INT,
        account_id TEXT,
        total_transacciones INT,
        account_balance DECIMAL,
        PRIMARY KEY ((user_id), total_transacciones)
    ) WITH CLUSTERING ORDER BY (total_transacciones DESC)
"""

#Requerimiento 7
CREATE_ALERTS_BY_ACCOUNT_STATUS_TABLE = """
    CREATE TABLE IF NOT EXISTS alerts_by_account_status (
        account_id TEXT,
        status TEXT,
        alert_id INT,
        date_detected TIMESTAMP,
        user_id INT,
        trs_id INT,
        alert_type TEXT,
        riskscore INT,
        descrip TEXT,
        PRIMARY KEY ((account_id), status, date_detected, alert_id)
    ) WITH CLUSTERING ORDER BY (status ASC, date_detected DESC, alert_id ASC)
"""

#Requerimiento 10
CREATE_RECEIVED_TRANSACTIONS_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS received_transactions_by_user (
        user_id INT,
        date DATE,
        tx_id INT,
        account_id TEXT,
        sender_acc_id TEXT,
        amount DECIMAL,
        status TEXT,
        tx_type TEXT,
        PRIMARY KEY ((user_id), date, amount)
    ) WITH CLUSTERING ORDER BY (date DESC, amount ASC)
"""

#Requerimiento 11:
CREATE_DUPLICATE_TRANSACTIONS_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS duplicate_transactions_by_user (
        user_id INT,
        date DATE,
        tx_id INT,
        account_id TEXT,
        sender_acc_id TEXT,
        amount DECIMAL,
        status TEXT,
        tx_type TEXT,
        PRIMARY KEY ((user_id), sender_acc_id, tx_id)
    ) WITH CLUSTERING ORDER BY (sender_acc_id ASC, tx_id ASC)
"""

#Requerimiento 12:
CREATE_TRANSACTION_STATUS_CHANGES_TABLE = """
    CREATE TABLE IF NOT EXISTS transaction_status_changes (
        trs_id INT,
        account_id TEXT,
        user_id INT,
        old_status TEXT,
        new_status TEXT,
        change_date TIMESTAMP,
        change_reason TEXT,
        PRIMARY KEY ((trs_id), change_date)
    ) WITH CLUSTERING ORDER BY (change_date ASC)
"""

# ==========================
# QUERIES - REQUERIMIENTOS CASSANDRA
# ==========================

# 1) Historial de movimientos (Cassandra #1)
def q_historial_transaccional(session, user_id: int, limit: int = 100):
    """
    Historial completo de movimientos de un usuario, ordenado por fecha desc y cuenta.
    """
    cql = f"""
    SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date
    FROM transactions_by_user
    WHERE user_id = %s
    ORDER BY tx_date DESC, account_id ASC
    LIMIT {limit};
    """
    return session.execute(cql, (user_id,))


# 2) Operaciones de mayor cuantía histórica (Cassandra #2)
def q_top_operaciones_por_usuario(session, user_id: int, limit: int = 20):
    """
    Top operaciones de mayor monto para un usuario.
    """
    cql = f"""
    SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date
    FROM top_transactions_by_user
    WHERE user_id = %s
    LIMIT {limit};
    """

    return session.execute(cql, (user_id,))


# 3) Usuarios con mayor frecuencia transaccional (Cassandra #3)
def q_cuentas_por_usuario(session, user_id: int):
    """
    Cuentas de un usuario ordenadas por número de transacciones (desc)
    """
    cql = """
    SELECT user_id, account_id, total_transacciones, account_balance
    FROM accounts_by_transactions
    WHERE user_id = %s
    ORDER BY total_transacciones DESC;
    """
    return session.execute(cql, (user_id,))


def q_top_cuentas_global(session, limit: int = 50):
    """
    Top global de cuentas por volumen/actividad.
    Cassandra no permite ORDER BY cross-partición, así que:
      - Aquí traemos todo y el orden global se haría en Python.
    Se usa en: analítica forense -> opción 1 (Top Cuentas por Volumen).
    """
    cql = "SELECT user_id, account_id, total_transacciones, account_balance FROM accounts_by_transactions;"
    rows = list(session.execute(cql))
    # Ordenamos en Python
    rows.sort(key=lambda r: r.total_transacciones, reverse=True)
    return rows[:limit]


# 4) Transferencias internas (Cassandra #4)
def q_transferencias_por_usuario(session, user_id: int):
    """
    Transferencias hechas por un usuario
    """
    cql = """
    SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date
    FROM transfers_by_user
    WHERE user_id = %s
    ORDER BY user_dty ASC;
    """
    return session.execute(cql, (user_id,))


# 6) Transacciones en tiempo real / por día (Cassandra #6)
def q_realtime_por_dia(session, tx_day: str):
    """
    Transacciones de un día lógico
    """
    cql = """
    SELECT tx_day, user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date
    FROM realtime_transactions
    WHERE tx_day = %s
    ORDER BY tx_id DESC;
    """
    return session.execute(cql, (tx_day,))


# 8) Transacciones fuera de rango/umbral (Cassandra #8)
def q_transacciones_fuera_de_rango_global(session, limit: int = 100):
    """
    Lista global de transacciones fuera de rango
    """
    cql = "SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date FROM out_of_range_transactions;"
    rows = list(session.execute(cql))
    rows.sort(key=lambda r: float(r.amount), reverse=True)
    return rows[:limit]


def q_transacciones_fuera_de_rango_usuario(session, user_id: int, limit: int = 50):
    """
    Versión filtrada por usuario.
    """
    cql = f"""
    SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date
    FROM out_of_range_transactions
    WHERE user_id = %s
    LIMIT {limit};
    """
    return session.execute(cql, (user_id,))


# 9) Intentos de operación rechazados (Cassandra #9)
def q_intentos_rechazados_global(session, limit: int = 100):
    """
   trae todos los intentos rechazados / fallidos.
    """
    cql = "SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date FROM rejected_attempts_by_user;"
    rows = list(session.execute(cql))
    return rows[:limit]


def q_intentos_rechazados_usuario(session, user_id: int):
    """
    Intentos rechazados por usuario.
    """
    cql = """
    SELECT user_id, account_id, tx_id, amount, type_tx, state, account_dty, user_dty, tx_date
    FROM rejected_attempts_by_user
    WHERE user_id = %s;
    """
    return session.execute(cql, (user_id,))


# 10) Flujo de dinero entrante (Cassandra #10)
def q_transacciones_recibidas_usuario(session, user_id: int, limit: int = 50):
    """
    Transacciones recibidas por un usuario (entrantes).
    """
    cql = f"""
    SELECT user_id, date, tx_id, account_id, sender_acc_id, amount, status, tx_type
    FROM received_transactions_by_user
    WHERE user_id = %s
    LIMIT {limit};
    """
    return session.execute(cql, (user_id,))


# 11) Auditoría de duplicados (Cassandra #11)
def q_duplicados_global(session, limit: int = 100):
    """
    Auditoría global de transacciones duplicadas.
    """
    cql = "SELECT user_id, date, tx_id, account_id, sender_acc_id, amount, status, tx_type FROM duplicate_transactions_by_user;"
    rows = list(session.execute(cql))
    return rows[:limit]


def q_duplicados_usuario(session, user_id: int):
    """
    Transacciones duplicadas por usuario.
    """
    cql = """
    SELECT user_id, date, tx_id, account_id, sender_acc_id, amount, status, tx_type
    FROM duplicate_transactions_by_user
    WHERE user_id = %s;
    """
    return session.execute(cql, (user_id,))


# 12) Estado de transacciones en curso (Cassandra #12)
def q_cambios_estado_por_usuario(session, user_id: int):
    """
    Historial de cambios de estado de transacciones para un usuario.
    """
    cql = """
    SELECT trs_id, account_id, user_id, old_status, new_status, change_date, change_reason
    FROM transaction_status_changes
    WHERE user_id = %s ALLOW FILTERING;
    """
    return session.execute(cql, (user_id,))


# FUNCIONES DE PRESENTACIÓN

def show_historial_transaccional(session, user_id, limit=100):
    rows = q_historial_transaccional(session, user_id, limit)
    print_table(
        rows,
        columns=[
            "tx_date",
            "account_id",   # cuenta origen
            "account_dty",  # cuenta destino
            "user_dty",     # usuario destino
            "tx_id",
            "amount",
            "type_tx",
            "state",
        ],
        title=f"[📄 Historial transaccional del usuario {user_id} (origen → destino)]"
    )




def show_transacciones_recibidas(session, user_id, limit=50):
    rows = q_transacciones_recibidas_usuario(session, user_id, limit)
    print_table(
        rows,
        columns=["date", "tx_id", "account_id", "sender_acc_id", "amount", "status"],
        title=f"[💰 Transacciones recibidas por el usuario {user_id}]"
    )


def show_transferencias_usuario(session, user_id):
    rows = q_transferencias_por_usuario(session, user_id)
    print_table(
        rows,
        columns=[
            "tx_date",
            "account_id",   # origen
            "account_dty",  # destino
            "user_dty",     # usuario destino
            "tx_id",
            "amount",
            "type_tx",
            "state",
        ],
        title=f"[🔄 Transferencias internas (pitufeo) user_id={user_id} (origen → destino)]"
    )



def show_cambios_estado_usuario(session, user_id):
    rows = q_cambios_estado_por_usuario(session, user_id)
    print_table(
        rows,
        columns=["change_date", "account_id", "trs_id", "old_status", "new_status"],
        title=f"[📌 Cambios de estado (cuentas / transacciones) user_id={user_id}]"
    )


def show_transacciones_fuera_de_rango_global(session, limit=100):
    rows = q_transacciones_fuera_de_rango_global(session, limit)
    print_table(
        rows,
        columns=[
            "user_id",
            "tx_date",
            "account_id",   # origen
            "account_dty",  # destino
            "user_dty",     # usuario destino
            "tx_id",
            "amount",
            "type_tx",
            "state",
        ],
        title="[🚨 Transacciones fuera de rango / umbral (origen → destino)]"
    )


def show_intentos_rechazados_global(session, limit=100):
    rows = q_intentos_rechazados_global(session, limit)
    print_table(
        rows,
        columns=[
            "user_id",
            "tx_date",
            "account_id",   # origen
            "account_dty",  # destino
            "user_dty",
            "tx_id",
            "amount",
            "state",
            "type_tx",
        ],
        title="[⛔ Intentos de operación rechazados / fallidos (origen → destino)]"
    )



def show_top_cuentas_global(session, limit=20):
    rows = q_top_cuentas_global(session, limit)
    print_table(
        rows,
        columns=["user_id", "account_id", "total_transacciones", "account_balance"],
        title="[🏆 Top cuentas por volumen de transacciones]"
    )


def show_cuentas_por_usuario(session, user_id):
    rows = q_cuentas_por_usuario(session, user_id)
    print_table(
        rows,
        columns=["account_id", "total_transacciones", "account_balance"],
        title=f"[📈 Frecuencia transaccional para user_id={user_id}]"
    )


def show_top_operaciones_usuario(session, user_id, limit=20):
    rows = q_top_operaciones_por_usuario(session, user_id, limit)
    print_table(
        rows,
        columns=["tx_date", "account_id", "tx_id", "amount", "type_tx", "state"],
        title=f"[💸 Top operaciones de mayor cuantía para user_id={user_id}]"
    )


def show_duplicados_global(session, limit=100):
    rows = q_duplicados_global(session, limit)
    print_table(
        rows,
        columns=["user_id", "date", "account_id", "sender_acc_id", "tx_id", "amount", "status"],
        title="[🧬 Auditoría de transacciones duplicadas]"
    )

