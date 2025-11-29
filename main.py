import time
from cassandra.cluster import Cluster
import connect as cn
from connect import CLUSTER_IPS, KEYSPACE
from populate import populate_cassandra, populate_dgraph
from Cassandra import model as cas
from Dgraph import querys as dg_qry

# =====================================================================
# UTILERÍAS
# =====================================================================

def get_cassandra_session():

    ips = [ip.strip() for ip in CLUSTER_IPS.split(",") if ip.strip()]
    if not ips:
        raise ValueError("No se han definido IPs para Cassandra en connect.py")

    cluster = Cluster(ips)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    return cluster, session

def ejecutar(db_name, menu_num, descripcion, param=None):
    """
    Función auxiliar para simular/ejecutar opciones que aún no tienen
    lógica implementada (ej: MongoDB) o para debug.
    """
    print(f"\n[🚧 MOCK] Ejecutando consulta en {db_name}...")
    print(f"   Opción #{menu_num}: {descripcion}")
    if param:
        print(f"   Parámetro: {param}")
    print("   ✅ Resultado simulado: Operación registrada/consultada con éxito.")
    time.sleep(0.5)

# =====================================================================
# 1. INVESTIGACIÓN INDIVIDUAL
# =====================================================================
def menu_investigacion_cliente(session, client):
    print("\n============== 🕵️ INVESTIGACIÓN DE OBJETIVO (CLIENTE) ==============")
    print("Ingrese el ID o Nombre del cliente a investigar:")
    cliente_id = input(">> ").strip()

    if not cliente_id:
        print("Error: Identificador requerido para iniciar rastreo.")
        return

    print(f"\n--- 🎯 Objetivo Fijado: {cliente_id} ---")

    while True:
        print(f"\n[OBJETIVO: {cliente_id}] Seleccione vector de análisis:")
        print("   --- 📋 Perfil Digital y Huella ---")
        print("   1. Perfil completo y Cuentas asociadas (Mongo #3)")
        print("   2. Dispositivos y Huella Digital (Mongo #8)")
        print("   3. Bitácora de Accesos/Login (Mongo #2)")

        print("   --- 💸 Análisis Transaccional (Cassandra) ---")
        print("   4. Historial de movimientos (Cassandra #1)")
        print("   5. Flujo de dinero entrante (Cassandra #10)")
        print("   6. Transferencias internas (Posible Pitufeo) (Cassandra #4)")
        print("   7. Estado de transacciones en curso (Cassandra #12)")

        print("   --- ⚠️ Evaluación de Riesgo ---")
        print("   8. Calcular Risk Score del sujeto (Mongo #12)")
        print("   9. Mapa de conexiones sospechosas (Dgraph #6)")

        print("   0. 🔙 Abortar investigación / Nuevo objetivo")

        opcion = input("   >> ").strip()

        # --- MONGO DB (Simulados con la función ejecutar) ---
        if opcion == "1":
            ejecutar("MongoDB", 3, "Información de Cuentas", cliente_id)
        elif opcion == "2":
            ejecutar("MongoDB", 8, "Dispositivos por usuario", cliente_id)
        elif opcion == "3":
            ejecutar("MongoDB", 2, "Inicio de Sesión", cliente_id)
        elif opcion == "8":
            ejecutar("MongoDB", 12, "Perfil de riesgo usuario", cliente_id)

        # --- DGRAPH ---
        elif opcion == "9":
            print(f"Analizando conexiones de riesgo para el usuario {cliente_id}...")
            # Llamada al script queries.py
            dg_qry.query_risk_scoring(client, cliente_id)

        # --- CASSANDRA ---
        elif opcion in {"4", "5", "6", "7"}:
            try:
                uid = int(cliente_id)
            except ValueError:
                print("   ⚠ Para consultas en Cassandra necesitas un ID numérico (user_id).")
                continue

            if opcion == "4":
                cas.show_historial_transaccional(session, uid, limit=100)
            elif opcion == "5":
                cas.show_transacciones_recibidas(session, uid, limit=50)
            elif opcion == "6":
                cas.show_transferencias_usuario(session, uid)
            elif opcion == "7":
                cas.show_cambios_estado_usuario(session, uid)

        elif opcion == "0":
            break
        else:
            print("Comando no reconocido.")


# =====================================================================
# 2. MONITOR DE AMENAZAS
# =====================================================================
def menu_monitor_amenazas(session, client):
    while True:
        print("\n============== 🛡️ MONITOR DE AMENAZAS GLOBALES ==============")
        print("   --- 🚨 Alertas Activas (Live) ---")
        print("   1. Transacciones fuera de rango/umbral (Cassandra #8)")
        print("   2. Intentos de operación rechazados (Cassandra #9)")
        print("   3. Alerta masiva: Cambios IP/Dispositivo (Mongo #11)")

        print("   --- 🕸️ Detección de Patrones Complejos (Graph) ---")
        print("   4. Anillos de Colaboración Fraudulenta (Dgraph #1)")
        print("   5. Tipologías de Lavado de Dinero (Dgraph #3)")
        print("   6. Cuentas Fantasma / Synthetic ID (Dgraph #7)")
        print("   7. Suplantación de Identidad (Account Takeover) (Dgraph #8)")
        print("   8. Rastreo de rutas de dinero ilícito (Dgraph #9)")

        print("   --- 🚩 Watchlists y Anomalías ---")
        print("   9. Usuarios en Lista Negra / Flageados (Mongo #6)")
        print("   10. Comportamiento errático de cuentas (Mongo #7)")

        print("   0. 🔙 Regresar al menú principal")

        opcion = input("   >> ").strip()

        # --- CASSANDRA ---
        if opcion == "1":
            cas.show_transacciones_fuera_de_rango_global(session, limit=100)
        elif opcion == "2":
            cas.show_intentos_rechazados_global(session, limit=100)

        # --- MONGO DB ---
        elif opcion == "3":
            ejecutar("MongoDB", 11, "Cambios masivos IP/Disp")
        elif opcion == "9":
            ejecutar("MongoDB", 6, "Cuentas Flageadas")
        elif opcion == "10":
            ejecutar("MongoDB", 7, "Comportamiento errático")

        # --- DGRAPH ---
        elif opcion == "4":
            # Anillos de Colaboración
            dev_input = input("   Ingrese ID del Dispositivo sospechoso (ej: DEV_FRAUD_RING_X): ").strip() or "DEV_FRAUD_RING_X"
            dg_qry.query_fraud_ring(client, dev_input)

        elif opcion == "5":
            # Lavado de dinero
            monto_input = input("   Monto mínimo para alertar (default 5000): ").strip() or "5000"
            try:
                dg_qry.query_money_laundering_pattern(client, float(monto_input))
            except ValueError:
                print("   Error: El monto debe ser un número.")

        elif opcion == "6":
            # Cuentas Fantasmas
            bal_input = input("   Saldo máximo (default 100): ").strip() or "100"
            try:
                dg_qry.query_ghost_accounts(client, float(bal_input), min_txs=2)
            except ValueError:
                print("   Error: El saldo debe ser un número.")

        elif opcion == "7":
            # Suplantación
            dg_qry.query_identity_theft(client)

        elif opcion == "8":
            # Rutas sospechosas
            acc_input = input("   Ingrese ID de Cuenta Origen para rastrear (ej: ACCT-3004-B): ").strip()
            if acc_input:
                dg_qry.query_suspicious_path(client, acc_input)
            else:
                print("   ⚠ ID de cuenta requerido.")

        elif opcion == "0":
            break
        else:
            print("Comando no reconocido.")


# =====================================================================
# 3. ANALÍTICA FORENSE
# =====================================================================
def menu_analitica_forense(session, client):
    while True:
        print("\n============== 📊 ANALÍTICA FORENSE Y REPORTES ==============")
        print("   1. Top Cuentas por Volumen/Actividad (Cassandra #5)")
        print("   2. Usuarios con mayor frecuencia transaccional (Cassandra #3)")
        print("   3. Operaciones de mayor cuantía histórica (Cassandra #2)")
        print("   4. Mapa global de saldos y usuarios (Mongo #5)")
        print("   5. Auditoría de cuentas nuevas (Alto Riesgo) (Mongo #10)")
        print("   6. Análisis de propagación de riesgo (Dgraph #10)")
        print("   7. Mapa de calor geográfico (Dgraph #4)")
        print("   8. Auditoría de duplicados (Cassandra #11)")

        print("   0. 🔙 Regresar al menú principal")

        opcion = input("   >> ").strip()

        # --- CASSANDRA ---
        if opcion == "1":
            cas.show_top_cuentas_global(session, limit=20)

        elif opcion == "2":
            uid_raw = input("   Ingrese user_id para analizar su frecuencia: ").strip()
            if uid_raw.isdigit():
                cas.show_cuentas_por_usuario(session, int(uid_raw))
            else:
                print("   ⚠ user_id debe ser numérico.")

        elif opcion == "3":
            uid_raw = input("   Ingrese user_id para ver sus operaciones de mayor monto: ").strip()
            if uid_raw.isdigit():
                cas.show_top_operaciones_usuario(session, int(uid_raw), limit=20)
            else:
                print("   ⚠ user_id debe ser numérico.")

        elif opcion == "8":
            cas.show_duplicados_global(session, limit=100)

        # --- MONGO DB ---
        elif opcion == "4":
            ejecutar("MongoDB", 5, "Mapa global de saldos y usuarios")
        elif opcion == "5":
            ejecutar("MongoDB", 10, "Auditoría de cuentas nuevas (alto riesgo)")

        # --- DGRAPH ---
        elif opcion == "6":
            # Reutilizamos el query de risk scoring, pidiendo un usuario
            print("   Análisis de propagación de riesgo (Network Risk).")
            uid_input = input("   Ingrese ID de usuario semilla (ej: 3003): ").strip()
            if uid_input:
                dg_qry.query_risk_scoring(client, uid_input)
            else:
                print("   ⚠ ID requerido.")

        elif opcion == "7":
             # Mapa de calor geográfico
             print("   Configuración de búsqueda Geo (Default: CDMX)")
             lat = input("   Latitud (default 19.4): ").strip() or "19.4"
             lon = input("   Longitud (default -99.1): ").strip() or "-99.1"
             rad = input("   Radio en KM (default 50): ").strip() or "50"

             try:
                dg_qry.query_geo_heatmap(client, float(lat), float(lon), float(rad))
             except ValueError:
                 print("   Error: Las coordenadas deben ser números.")

        elif opcion == "0":
            break
        else:
            print("Comando no reconocido.")


# =====================================================================
# MENÚ PRINCIPAL
# =====================================================================
def main():
    # 1. Conexión Dgraph
    try:
        client_stub = cn.create_client_stub()
        client = cn.create_client(client_stub)
        print("🔌 Dgraph conectado.")
    except Exception as e:
        print(f"❌ Error conectando a Dgraph: {e}")
        return

    # 2. Conexión Cassandra
    cluster = None
    session = None
    try:
        cluster, session = get_cassandra_session()
        print("🔌 Cassandra conectado.")
    except Exception as e:
        print("⚠ No se pudo conectar a Cassandra o al keyspace.")
        print("   (Si es la primera vez, usa la opción 4 -> 1 para poblar)")
        print(f"   Detalle: {e}")

    while True:
        print("\n\n############################################################")
        print("      🕵️  SISTEMA DETECCION FRAUDES ITESOBANK  🕵️")
        print("############################################################")
        print("1. 🔍 INVESTIGACIÓN INDIVIDUAL (Targeting)")
        print("2. 🛡️ MONITOR DE AMENAZAS (Global Monitoring)")
        print("3. 📊 ANALÍTICA FORENSE (Reports)")
        print("4. ⚙️  CONFIGURACIÓN Y DATOS")
        print("0. SALIR")

        opcion = input("\nSeleccione operación: ").strip()

        if opcion == "1":
            if session:
                menu_investigacion_cliente(session, client)
            else:
                print("❌ Cassandra no disponible.")

        elif opcion == "2":
            if session:
                menu_monitor_amenazas(session, client)
            else:
                print("❌ Cassandra no disponible.")

        elif opcion == "3":
            if session:
                menu_analitica_forense(session, client)
            else:
                print("❌ Cassandra no disponible.")

        elif opcion == "4":
            print("\n[⚙️ MODO ADMINISTRADOR]")
            print("1. Poblar Cassandra y Dgraph (Carga Inicial)")
            print("2. DROP ALL DATA (Simulación)")
            sub_op = input(">> ").strip()

            if sub_op == "1":
                print("\n🚀 Iniciando población de Cassandra...")
                try:
                    populate_cassandra()
                except Exception as e:
                    print(f"Error en Cassandra: {e}")

                print("\n🚀 Iniciando población de Dgraph...")
                try:
                    populate_dgraph() # Ya tiene su propia gestión de conexión interna si usas el código anterior
                except Exception as e:
                    print(f"Error en Dgraph: {e}")

                print("\n✅ Procesos de carga finalizados.")

                # Intentar reconectar Cassandra si estaba caído
                if session is None:
                    try:
                        cluster, session = get_cassandra_session()
                        print("🔌 Conectado a Cassandra tras la carga.")
                    except: pass

            elif sub_op == "2":
                print("⚠️ (Simulación) Eliminando registros...")
                time.sleep(1)
                print("Sistema reseteado (simulado).")

        elif opcion == "0":
            print("Cerrando conexiones...")
            cn.close_client_stub(client_stub)
            if cluster:
                cluster.shutdown()
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    main()
