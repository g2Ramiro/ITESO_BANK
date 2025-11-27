import time
from cassandra.cluster import Cluster
from connect import CLUSTER_IPS, KEYSPACE
from populate import populate_cassandra
from Cassandra import model as cas


# Utilidad para conectar a Cassandra
def get_cassandra_session():
    ips = [ip.strip() for ip in CLUSTER_IPS.split(",") if ip.strip()]
    cluster = Cluster(ips)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    return cluster, session


# =====================================================================
# 1. INVESTIGACIÓN INDIVIDUAL
# =====================================================================
def menu_investigacion_cliente(session):
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

        # Mongo / Dgraph simulados
        if opcion == "1":
            ejecutar("MongoDB", 3, "Información de Cuentas", cliente_id)
        elif opcion == "2":
            ejecutar("MongoDB", 8, "Dispositivos por usuario", cliente_id)
        elif opcion == "3":
            ejecutar("MongoDB", 2, "Inicio de Sesión", cliente_id)
        elif opcion == "8":
            ejecutar("MongoDB", 12, "Perfil de riesgo usuario", cliente_id)
        elif opcion == "9":
            ejecutar("Dgraph", 6, "Scoring de conexiones", cliente_id)

        # Cassandra reales: usamos las show_* del módulo cas
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
def menu_monitor_amenazas(session):
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

        if opcion == "1":
            cas.show_transacciones_fuera_de_rango_global(session, limit=100)
        elif opcion == "2":
            cas.show_intentos_rechazados_global(session, limit=100)
        elif opcion == "3":
            ejecutar("MongoDB", 11, "Cambios masivos IP/Disp")
        elif opcion == "4":
            ejecutar("Dgraph", 1, "Colaboración fraudulenta")
        elif opcion == "5":
            ejecutar("Dgraph", 3, "Lavado de dinero")
        elif opcion == "6":
            ejecutar("Dgraph", 7, "Cuentas fantasmas")
        elif opcion == "7":
            ejecutar("Dgraph", 8, "Suplantación de identidad")
        elif opcion == "8":
            ejecutar("Dgraph", 9, "Rutas sospechosas")
        elif opcion == "9":
            ejecutar("MongoDB", 6, "Cuentas Flageadas")
        elif opcion == "10":
            ejecutar("MongoDB", 7, "Comportamiento errático")
        elif opcion == "0":
            break
        else:
            print("Comando no reconocido.")


# =====================================================================
# 3. ANALÍTICA FORENSE
# =====================================================================
def menu_analitica_forense(session):
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

        if opcion == "1":
            cas.show_top_cuentas_global(session, limit=20)

        elif opcion == "2":
            uid_raw = input("   Ingrese user_id para analizar su frecuencia: ").strip()
            try:
                uid = int(uid_raw)
            except ValueError:
                print("   ⚠ user_id debe ser numérico.")
                continue
            cas.show_cuentas_por_usuario(session, uid)

        elif opcion == "3":
            uid_raw = input("   Ingrese user_id para ver sus operaciones de mayor monto: ").strip()
            try:
                uid = int(uid_raw)
            except ValueError:
                print("   ⚠ user_id debe ser numérico.")
                continue
            cas.show_top_operaciones_usuario(session, uid, limit=20)

        elif opcion == "4":
            ejecutar("MongoDB", 5, "Mapa global de saldos y usuarios")
        elif opcion == "5":
            ejecutar("MongoDB", 10, "Auditoría de cuentas nuevas (alto riesgo)")
        elif opcion == "6":
            ejecutar("Dgraph", 10, "Análisis de propagación de riesgo")
        elif opcion == "7":
            ejecutar("Dgraph", 4, "Mapa de calor geográfico")

        elif opcion == "8":
            cas.show_duplicados_global(session, limit=100)

        elif opcion == "0":
            break
        else:
            print("Comando no reconocido.")


# =====================================================================
# MENÚ PRINCIPAL
# =====================================================================
def main():
    try:
        cluster, session = get_cassandra_session()
    except Exception as e:
        print("⚠ No se pudo conectar a Cassandra o al keyspace.")
        print("   Detalle:", e)
        print("   Puedes usar la opción 4.1 (Poblar Cassandra) primero.")
        cluster = None
        session = None

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
            if session is None:
                print("⚠ Cassandra no está disponible. Conéctate o popula primero.")
            else:
                menu_investigacion_cliente(session)

        elif opcion == "2":
            if session is None:
                print("⚠ Cassandra no está disponible. Conéctate o popula primero.")
            else:
                menu_monitor_amenazas(session)

        elif opcion == "3":
            if session is None:
                print("⚠ Cassandra no está disponible. Conéctate o popula primero.")
            else:
                menu_analitica_forense(session)

        elif opcion == "4":
            print("\n[⚙️ MODO ADMINISTRADOR]")
            print("1. Poblar Cassandra (CSV → Tablas)")
            print("2. DROP ALL DATA (⚠️ solo simulación)")
            sub_op = input(">> ").strip()

            if sub_op == "1":
                print("\n🚀 Iniciando población de Cassandra...\n")
                populate_cassandra()
                print("\n🌱 Carga de datos finalizada.\n")

                # Re-conectar
                try:
                    cluster, session = get_cassandra_session()
                    print("🔌 Conectado a Cassandra y keyspace listo.")
                except Exception as e:
                    print("⚠ No se pudo reconectar a Cassandra después de poblar.")
                    print("   Detalle:", e)
                    cluster = None
                    session = None

            elif sub_op == "2":
                print("⚠️ (Simulación) Eliminando registros de todas las DBs...")
                time.sleep(1)
                print("Sistema reseteado (simulado).")

        elif opcion == "0":
            print("Cerrando conexión segura...")
            break
        else:
            print("Opción inválida.")

    if cluster is not None:
        cluster.shutdown()


if __name__ == "__main__":
    main()
