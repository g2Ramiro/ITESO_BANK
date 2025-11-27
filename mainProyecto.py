import time

# --- Simulador de Ejecución (Mocking) ---
def ejecutar(db, req_id, descripcion, contexto=None):
    if contexto:
        print(f"\n[🔍 INVESTIGANDO OBJETIVO EN {db.upper()} -> '{contexto}']")
        print(f"   └── Ejecutando protocolo #{req_id}: {descripcion}...")
    else:
        print(f"\n[📡 ESCANEO GLOBAL EN {db.upper()}]")
        print(f"   └── Ejecutando protocolo #{req_id}: {descripcion}...")
    
    # Simulación de carga
    time.sleep(0.6)
    print("   ✅ Análisis completado. Datos recuperados.\n")

# ==============================================================================
# 1. INVESTIGACIÓN INDIVIDUAL (Targeted Investigation)
# ==============================================================================
def menu_investigacion_cliente():
    print("\n============== 🕵️ INVESTIGACIÓN DE OBJETIVO (CLIENTE) ==============")
    print("Ingrese el ID o Nombre del cliente a investigar:")
    cliente_id = input(">> ")
    
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
        
        print("   --- 💸 Análisis Transaccional ---")
        print("   4. Historial de movimientos (Cassandra #1)")
        print("   5. Flujo de dinero entrante (Cassandra #10)")
        print("   6. Transferencias internas (Posible Pitufeo) (Cassandra #4)")
        print("   7. Estado de transacciones en curso (Cassandra #12)")
        
        print("   --- ⚠️ Evaluación de Riesgo ---")
        print("   8. Calcular Risk Score del sujeto (Mongo #12)")
        print("   9. Mapa de conexiones sospechosas (Dgraph #6)")
        
        print("   0. 🔙 Abortar investigación / Nuevo objetivo")

        opcion = input("   >> ")

        if opcion == "1": ejecutar("MongoDB", 3, "Información de Cuentas", cliente_id)
        elif opcion == "2": ejecutar("MongoDB", 8, "Dispositivos por usuario", cliente_id)
        elif opcion == "3": ejecutar("MongoDB", 2, "Inicio de Sesión", cliente_id)
        elif opcion == "4": ejecutar("Cassandra", 1, "Historial transaccional", cliente_id)
        elif opcion == "5": ejecutar("Cassandra", 10, "Transacciones recibidas", cliente_id)
        elif opcion == "6": ejecutar("Cassandra", 4, "Transferencias internas", cliente_id)
        elif opcion == "7": ejecutar("Cassandra", 12, "Seguimiento estado transacciones", cliente_id)
        elif opcion == "8": ejecutar("MongoDB", 12, "Perfil de riesgo usuario", cliente_id)
        elif opcion == "9": ejecutar("Dgraph", 6, "Scoring de conexiones", cliente_id)
        elif opcion == "0": break
        else: print("Comando no reconocido.")

# ==============================================================================
# 2. MONITOR DE AMENAZAS GLOBAL (Global Threat Monitor)
# ==============================================================================
def menu_monitor_amenazas():
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
        
        opcion = input("   >> ")

        if opcion == "1": ejecutar("Cassandra", 8, "Transacciones fuera de rango")
        elif opcion == "2": ejecutar("Cassandra", 9, "Intentos rechazados")
        elif opcion == "3": ejecutar("MongoDB", 11, "Cambios masivos IP/Disp")
        elif opcion == "4": ejecutar("Dgraph", 1, "Colaboración fraudulenta")
        elif opcion == "5": ejecutar("Dgraph", 3, "Lavado de dinero")
        elif opcion == "6": ejecutar("Dgraph", 7, "Cuentas fantasmas")
        elif opcion == "7": ejecutar("Dgraph", 8, "Suplantación de identidad")
        elif opcion == "8": ejecutar("Dgraph", 9, "Rutas sospechosas")
        elif opcion == "9": ejecutar("MongoDB", 6, "Cuentas Flageadas")
        elif opcion == "10": ejecutar("MongoDB", 7, "Comportamiento errático")
        elif opcion == "0": break

# ==============================================================================
# 3. INTELIGENCIA DE NEGOCIO (Forensic Analytics)
# ==============================================================================
def menu_analitica_forense():
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
        
        opcion = input("   >> ")

        reqs = {
            "1": ("Cassandra", 5), "2": ("Cassandra", 3), "3": ("Cassandra", 2),
            "4": ("MongoDB", 5), "5": ("MongoDB", 10), "6": ("Dgraph", 10),
            "7": ("Dgraph", 4), "8": ("Cassandra", 11)
        }
        
        if opcion in reqs:
            ejecutar(reqs[opcion][0], reqs[opcion][1], "Reporte Forense")
        elif opcion == "0": break
        else: print("Comando no reconocido.")

# ==============================================================================
# MENÚ PRINCIPAL
# ==============================================================================
def main():
    while True:
        print("\n\n############################################################")
        print("      🕵️  SISTEMA DETECCION FRAUDES ITESOBANK  🕵️")
        print("############################################################")
        print("1. 🔍 INVESTIGACIÓN INDIVIDUAL (Targeting)")
        print("   >> Analizar cliente específico, riesgo, historial y grafos.")
        
        print("2. 🛡️ MONITOR DE AMENAZAS (Global Monitoring)")
        print("   >> Alertas en tiempo real, anillos de fraude, lavado de dinero.")
        
        print("3. 📊 ANALÍTICA FORENSE (Reports)")
        print("   >> Rankings, estadísticas globales y auditoría.")
        
        print("4. ⚙️  CONFIGURACIÓN Y DATOS")
        print("   >> [Admin] Poblar DBs, Limpieza de datos.")
        
        print("0. SALIR")
        
        opcion = input("\nSeleccione operación: ")

        if opcion == "1":
            menu_investigacion_cliente()
        elif opcion == "2":
            menu_monitor_amenazas()
        elif opcion == "3":
            menu_analitica_forense()
        elif opcion == "4":
            print("\n[⚙️ MODO ADMINISTRADOR]")
            print("1. Poblar Bases de Datos")
            print("2. DROP ALL DATA (⚠️ Danger)")
            sub_op = input(">> ")
            if sub_op == "1":
                print("Iniciando script de población...")
                time.sleep(1)
                print("Bases de datos actualizadas.")
            elif sub_op == "2":
                print("Eliminando registros...")
                time.sleep(1)
                print("Sistema reseteado.")
        elif opcion == "0":
            print("Cerrando conexión segura...")
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    main()