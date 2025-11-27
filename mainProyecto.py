#Cassandra
def cassandra_menu():
    while True:
        print("\n=== Cassandra Requirements ===")
        print("1. Registro de transacciones e historial por usuario")
        print("2. Transacciones con mayor cantidad por usuario")
        print("3. Cuentas de usuarios con mayor número de transacciones")
        print("4. Transferencias entre cuentas del mismo usuario")
        print("5. Ranking de cuentas por actividad (periodo)")
        print("6. Transacciones recientes en tiempo real")
        print("7. Historial de alertas de fraude")
        print("8. Transacciones fuera de rango")
        print("9. Intentos de transacción rechazados")
        print("10. Transacciones recibidas por usuario")
        print("11. Transacciones duplicadas")
        print("12. Seguimiento de cambios en estado de transacciones")
        print("0. Regresar al menú principal")

        opcion = input("Selecciona una opción: ")

        if opcion == "0":
            break
        else:
            print(f"\n[+] Ejecutando requerimiento Cassandra #{opcion}...\n")

#MongoDB
def mongodb_menu():
    while True:
        print("\n=== MongoDB Requirements ===")
        print("1. Registro de Usuarios")
        print("2. Inicio de Sesión y Dispositivos")
        print("3. Información de Cuentas")
        print("4. Registro de Metadatos de Transacciones")
        print("5. Vista de usuarios con cuentas y saldos")
        print("6. Monitoreo de Cuentas flageadas")
        print("7. Cuentas con cambios frecuentes de estatus")
        print("8. Dispositivos por usuario")
        print("9. Búsqueda flexible de cuentas")
        print("10. Análisis de cuentas nuevas con alto riesgo")
        print("11. Detección de cambios de IP/dispositivo")
        print("12. Perfil de riesgo por usuario")
        print("0. Regresar al menú principal")

        opcion = input("Selecciona una opción: ")

        if opcion == "0":
            break
        else:
            print(f"\n[+] Ejecutando requerimiento MongoDB #{opcion}...\n")

#dgraph
def dgraph_menu():
    while True:
        print("\n=== Dgraph Requirements ===")
        print("1. Detección de colaboración fraudulenta")
        print("2. Patrones de comportamiento sospechoso")
        print("3. Detección de lavado de dinero")
        print("4. Análisis por ubicación de dispositivos")
        print("5. Integración con alertas en tiempo real")
        print("6. Scoring de riesgo basado en conexiones")
        print("7. Detección de cuentas fantasmas")
        print("8. Detección de suplantación de identidad")
        print("9. Detección de rutas sospechosas")
        print("10. Propagación de riesgo")
        print("11. Detección de dispositivos/IPs múltiples")
        print("0. Regresar al menú principal")

        opcion = input("Selecciona una opción: ")

        if opcion == "0":
            break
        else:
            print(f"\n[+] Ejecutando requerimiento Dgraph #{opcion}...\n")


#Menu principal
def mostrar_menu_principal():
    print("\n=== Welcome To ItesoBank ===")
    print("1. Populate Data Bases")
    print("2. Requirements MongoDB")
    print("3. Requirements Cassandra")
    print("4. Requirements Dgraph")
    print("5. Drop All Data")
    print("0. Exit")


def main():
    while True:
        mostrar_menu_principal()
        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            print("\n[+] Ejecutando: Populate Data Bases...\n")

        elif opcion == "2":
            mongodb_menu()

        elif opcion == "3":
            cassandra_menu()

        elif opcion == "4":
            dgraph_menu()

        elif opcion == "5":
            print("\n[!] Ejecutando: Drop ALL Data (Precaución)...\n")

        elif opcion == "0":
            print("\nSaliendo... Gracias por usar ItesoBank.")
            break

        else:
            print("\n[!] Opción inválida, intenta de nuevo.")


main()
