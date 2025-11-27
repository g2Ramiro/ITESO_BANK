# Cassandra/utils.py

def print_table(rows, columns, max_rows=50, title=None):
    """
    Imprime una tabla formateada en consola.
    - rows: iterable de filas devueltas por Cassandra
    - columns: lista de nombres de columnas a mostrar
    - title: título opcional
    """
    rows = list(rows)

    if title:
        print(f"\n{title}")

    if not rows:
        print("   (sin resultados)")
        return

    # Convertir filas a texto
    str_rows = []
    for r in rows[:max_rows]:
        fila = []
        for col in columns:
            try:
                val = getattr(r, col)
            except AttributeError:
                try:
                    val = r[col]
                except Exception:
                    val = ""
            if val is None:
                val = ""
            fila.append(str(val))
        str_rows.append(fila)

    # Calcular anchos de columna
    widths = []
    for i, col in enumerate(columns):
        max_len = len(col)
        for fila in str_rows:
            if i < len(fila):
                max_len = max(max_len, len(fila[i]))
        widths.append(max_len)

    # Formateador de filas
    def fmt_row(values):
        return " | ".join(v.ljust(widths[i]) for i, v in enumerate(values))

    # Imprimir encabezado
    header = fmt_row(columns)
    sep = "-+-".join("-" * w for w in widths)

    print("   " + header)
    print("   " + sep)

    # Imprimir filas
    for fila in str_rows:
        print("   " + fmt_row(fila))

    if len(rows) > max_rows:
        print(f"\n   ... mostrando solo las primeras {max_rows} filas ...")
