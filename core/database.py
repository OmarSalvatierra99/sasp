# ===========================================================
# core/database.py — SCIL 2025
# Manejador central de base de datos SQLite
# ===========================================================

import sqlite3
import json
import hashlib
from pathlib import Path
from collections import defaultdict


class DatabaseManager:
    def __init__(self, db_path="scil.db"):
        self.db_path = db_path
        print(f"📂 Base de datos en uso: {Path(self.db_path).resolve()}")
        self._init_db()

    # -------------------------------------------------------
    # Conexión
    # -------------------------------------------------------
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # -------------------------------------------------------
    # Inicialización de tablas
    # -------------------------------------------------------
    def _init_db(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS laboral (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_analisis TEXT NOT NULL,
                rfc TEXT NOT NULL,
                datos TEXT NOT NULL,
                hash_firma TEXT UNIQUE,
                fecha_analisis TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS registros_laborales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                ente TEXT NOT NULL,
                nombre TEXT NOT NULL,
                puesto TEXT,
                fecha_ingreso TEXT,
                fecha_egreso TEXT,
                monto REAL,
                qnas TEXT NOT NULL,
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(rfc, ente)
            );

            CREATE TABLE IF NOT EXISTS solventaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                ente TEXT NOT NULL,
                estado TEXT NOT NULL,
                comentario TEXT,
                catalogo TEXT,
                otro_texto TEXT,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(rfc, ente)
            );

            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                usuario TEXT UNIQUE NOT NULL,
                clave TEXT NOT NULL,
                entes TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS entes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                num TEXT NOT NULL,
                clave TEXT UNIQUE NOT NULL,
                nombre TEXT NOT NULL,
                siglas TEXT,
                clasificacion TEXT,
                ambito TEXT,
                activo INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS municipios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                num TEXT NOT NULL,
                clave TEXT UNIQUE NOT NULL,
                nombre TEXT NOT NULL,
                siglas TEXT,
                clasificacion TEXT,
                ambito TEXT DEFAULT 'MUNICIPAL',
                activo INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

        # Migrar columnas nuevas en solventaciones si no existen
        self._migrate_solventaciones_columns(cur)

        conn.commit()
        conn.close()
        print(f"✅ Tablas listas en {self.db_path}")

    def _migrate_solventaciones_columns(self, cur):
        """Agrega columnas catalogo y otro_texto a solventaciones si no existen"""
        # Verificar si las columnas ya existen
        cur.execute("PRAGMA table_info(solventaciones)")
        columns = [row[1] for row in cur.fetchall()]

        if 'catalogo' not in columns:
            cur.execute("ALTER TABLE solventaciones ADD COLUMN catalogo TEXT")
            print("  ↳ Columna 'catalogo' agregada a solventaciones")

        if 'otro_texto' not in columns:
            cur.execute("ALTER TABLE solventaciones ADD COLUMN otro_texto TEXT")
            print("  ↳ Columna 'otro_texto' agregada a solventaciones")

    # -------------------------------------------------------
    # Poblar datos base
    # -------------------------------------------------------
    def poblar_datos_iniciales(self):
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM usuarios")
        if cur.fetchone()[0] == 0:
            base = [
                ("C.P. Odilia Cuamatzi Bautista", "odilia",
                 hashlib.sha256("odilia2025".encode()).hexdigest(), "TODOS"),
                ("C.P. Luis Felipe Camilo Fuentes", "felipe",
                 hashlib.sha256("felipe2025".encode()).hexdigest(), "TODOS"),
            ]
            cur.executemany(
                "INSERT INTO usuarios (nombre, usuario, clave, entes) VALUES (?, ?, ?, ?)", base)
            print("👥 Usuarios base insertados")

        cur.execute("SELECT COUNT(*) FROM entes")
        if cur.fetchone()[0] == 0:
            entes = [
                ("1.2", "ENTE_1_2", "Secretaría de Gobierno", "SEGOB", "Dependencia", "Estatal"),
                ("1.4", "ENTE_1_4", "Secretaría de Finanzas", "SEFIN", "Dependencia", "Estatal"),
                ("1.8", "ENTE_1_8", "Secretaría de Educación Pública", "SEPE", "Dependencia", "Estatal"),
            ]
            cur.executemany(
                "INSERT INTO entes (num, clave, nombre, siglas, clasificacion, ambito) VALUES (?,?,?,?,?,?)", entes)
            print("🏛️ Entes base insertados")

        conn.commit()
        conn.close()

    # -------------------------------------------------------
    # Catálogos
    # -------------------------------------------------------
    def listar_entes(self, solo_activos=True):
        """Lista entes ordenados por NUM (respeta el orden institucional jerárquico)."""
        conn = self._connect()
        cur = conn.cursor()
        q = "SELECT num, clave, nombre, siglas, clasificacion, ambito FROM entes"
        if solo_activos:
            q += " WHERE activo=1"
        cur.execute(q)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()

        # Función de ordenamiento jerárquico para números tipo 1.2.3
        def orden_jerarquico(item):
            num_str = item['num'].strip().rstrip('.')
            partes = []
            for parte in num_str.split('.'):
                try:
                    partes.append(int(parte))
                except ValueError:
                    partes.append(0)
            while len(partes) < 5:
                partes.append(0)
            return tuple(partes)

        data.sort(key=orden_jerarquico)
        return data

    def listar_municipios(self):
        """Lista municipios ordenados por NUM (respeta el orden institucional)."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT num, clave, nombre, siglas, clasificacion, ambito
            FROM municipios
            WHERE activo=1
            ORDER BY CAST(num AS INTEGER), num
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    # -------------------------------------------------------
    # Mapas rápidos de entes
    # -------------------------------------------------------
    def get_mapa_siglas(self):
        """Genera diccionario {SIGLA_NORMALIZADA: CLAVE_ENTE}."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT siglas, clave FROM entes WHERE activo=1
            UNION ALL
            SELECT siglas, clave FROM municipios WHERE activo=1
        """)
        mapa = {self._sanitize(sigla): clave for sigla, clave in cur.fetchall() if sigla}
        conn.close()
        return mapa

    def get_mapa_claves_inverso(self):
        """Genera diccionario {CLAVE_ENTE: SIGLA_O_NOMBRE}."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT clave, siglas, nombre FROM entes WHERE activo=1
            UNION ALL
            SELECT clave, siglas, nombre FROM municipios WHERE activo=1
        """)
        mapa = {}
        for clave, sigla, nombre in cur.fetchall():
            display = sigla if sigla else nombre
            mapa[self._sanitize(clave)] = self._sanitize(display)
        conn.close()
        return mapa

    # -------------------------------------------------------
    # Utilidades
    # -------------------------------------------------------
    def _hash_text(self, text):
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _sanitize(self, s):
        if not s:
            return ""
        s = str(s).strip().upper()
        for a, b in zip("ÁÉÍÓÚ", "AEIOU"):
            s = s.replace(a, b)
        return s

    # -------------------------------------------------------
    # Normalización de entes
    # -------------------------------------------------------
    def normalizar_ente(self, valor):
        """
        Busca un ente o municipio por sigla, clave o nombre y devuelve el NOMBRE completo.
        Útil para mostrar el nombre oficial en reportes.
        """
        if not valor:
            return None
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT nombre FROM (
                SELECT nombre, siglas, clave FROM entes WHERE activo=1
                UNION ALL
                SELECT nombre, siglas, clave FROM municipios WHERE activo=1
            )
            WHERE UPPER(siglas)=UPPER(?) OR UPPER(clave)=UPPER(?) OR UPPER(nombre)=UPPER(?)
            LIMIT 1
        """, (valor, valor, valor))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None

    def normalizar_ente_clave(self, valor):
        """
        Busca un ente o municipio por sigla, clave o nombre y devuelve la CLAVE única.
        Útil para operaciones de base de datos y referencias internas.
        """
        if not valor:
            return None
        val = self._sanitize(valor)
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT clave FROM (
                SELECT clave, siglas, nombre FROM entes WHERE activo=1
                UNION ALL
                SELECT clave, siglas, nombre FROM municipios WHERE activo=1
            )
            WHERE UPPER(siglas)=? OR UPPER(nombre)=? OR UPPER(clave)=?
            LIMIT 1
        """, (val, val, val))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None

    # -------------------------------------------------------
    # Resultados laborales
    # -------------------------------------------------------
    def comparar_con_historico(self, nuevos):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT hash_firma FROM laboral")
        existentes = {r[0] for r in cur.fetchall() if r[0]}
        conn.close()

        nuevos_validos, repetidos = [], []
        for r in nuevos:
            texto = json.dumps(r, sort_keys=True, ensure_ascii=False)
            h = self._hash_text(texto)
            if h not in existentes:
                r["hash_firma"] = h
                nuevos_validos.append(r)
            else:
                repetidos.append(r)
        return nuevos_validos, repetidos, len(repetidos)

    def guardar_registros_individuales(self, registros):
        """
        Guarda o actualiza registros individuales por RFC+ENTE.
        Si ya existe el registro, lo actualiza. Si es nuevo, lo inserta.

        Args:
            registros: Lista de diccionarios con datos de empleados por ente

        Returns:
            (insertados, actualizados)
        """
        if not registros:
            return 0, 0

        conn = self._connect()
        cur = conn.cursor()
        insertados, actualizados = 0, 0

        for reg in registros:
            rfc = reg.get("rfc", "")
            ente = reg.get("ente", "")

            if not rfc or not ente:
                continue

            # Serializar QNAs a JSON
            qnas_json = json.dumps(reg.get("qnas", {}), ensure_ascii=False)

            try:
                # Intentar insertar o actualizar usando ON CONFLICT
                cur.execute("""
                    INSERT INTO registros_laborales
                    (rfc, ente, nombre, puesto, fecha_ingreso, fecha_egreso, monto, qnas)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(rfc, ente) DO UPDATE SET
                        nombre = excluded.nombre,
                        puesto = excluded.puesto,
                        fecha_ingreso = excluded.fecha_ingreso,
                        fecha_egreso = excluded.fecha_egreso,
                        monto = excluded.monto,
                        qnas = excluded.qnas,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                """, (
                    rfc,
                    ente,
                    reg.get("nombre", ""),
                    reg.get("puesto", ""),
                    reg.get("fecha_ingreso"),
                    reg.get("fecha_egreso"),
                    reg.get("monto"),
                    qnas_json
                ))

                # Verificar si fue INSERT o UPDATE
                if cur.rowcount > 0:
                    # Verificar si ya existía
                    cur.execute("""
                        SELECT COUNT(*) FROM registros_laborales
                        WHERE rfc=? AND ente=? AND fecha_carga < fecha_actualizacion
                    """, (rfc, ente))
                    if cur.fetchone()[0] > 0:
                        actualizados += 1
                    else:
                        insertados += 1

            except Exception as e:
                print(f"⚠️  Error guardando RFC={rfc}, ENTE={ente}: {e}")
                continue

        conn.commit()
        conn.close()
        return insertados, actualizados

    def contar_trabajadores_por_ente(self):
        """
        Cuenta el total de trabajadores (RFCs únicos) por ente.

        Returns:
            dict: {ente_clave: cantidad_de_rfc}
        """
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            SELECT ente, COUNT(DISTINCT rfc) as total
            FROM registros_laborales
            GROUP BY ente
        """)

        resultado = {}
        for row in cur.fetchall():
            resultado[row["ente"]] = row["total"]

        conn.close()
        return resultado

    def obtener_cruces_reales(self):
        """
        Detecta empleados que están activos en más de un ente durante la misma QNA.

        Returns:
            Lista de diccionarios con información de cruces detectados
        """
        conn = self._connect()
        cur = conn.cursor()

        # Obtener todos los registros
        cur.execute("""
            SELECT rfc, ente, nombre, puesto, fecha_ingreso, fecha_egreso, monto, qnas
            FROM registros_laborales
            ORDER BY rfc, ente
        """)

        registros = []
        for row in cur.fetchall():
            registros.append({
                "rfc": row["rfc"],
                "ente": row["ente"],
                "nombre": row["nombre"],
                "puesto": row["puesto"],
                "fecha_ingreso": row["fecha_ingreso"],
                "fecha_egreso": row["fecha_egreso"],
                "monto": row["monto"],
                "qnas": json.loads(row["qnas"])
            })

        conn.close()

        # Agrupar por RFC
        rfcs_map = defaultdict(list)
        for reg in registros:
            rfcs_map[reg["rfc"]].append(reg)

        # Detectar cruces reales
        cruces = []
        for rfc, regs in rfcs_map.items():
            if len(regs) < 2:
                continue

            # Verificar si hay cruces de QNAs entre diferentes entes
            qnas_por_ente = {}
            for reg in regs:
                qnas_activas = set(reg["qnas"].keys())
                qnas_por_ente[reg["ente"]] = qnas_activas

            # Buscar intersecciones
            entes_list = list(qnas_por_ente.keys())
            qnas_con_cruce = set()
            entes_con_cruce = set()

            for i in range(len(entes_list)):
                for j in range(i + 1, len(entes_list)):
                    e1, e2 = entes_list[i], entes_list[j]
                    interseccion = qnas_por_ente[e1].intersection(qnas_por_ente[e2])
                    if interseccion:
                        qnas_con_cruce.update(interseccion)
                        entes_con_cruce.update([e1, e2])

            # Si hay cruce real, agregarlo
            if qnas_con_cruce:
                cruces.append({
                    "rfc": rfc,
                    "nombre": regs[0]["nombre"],
                    "entes": sorted(list(entes_con_cruce)),
                    "qnas_cruce": sorted(list(qnas_con_cruce)),
                    "tipo_patron": "CRUCE_ENTRE_ENTES_QNA",
                    "descripcion": f"Activo en {len(entes_con_cruce)} entes durante {len(qnas_con_cruce)} quincena(s) simultáneas.",
                    "registros": regs,
                    "estado": "Sin valoración",
                    "solventacion": ""
                })

        return cruces

    def guardar_resultados(self, resultados):
        if not resultados:
            return 0, 0
        conn = self._connect()
        cur = conn.cursor()
        nuevos, duplicados = 0, 0
        for r in resultados:
            texto = json.dumps(r, ensure_ascii=False, sort_keys=True)
            h = self._hash_text(texto)
            try:
                cur.execute("""
                    INSERT INTO laboral (tipo_analisis, rfc, datos, hash_firma)
                    VALUES (?, ?, ?, ?)
                """, (r.get("tipo_patron", "GENERAL"), r.get("rfc", ""), texto, h))
                nuevos += 1
            except sqlite3.IntegrityError:
                duplicados += 1
        conn.commit()
        conn.close()
        return nuevos, duplicados

    def obtener_resultados_paginados(self, tabla="laboral", filtro=None, pagina=1, limite=10000):
        conn = self._connect()
        cur = conn.cursor()
        offset = (pagina - 1) * limite
        if filtro:
            cur.execute(
                f"SELECT datos FROM {tabla} WHERE datos LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (f"%{filtro}%", limite, offset))
        else:
            cur.execute(
                f"SELECT datos FROM {tabla} ORDER BY id DESC LIMIT ? OFFSET ?",
                (limite, offset))
        rows = cur.fetchall()
        conn.close()

        resultados = []
        for row in rows:
            try:
                resultados.append(json.loads(row[0]))
            except Exception:
                continue
        return resultados, len(resultados)

    def obtener_resultados_por_rfc(self, rfc):
        """
        Obtiene todos los registros de un RFC específico desde la tabla de registros individuales.

        Returns:
            dict con información consolidada del RFC o None si no existe
        """
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            SELECT rfc, ente, nombre, puesto, fecha_ingreso, fecha_egreso, monto, qnas
            FROM registros_laborales
            WHERE UPPER(rfc) = UPPER(?)
            ORDER BY ente
        """, (rfc,))

        rows = cur.fetchall()
        conn.close()

        if not rows:
            return None

        registros = []
        entes = set()
        nombre = ""

        for row in rows:
            nombre = row["nombre"]  # Tomar el nombre (debería ser el mismo en todos)
            entes.add(row["ente"])
            registros.append({
                "ente": row["ente"],
                "puesto": row["puesto"],
                "fecha_ingreso": row["fecha_ingreso"],
                "fecha_egreso": row["fecha_egreso"],
                "monto": row["monto"],
                "qnas": json.loads(row["qnas"])
            })

        return {
            "rfc": rfc,
            "nombre": nombre,
            "entes": sorted(list(entes)),
            "registros": registros,
            "estado": "Sin valoración",  # Se actualiza desde solventaciones
            "solventacion": ""
        }

    # -------------------------------------------------------
    # Solventaciones
    # -------------------------------------------------------
    def get_solventaciones_por_rfc(self, rfc):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT ente, estado, comentario FROM solventaciones WHERE rfc=?", (rfc,))
        data = {}
        for row in cur.fetchall():
            data[row["ente"]] = {
                "estado": row["estado"],
                "comentario": row["comentario"]
            }
        conn.close()
        return data

    def actualizar_solventacion(self, rfc, estado, comentario, catalogo=None, otro_texto=None, ente="GENERAL"):
        if not ente:
            ente = "GENERAL"
        ente = self.normalizar_ente_clave(ente) or ente
        if not estado:
            estado = "Sin valoración"

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO solventaciones (rfc, ente, estado, comentario, catalogo, otro_texto)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(rfc, ente) DO UPDATE SET
                estado=excluded.estado,
                comentario=excluded.comentario,
                catalogo=excluded.catalogo,
                otro_texto=excluded.otro_texto,
                actualizado=CURRENT_TIMESTAMP
        """, (rfc, ente, estado, comentario, catalogo, otro_texto))
        filas = cur.rowcount
        conn.commit()
        conn.close()
        return filas

    def get_estado_rfc_ente(self, rfc, ente_clave):
        if not rfc or not ente_clave:
            return None
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT estado FROM solventaciones
            WHERE rfc = ? AND ente = ?
            ORDER BY actualizado DESC
            LIMIT 1
        """, (rfc, ente_clave))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None

    def get_usuario(self, usuario, clave):
        if not usuario or not clave:
            return None
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT nombre, usuario, clave, entes
            FROM usuarios
            WHERE LOWER(usuario)=LOWER(?)
            LIMIT 1
        """, (usuario,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None

        clave_hash = hashlib.sha256(clave.encode()).hexdigest()
        if clave_hash != row["clave"]:
            return None

        entes = [e.strip().upper() for e in (row["entes"] or "").split(",") if e.strip()]
        return {
            "nombre": row["nombre"],
            "usuario": row["usuario"],
            "entes": entes
        }


# -----------------------------------------------------------
# Ejecución directa
# -----------------------------------------------------------
if __name__ == "__main__":
    db = DatabaseManager("scil.db")
    db.poblar_datos_iniciales()

