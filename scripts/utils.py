# ===========================================================
# scripts/utils.py — SCIL / SASP 2025
# Utilidades y lógica auxiliar centralizada
# ===========================================================

import hashlib
import json
import re
import sqlite3
import sys
import threading
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from shared_user_catalog import get_project_entes, list_users

_ENTE_ALIAS_MAP = {
    "SM": "SMYT",
    "SMYT": "SMYT",
    "SEFIN": "SF",
    "SF": "SF",
    "SOTYV": "SOTYV",
    "SECRETARIADEMOVILIDADYTRANSPORTE": "SMYT",
    "SECRETARIADEORDENAMIENTOTERRITORIALYVIVIENDA": "SOTYV",
    "SECRETARIADEFINANZAS": "SF",
}

_LEADING_NUM_PREFIX_RE = re.compile(r"^\s*\d+(?:\.\d+)*\s*[\.\)\-:]?\s*")


def _strip_accents_upper(s):
    out = str(s or "").strip().upper()
    if not out:
        return ""
    decomposed = unicodedata.normalize("NFD", out)
    return "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")


def _strip_numeric_prefix(text):
    if not text:
        return text
    cleaned = _LEADING_NUM_PREFIX_RE.sub("", text).strip()
    return cleaned or text


def _normalize_ente_alias(s):
    text = _strip_accents_upper(s)
    text = _strip_numeric_prefix(text)
    compact = re.sub(r"[^A-Z0-9]", "", text)
    if text in _ENTE_ALIAS_MAP:
        return _ENTE_ALIAS_MAP[text]
    if compact in _ENTE_ALIAS_MAP:
        return _ENTE_ALIAS_MAP[compact]
    return text


class DatabaseManager:
    def __init__(self, db_path="scil.db"):
        self.db_path = db_path
        self._catalog_lock = threading.Lock()
        self._catalog_snapshot = None
        self._connect_target = db_path
        self._connect_uri = False
        self._memory_keeper = None
        if db_path == ":memory:":
            self._connect_target = "file:05-sasp-memory?mode=memory&cache=shared"
            self._connect_uri = True
            self._memory_keeper = sqlite3.connect(
                self._connect_target,
                timeout=30,
                check_same_thread=False,
                uri=True,
            )
            self._configure_connection(self._memory_keeper)
        print(f"📂 Base de datos en uso: {Path(self.db_path).resolve()}")
        self._init_db()

    # -------------------------------------------------------
    # Conexión
    # -------------------------------------------------------
    def _configure_connection(self, conn):
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-20000")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _connect(self):
        conn = sqlite3.connect(
            self._connect_target,
            timeout=30,
            check_same_thread=False,
            uri=self._connect_uri,
        )
        return self._configure_connection(conn)

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

            CREATE TABLE IF NOT EXISTS workflow_estado (
                clave TEXT PRIMARY KEY,
                valor TEXT NOT NULL,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_por TEXT
            );

            CREATE TABLE IF NOT EXISTS prevalidaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                ente TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'Sin valoración',
                comentario TEXT,
                catalogo TEXT,
                otro_texto TEXT,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                usuario TEXT,
                UNIQUE(rfc, ente)
            );

            CREATE TABLE IF NOT EXISTS prevalidaciones_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                ente TEXT NOT NULL,
                estado_anterior TEXT,
                comentario_anterior TEXT,
                catalogo_anterior TEXT,
                otro_texto_anterior TEXT,
                estado_nuevo TEXT,
                comentario_nuevo TEXT,
                catalogo_nuevo TEXT,
                otro_texto_nuevo TEXT,
                accion TEXT NOT NULL DEFAULT 'actualizacion',
                usuario TEXT,
                creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

            CREATE TABLE IF NOT EXISTS horarios_laborales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                ente TEXT NOT NULL,
                dia_semana INTEGER NOT NULL,
                hora_inicio TEXT,
                hora_fin TEXT,
                observaciones TEXT,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_por TEXT,
                UNIQUE(rfc, ente, dia_semana)
            );

            CREATE TABLE IF NOT EXISTS observaciones_beta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                ente TEXT NOT NULL,
                observacion TEXT NOT NULL,
                estatus TEXT NOT NULL DEFAULT 'Borrador',
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_por TEXT,
                UNIQUE(rfc, ente)
            );

            CREATE TABLE IF NOT EXISTS horarios_persona (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                nombre TEXT NOT NULL,
                ente TEXT NOT NULL,
                cargo TEXT,
                dia_semana INTEGER NOT NULL,
                hora_inicio TEXT NOT NULL,
                hora_fin TEXT NOT NULL,
                fecha_inicio_vigencia TEXT NOT NULL,
                fecha_fin_vigencia TEXT,
                periodo TEXT,
                observaciones TEXT,
                estatus TEXT NOT NULL DEFAULT 'activo',
                permite_traslape_interno INTEGER NOT NULL DEFAULT 0,
                origen TEXT NOT NULL DEFAULT 'manual',
                creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_por TEXT
            );

            CREATE TABLE IF NOT EXISTS observaciones_cruce (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conflicto_hash TEXT UNIQUE NOT NULL,
                rfc TEXT NOT NULL,
                nombre TEXT NOT NULL,
                ente_a TEXT NOT NULL,
                ente_b TEXT NOT NULL,
                dia_semana INTEGER NOT NULL,
                horario_a TEXT NOT NULL,
                horario_b TEXT NOT NULL,
                minutos_traslape INTEGER NOT NULL DEFAULT 0,
                fecha_inicio_a TEXT,
                fecha_fin_a TEXT,
                fecha_inicio_b TEXT,
                fecha_fin_b TEXT,
                severidad TEXT NOT NULL,
                texto_observacion TEXT NOT NULL,
                recomendacion TEXT,
                estatus TEXT NOT NULL DEFAULT 'pendiente',
                creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                creado_por TEXT,
                comentarios_adicionales TEXT,
                referencia_documental TEXT
            );

            CREATE TABLE IF NOT EXISTS periodos_quincenales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                etiqueta TEXT UNIQUE NOT NULL,
                ejercicio INTEGER NOT NULL,
                quincena INTEGER NOT NULL,
                fecha_inicio TEXT NOT NULL,
                fecha_fin TEXT NOT NULL,
                activo INTEGER NOT NULL DEFAULT 1,
                creado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS pagos_pdp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfc TEXT NOT NULL,
                nombre TEXT NOT NULL,
                ente TEXT NOT NULL,
                periodo_quincenal TEXT NOT NULL,
                fecha_inicio_periodo TEXT NOT NULL,
                fecha_fin_periodo TEXT NOT NULL,
                sueldo_base REAL NOT NULL DEFAULT 0,
                monto_pdp REAL NOT NULL DEFAULT 0,
                deducciones REAL NOT NULL DEFAULT 0,
                percepciones_adicionales REAL NOT NULL DEFAULT 0,
                total_calculado REAL NOT NULL DEFAULT 0,
                estatus TEXT NOT NULL DEFAULT 'calculado',
                observaciones TEXT,
                conflicto_hash TEXT,
                actualizado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_por TEXT,
                UNIQUE(rfc, ente, periodo_quincenal)
            );
        """)
        conn.commit()

        # Migrar columnas nuevas en solventaciones si no existen
        self._migrate_solventaciones_columns(cur)
        self._migrate_workflow_defaults(cur)
        self._migrate_entes_siglas(cur)
        self._migrate_catalogos_mayusculas(cur)
        self._migrate_horarios_persona(cur)
        self._sync_catalog_users(cur)
        self._create_indexes(cur)
        self._seed_periodos_quincenales(cur)
        self._invalidate_catalog_cache()

        conn.commit()
        conn.close()
        print(f"✅ Tablas listas en {self.db_path}")

    def _create_indexes(self, cur):
        cur.executescript("""
            CREATE INDEX IF NOT EXISTS idx_registros_laborales_rfc
                ON registros_laborales(rfc);
            CREATE INDEX IF NOT EXISTS idx_registros_laborales_ente
                ON registros_laborales(ente);
            CREATE INDEX IF NOT EXISTS idx_registros_laborales_rfc_ente
                ON registros_laborales(rfc, ente);
            CREATE INDEX IF NOT EXISTS idx_solventaciones_rfc
                ON solventaciones(rfc);
            CREATE INDEX IF NOT EXISTS idx_solventaciones_rfc_ente
                ON solventaciones(rfc, ente);
            CREATE INDEX IF NOT EXISTS idx_prevalidaciones_rfc
                ON prevalidaciones(rfc);
            CREATE INDEX IF NOT EXISTS idx_prevalidaciones_rfc_ente
                ON prevalidaciones(rfc, ente);
            CREATE INDEX IF NOT EXISTS idx_usuarios_usuario
                ON usuarios(usuario);
            CREATE INDEX IF NOT EXISTS idx_horarios_laborales_rfc
                ON horarios_laborales(rfc);
            CREATE INDEX IF NOT EXISTS idx_horarios_laborales_ente
                ON horarios_laborales(ente);
            CREATE INDEX IF NOT EXISTS idx_horarios_laborales_rfc_ente
                ON horarios_laborales(rfc, ente);
            CREATE INDEX IF NOT EXISTS idx_observaciones_beta_rfc
                ON observaciones_beta(rfc);
            CREATE INDEX IF NOT EXISTS idx_observaciones_beta_rfc_ente
                ON observaciones_beta(rfc, ente);
            CREATE INDEX IF NOT EXISTS idx_horarios_persona_rfc
                ON horarios_persona(rfc);
            CREATE INDEX IF NOT EXISTS idx_horarios_persona_ente
                ON horarios_persona(ente);
            CREATE INDEX IF NOT EXISTS idx_horarios_persona_rfc_dia
                ON horarios_persona(rfc, dia_semana, estatus);
            CREATE INDEX IF NOT EXISTS idx_observaciones_cruce_rfc
                ON observaciones_cruce(rfc);
            CREATE INDEX IF NOT EXISTS idx_observaciones_cruce_estatus
                ON observaciones_cruce(estatus, severidad);
            CREATE INDEX IF NOT EXISTS idx_periodos_quincenales_ejercicio
                ON periodos_quincenales(ejercicio, quincena);
            CREATE INDEX IF NOT EXISTS idx_pagos_pdp_periodo
                ON pagos_pdp(periodo_quincenal, estatus);
            CREATE INDEX IF NOT EXISTS idx_pagos_pdp_rfc
                ON pagos_pdp(rfc, ente);
        """)

    def _invalidate_catalog_cache(self):
        with self._catalog_lock:
            self._catalog_snapshot = None

    def _get_catalog_snapshot(self):
        with self._catalog_lock:
            if self._catalog_snapshot is not None:
                return self._catalog_snapshot

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT num, clave, nombre, siglas, clasificacion, ambito, 'ENTE' AS tipo_tabla
            FROM entes
            WHERE activo=1
            UNION ALL
            SELECT num, clave, nombre, siglas, clasificacion, ambito, 'MUNICIPIO' AS tipo_tabla
            FROM municipios
            WHERE activo=1
        """)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        lookup = {}
        for row in rows:
            for raw_value in (row.get("clave"), row.get("siglas"), row.get("nombre")):
                normalized = self._sanitize(raw_value)
                if normalized and normalized not in lookup:
                    lookup[normalized] = row

        snapshot = {"rows": rows, "lookup": lookup}
        with self._catalog_lock:
            self._catalog_snapshot = snapshot
        return snapshot

    def get_catalog_lookup(self):
        return dict(self._get_catalog_snapshot()["lookup"])

    def _sync_catalog_users(self, cur):
        for catalog_user in list_users(project_key="05-sasp"):
            username = str(catalog_user.get("usuario") or "").strip().lower()
            password = str(catalog_user.get("clave") or "").strip()
            display_name = str(catalog_user.get("nombre_completo") or username).strip()
            if not username or not password:
                continue

            entes = get_project_entes(catalog_user, "05-sasp")
            entes_text = ",".join(entes or ["TODOS"])
            password_hash = hashlib.sha256(password.encode()).hexdigest()

            cur.execute(
                """
                SELECT id
                FROM usuarios
                WHERE LOWER(usuario)=LOWER(?)
                LIMIT 1
                """,
                (username,),
            )
            row = cur.fetchone()

            if row:
                cur.execute(
                    """
                    UPDATE usuarios
                    SET nombre=?, clave=?, entes=?
                    WHERE id=?
                    """,
                    (display_name, password_hash, entes_text, row["id"]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO usuarios (nombre, usuario, clave, entes)
                    VALUES (?, ?, ?, ?)
                    """,
                    (display_name, username, password_hash, entes_text),
                )

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

    def _migrate_workflow_defaults(self, cur):
        """Inicializa el estado global de validación si no existe."""
        cur.execute("""
            INSERT OR IGNORE INTO workflow_estado (clave, valor, actualizado_por)
            VALUES ('validacion_resultados', 'borrador', 'SISTEMA')
        """)

    def _migrate_entes_siglas(self, cur):
        """Normaliza siglas oficiales de entes en instalaciones existentes."""
        cur.execute("UPDATE entes SET siglas='SMyT' WHERE UPPER(siglas)='SM'")
        cur.execute("UPDATE entes SET siglas='SF' WHERE UPPER(siglas)='SEFIN'")

    def _migrate_catalogos_mayusculas(self, cur):
        """Normaliza a mayúsculas nombre/siglas/clasificación en entes y municipios."""
        for tabla in ("entes", "municipios"):
            cur.execute(f"SELECT id, nombre, siglas, clasificacion FROM {tabla}")
            updates = []
            for row in cur.fetchall():
                nombre = (row["nombre"] or "").strip().upper()
                siglas = (row["siglas"] or "").strip().upper()
                clasificacion = (row["clasificacion"] or "").strip().upper()
                if (
                    nombre != (row["nombre"] or "")
                    or siglas != (row["siglas"] or "")
                    or clasificacion != (row["clasificacion"] or "")
                ):
                    updates.append((nombre, siglas, clasificacion, row["id"]))
            if updates:
                cur.executemany(
                    f"""
                    UPDATE {tabla}
                    SET nombre=?, siglas=?, clasificacion=?
                    WHERE id=?
                    """,
                    updates
                )

    def _migrate_horarios_persona(self, cur):
        """Asegura columnas auxiliares en horarios_persona para instalaciones existentes."""
        cur.execute("PRAGMA table_info(horarios_persona)")
        columns = [row[1] for row in cur.fetchall()]
        expected_columns = {
            "permite_traslape_interno": "INTEGER NOT NULL DEFAULT 0",
            "origen": "TEXT NOT NULL DEFAULT 'manual'",
            "actualizado": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "actualizado_por": "TEXT",
        }
        for column_name, definition in expected_columns.items():
            if column_name not in columns:
                cur.execute(f"ALTER TABLE horarios_persona ADD COLUMN {column_name} {definition}")

    def _seed_periodos_quincenales(self, cur, ejercicio=None):
        ejercicio = int(ejercicio or datetime.now().year)
        cur.execute("SELECT COUNT(*) FROM periodos_quincenales WHERE ejercicio=?", (ejercicio,))
        if cur.fetchone()[0] > 0:
            return

        seed_rows = []
        for quincena in range(1, 25):
            month = ((quincena - 1) // 2) + 1
            if quincena % 2 == 1:
                fecha_inicio = date(ejercicio, month, 1)
                fecha_fin = date(ejercicio, month, 15)
            else:
                fecha_inicio = date(ejercicio, month, 16)
                if month == 12:
                    fecha_fin = date(ejercicio, month, 31)
                else:
                    fecha_fin = date(ejercicio, month + 1, 1) - timedelta(days=1)
            etiqueta = f"{ejercicio}-QNA{quincena}"
            seed_rows.append((etiqueta, ejercicio, quincena, fecha_inicio.isoformat(), fecha_fin.isoformat(), 1))

        cur.executemany("""
            INSERT OR IGNORE INTO periodos_quincenales
                (etiqueta, ejercicio, quincena, fecha_inicio, fecha_fin, activo)
            VALUES (?, ?, ?, ?, ?, ?)
        """, seed_rows)

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
                ("1.2", "ENTE_1_2", "SECRETARÍA DE GOBIERNO", "SEGOB", "DEPENDENCIA", "ESTATAL"),
                ("1.4", "ENTE_1_4", "SECRETARÍA DE FINANZAS", "SF", "DEPENDENCIA", "ESTATAL"),
                ("1.8", "ENTE_1_8", "SECRETARÍA DE EDUCACIÓN PÚBLICA", "SEPE", "DEPENDENCIA", "ESTATAL"),
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
        return _normalize_ente_alias(s)

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
        row = self._get_catalog_snapshot()["lookup"].get(self._sanitize(valor))
        return row["nombre"] if row else None

    def normalizar_ente_clave(self, valor):
        """
        Busca un ente o municipio por sigla, clave o nombre y devuelve la CLAVE única.
        Útil para operaciones de base de datos y referencias internas.
        """
        if not valor:
            return None
        row = self._get_catalog_snapshot()["lookup"].get(self._sanitize(valor))
        return row["clave"] if row else None

    def guardar_horarios_laborales(self, rfc, ente, horarios, usuario=""):
        rfc_norm = str(rfc or "").strip().upper()
        ente_norm = self.normalizar_ente_clave(ente) or str(ente or "").strip()
        if not rfc_norm or not ente_norm:
            return 0

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM horarios_laborales WHERE rfc=? AND ente=?", (rfc_norm, ente_norm))

        filas = 0
        for horario in horarios:
            dia_semana = int(horario.get("dia_semana", -1))
            hora_inicio = str(horario.get("hora_inicio") or "").strip()
            hora_fin = str(horario.get("hora_fin") or "").strip()
            observaciones = str(horario.get("observaciones") or "").strip()

            if dia_semana < 0 or dia_semana > 6:
                continue
            if not hora_inicio or not hora_fin:
                continue

            cur.execute("""
                INSERT INTO horarios_laborales
                    (rfc, ente, dia_semana, hora_inicio, hora_fin, observaciones, actualizado, actualizado_por)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            """, (rfc_norm, ente_norm, dia_semana, hora_inicio, hora_fin, observaciones, usuario))
            filas += 1

        conn.commit()
        conn.close()
        return filas

    def get_horarios_por_rfc(self, rfc):
        rfc_norm = str(rfc or "").strip().upper()
        if not rfc_norm:
            return {}

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT ente, dia_semana, hora_inicio, hora_fin, observaciones, actualizado, actualizado_por
            FROM horarios_laborales
            WHERE rfc=?
            ORDER BY ente, dia_semana
        """, (rfc_norm,))
        rows = cur.fetchall()
        conn.close()

        horarios = defaultdict(list)
        for row in rows:
            horarios[row["ente"]].append({
                "dia_semana": row["dia_semana"],
                "hora_inicio": row["hora_inicio"] or "",
                "hora_fin": row["hora_fin"] or "",
                "observaciones": row["observaciones"] or "",
                "actualizado": row["actualizado"],
                "actualizado_por": row["actualizado_por"] or "",
            })
        return dict(horarios)

    def guardar_observacion_beta(self, rfc, ente, observacion, estatus="Borrador", usuario=""):
        rfc_norm = str(rfc or "").strip().upper()
        ente_norm = self.normalizar_ente_clave(ente) or str(ente or "").strip()
        observacion_txt = str(observacion or "").strip()
        estatus_txt = str(estatus or "Borrador").strip() or "Borrador"
        if not rfc_norm or not ente_norm:
            return 0

        conn = self._connect()
        cur = conn.cursor()
        if observacion_txt:
            cur.execute("""
                INSERT INTO observaciones_beta (rfc, ente, observacion, estatus, actualizado, actualizado_por)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(rfc, ente) DO UPDATE SET
                    observacion=excluded.observacion,
                    estatus=excluded.estatus,
                    actualizado=CURRENT_TIMESTAMP,
                    actualizado_por=excluded.actualizado_por
            """, (rfc_norm, ente_norm, observacion_txt, estatus_txt, usuario))
            filas = cur.rowcount
        else:
            cur.execute("DELETE FROM observaciones_beta WHERE rfc=? AND ente=?", (rfc_norm, ente_norm))
            filas = cur.rowcount
        conn.commit()
        conn.close()
        return filas

    def get_observaciones_beta_por_rfc(self, rfc):
        rfc_norm = str(rfc or "").strip().upper()
        if not rfc_norm:
            return {}

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT ente, observacion, estatus, actualizado, actualizado_por
            FROM observaciones_beta
            WHERE rfc=?
        """, (rfc_norm,))
        rows = cur.fetchall()
        conn.close()
        return {
            row["ente"]: {
                "observacion": row["observacion"] or "",
                "estatus": row["estatus"] or "Borrador",
                "actualizado": row["actualizado"],
                "actualizado_por": row["actualizado_por"] or "",
            }
            for row in rows
        }

    # -------------------------------------------------------
    # Horarios institucionales
    # -------------------------------------------------------
    def obtener_nombre_persona_por_rfc(self, rfc):
        rfc_norm = str(rfc or "").strip().upper()
        if not rfc_norm:
            return ""

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT nombre
            FROM registros_laborales
            WHERE UPPER(rfc)=UPPER(?)
            ORDER BY fecha_actualizacion DESC, fecha_carga DESC
            LIMIT 1
        """, (rfc_norm,))
        row = cur.fetchone()
        conn.close()
        return str(row["nombre"] or "").strip() if row else ""

    def guardar_horario_persona(self, horario, usuario=""):
        rfc = str(horario.get("rfc") or "").strip().upper()
        ente = self.normalizar_ente_clave(horario.get("ente")) or str(horario.get("ente") or "").strip()
        nombre = str(horario.get("nombre") or self.obtener_nombre_persona_por_rfc(rfc) or "").strip()
        cargo = str(horario.get("cargo") or "").strip()
        periodo = str(horario.get("periodo") or "").strip()
        observaciones = str(horario.get("observaciones") or "").strip()
        estatus = str(horario.get("estatus") or "activo").strip().lower()
        origen = str(horario.get("origen") or "manual").strip()
        dia_semana = int(horario.get("dia_semana", -1))
        hora_inicio = str(horario.get("hora_inicio") or "").strip()
        hora_fin = str(horario.get("hora_fin") or "").strip()
        fecha_inicio_vigencia = str(horario.get("fecha_inicio_vigencia") or "").strip()
        fecha_fin_vigencia = str(horario.get("fecha_fin_vigencia") or "").strip()
        permite_traslape_interno = 1 if horario.get("permite_traslape_interno") else 0

        if not rfc or not nombre or not ente or dia_semana < 0 or dia_semana > 6:
            raise ValueError("Horario incompleto o inválido.")
        if not hora_inicio or not hora_fin or not fecha_inicio_vigencia:
            raise ValueError("Debes indicar día, horas y vigencia inicial.")

        conn = self._connect()
        cur = conn.cursor()
        if horario.get("id"):
            cur.execute("""
                UPDATE horarios_persona
                SET nombre=?, ente=?, cargo=?, dia_semana=?, hora_inicio=?, hora_fin=?,
                    fecha_inicio_vigencia=?, fecha_fin_vigencia=?, periodo=?, observaciones=?,
                    estatus=?, permite_traslape_interno=?, origen=?, actualizado=CURRENT_TIMESTAMP,
                    actualizado_por=?
                WHERE id=?
            """, (
                nombre, ente, cargo, dia_semana, hora_inicio, hora_fin,
                fecha_inicio_vigencia, fecha_fin_vigencia or None, periodo, observaciones,
                estatus, permite_traslape_interno, origen, usuario, int(horario["id"]),
            ))
            horario_id = int(horario["id"])
        else:
            cur.execute("""
                INSERT INTO horarios_persona
                    (rfc, nombre, ente, cargo, dia_semana, hora_inicio, hora_fin,
                     fecha_inicio_vigencia, fecha_fin_vigencia, periodo, observaciones,
                     estatus, permite_traslape_interno, origen, actualizado_por)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rfc, nombre, ente, cargo, dia_semana, hora_inicio, hora_fin,
                fecha_inicio_vigencia, fecha_fin_vigencia or None, periodo, observaciones,
                estatus, permite_traslape_interno, origen, usuario,
            ))
            horario_id = cur.lastrowid
        conn.commit()
        conn.close()
        return horario_id

    def desactivar_horario_persona(self, horario_id, usuario=""):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE horarios_persona
            SET estatus='inactivo', actualizado=CURRENT_TIMESTAMP, actualizado_por=?
            WHERE id=?
        """, (usuario, int(horario_id)))
        filas = cur.rowcount
        conn.commit()
        conn.close()
        return filas

    def eliminar_horario_persona(self, horario_id):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM horarios_persona WHERE id=?", (int(horario_id),))
        filas = cur.rowcount
        conn.commit()
        conn.close()
        return filas

    def obtener_horario_persona(self, horario_id):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT * FROM horarios_persona WHERE id=?", (int(horario_id),))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def listar_horarios_persona(self, filtros=None):
        filtros = filtros or {}
        clauses = []
        params = []

        if filtros.get("rfc"):
            clauses.append("UPPER(rfc)=UPPER(?)")
            params.append(str(filtros["rfc"]).strip())
        if filtros.get("ente"):
            ente_clave = self.normalizar_ente_clave(filtros["ente"]) or str(filtros["ente"]).strip()
            clauses.append("ente=?")
            params.append(ente_clave)
        if filtros.get("dia_semana") not in (None, ""):
            clauses.append("dia_semana=?")
            params.append(int(filtros["dia_semana"]))
        if filtros.get("estatus"):
            clauses.append("LOWER(estatus)=LOWER(?)")
            params.append(str(filtros["estatus"]).strip())
        if filtros.get("nombre"):
            clauses.append("nombre LIKE ?")
            params.append(f"%{str(filtros['nombre']).strip()}%")
        if filtros.get("fecha_desde"):
            clauses.append("date(fecha_inicio_vigencia) >= date(?)")
            params.append(str(filtros["fecha_desde"]).strip())
        if filtros.get("fecha_hasta"):
            clauses.append("(fecha_fin_vigencia IS NULL OR date(fecha_fin_vigencia) <= date(?))")
            params.append(str(filtros["fecha_hasta"]).strip())

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT *
            FROM horarios_persona
            {where_sql}
            ORDER BY nombre, rfc, ente, dia_semana, hora_inicio
        """, params)
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows

    def upsert_observacion_cruce(self, observacion, usuario=""):
        conflicto_hash = str(observacion.get("conflicto_hash") or "").strip()
        if not conflicto_hash:
            raise ValueError("La observación requiere un conflicto_hash.")

        payload = (
            conflicto_hash,
            str(observacion.get("rfc") or "").strip().upper(),
            str(observacion.get("nombre") or "").strip(),
            self.normalizar_ente_clave(observacion.get("ente_a")) or str(observacion.get("ente_a") or "").strip(),
            self.normalizar_ente_clave(observacion.get("ente_b")) or str(observacion.get("ente_b") or "").strip(),
            int(observacion.get("dia_semana", 0)),
            str(observacion.get("horario_a") or "").strip(),
            str(observacion.get("horario_b") or "").strip(),
            int(observacion.get("minutos_traslape", 0)),
            str(observacion.get("fecha_inicio_a") or "").strip() or None,
            str(observacion.get("fecha_fin_a") or "").strip() or None,
            str(observacion.get("fecha_inicio_b") or "").strip() or None,
            str(observacion.get("fecha_fin_b") or "").strip() or None,
            str(observacion.get("severidad") or "baja").strip().lower(),
            str(observacion.get("texto_observacion") or "").strip(),
            str(observacion.get("recomendacion") or "").strip(),
            str(observacion.get("estatus") or "pendiente").strip().lower(),
            usuario or str(observacion.get("creado_por") or "").strip(),
            str(observacion.get("comentarios_adicionales") or "").strip(),
            str(observacion.get("referencia_documental") or "").strip(),
        )

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO observaciones_cruce
                (conflicto_hash, rfc, nombre, ente_a, ente_b, dia_semana, horario_a, horario_b,
                 minutos_traslape, fecha_inicio_a, fecha_fin_a, fecha_inicio_b, fecha_fin_b,
                 severidad, texto_observacion, recomendacion, estatus, creado_por,
                 comentarios_adicionales, referencia_documental)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(conflicto_hash) DO UPDATE SET
                nombre=excluded.nombre,
                ente_a=excluded.ente_a,
                ente_b=excluded.ente_b,
                dia_semana=excluded.dia_semana,
                horario_a=excluded.horario_a,
                horario_b=excluded.horario_b,
                minutos_traslape=excluded.minutos_traslape,
                fecha_inicio_a=excluded.fecha_inicio_a,
                fecha_fin_a=excluded.fecha_fin_a,
                fecha_inicio_b=excluded.fecha_inicio_b,
                fecha_fin_b=excluded.fecha_fin_b,
                severidad=excluded.severidad,
                texto_observacion=excluded.texto_observacion,
                recomendacion=excluded.recomendacion,
                estatus=excluded.estatus,
                actualizado=CURRENT_TIMESTAMP,
                comentarios_adicionales=excluded.comentarios_adicionales,
                referencia_documental=excluded.referencia_documental
        """, payload)
        conn.commit()
        conn.close()
        return conflicto_hash

    def actualizar_estatus_observacion_cruce(self, conflicto_hash, estatus, comentarios="", usuario=""):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE observaciones_cruce
            SET estatus=?, comentarios_adicionales=?, actualizado=CURRENT_TIMESTAMP, creado_por=COALESCE(creado_por, ?)
            WHERE conflicto_hash=?
        """, (estatus, comentarios, usuario, conflicto_hash))
        filas = cur.rowcount
        conn.commit()
        conn.close()
        return filas

    def listar_observaciones_cruce(self, filtros=None):
        filtros = filtros or {}
        clauses = []
        params = []
        if filtros.get("rfc"):
            clauses.append("UPPER(rfc)=UPPER(?)")
            params.append(str(filtros["rfc"]).strip())
        if filtros.get("estatus"):
            clauses.append("LOWER(estatus)=LOWER(?)")
            params.append(str(filtros["estatus"]).strip())
        if filtros.get("severidad"):
            clauses.append("LOWER(severidad)=LOWER(?)")
            params.append(str(filtros["severidad"]).strip())
        if filtros.get("ente"):
            ente_clave = self.normalizar_ente_clave(filtros["ente"]) or str(filtros["ente"]).strip()
            clauses.append("(ente_a=? OR ente_b=?)")
            params.extend([ente_clave, ente_clave])
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT *
            FROM observaciones_cruce
            {where_sql}
            ORDER BY actualizado DESC, severidad DESC, nombre
        """, params)
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows

    def listar_periodos_quincenales(self, ejercicio=None):
        clauses = []
        params = []
        if ejercicio:
            clauses.append("ejercicio=?")
            params.append(int(ejercicio))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT *
            FROM periodos_quincenales
            {where_sql}
            ORDER BY ejercicio DESC, quincena
        """, params)
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows

    def get_periodo_quincenal(self, etiqueta):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT * FROM periodos_quincenales WHERE etiqueta=?", (str(etiqueta).strip(),))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def upsert_pago_pdp(self, pago, usuario=""):
        payload = (
            str(pago.get("rfc") or "").strip().upper(),
            str(pago.get("nombre") or "").strip(),
            self.normalizar_ente_clave(pago.get("ente")) or str(pago.get("ente") or "").strip(),
            str(pago.get("periodo_quincenal") or "").strip(),
            str(pago.get("fecha_inicio_periodo") or "").strip(),
            str(pago.get("fecha_fin_periodo") or "").strip(),
            float(pago.get("sueldo_base") or 0),
            float(pago.get("monto_pdp") or 0),
            float(pago.get("deducciones") or 0),
            float(pago.get("percepciones_adicionales") or 0),
            float(pago.get("total_calculado") or 0),
            str(pago.get("estatus") or "calculado").strip().lower(),
            str(pago.get("observaciones") or "").strip(),
            str(pago.get("conflicto_hash") or "").strip() or None,
            usuario,
        )

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pagos_pdp
                (rfc, nombre, ente, periodo_quincenal, fecha_inicio_periodo, fecha_fin_periodo,
                 sueldo_base, monto_pdp, deducciones, percepciones_adicionales, total_calculado,
                 estatus, observaciones, conflicto_hash, actualizado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rfc, ente, periodo_quincenal) DO UPDATE SET
                nombre=excluded.nombre,
                fecha_inicio_periodo=excluded.fecha_inicio_periodo,
                fecha_fin_periodo=excluded.fecha_fin_periodo,
                sueldo_base=excluded.sueldo_base,
                monto_pdp=excluded.monto_pdp,
                deducciones=excluded.deducciones,
                percepciones_adicionales=excluded.percepciones_adicionales,
                total_calculado=excluded.total_calculado,
                estatus=excluded.estatus,
                observaciones=excluded.observaciones,
                conflicto_hash=excluded.conflicto_hash,
                actualizado=CURRENT_TIMESTAMP,
                actualizado_por=excluded.actualizado_por
        """, payload)
        conn.commit()
        conn.close()
        return True

    def listar_pagos_pdp(self, filtros=None):
        filtros = filtros or {}
        clauses = []
        params = []
        if filtros.get("periodo_quincenal"):
            clauses.append("periodo_quincenal=?")
            params.append(str(filtros["periodo_quincenal"]).strip())
        if filtros.get("ente"):
            ente_clave = self.normalizar_ente_clave(filtros["ente"]) or str(filtros["ente"]).strip()
            clauses.append("ente=?")
            params.append(ente_clave)
        if filtros.get("rfc"):
            clauses.append("UPPER(rfc)=UPPER(?)")
            params.append(str(filtros["rfc"]).strip())
        if filtros.get("estatus"):
            clauses.append("LOWER(estatus)=LOWER(?)")
            params.append(str(filtros["estatus"]).strip())

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT *
            FROM pagos_pdp
            {where_sql}
            ORDER BY fecha_inicio_periodo DESC, nombre, ente
        """, params)
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows

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

    def obtener_trabajadores_por_ente(self):
        """Obtiene trabajadores agrupados por ente para vistas de auditoría."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT ente, rfc, nombre, puesto, fecha_ingreso, fecha_egreso, monto, qnas
            FROM registros_laborales
            ORDER BY ente, nombre, rfc
        """)

        resultado = {}
        for row in cur.fetchall():
            ente = (row["ente"] or "").strip()
            if not ente:
                continue

            try:
                qnas = json.loads(row["qnas"] or "{}")
                if not isinstance(qnas, dict):
                    qnas = {}
            except Exception:
                qnas = {}

            resultado.setdefault(ente, []).append({
                "rfc": row["rfc"] or "",
                "nombre": row["nombre"] or "",
                "puesto": row["puesto"] or "",
                "fecha_ingreso": row["fecha_ingreso"],
                "fecha_egreso": row["fecha_egreso"],
                "monto": row["monto"],
                "qnas": qnas
            })

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

    def obtener_entes_con_cruce_por_rfc(self, rfc):
        """Obtiene entes de un RFC con cruce real de QNAs entre sí."""
        rfc = (rfc or "").strip().upper()
        if not rfc:
            return []

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT ente, qnas
            FROM registros_laborales
            WHERE UPPER(rfc) = UPPER(?)
            ORDER BY ente
        """, (rfc,))
        rows = cur.fetchall()
        conn.close()

        if len(rows) < 2:
            return []

        qnas_por_ente = {}
        for row in rows:
            ente = (row["ente"] or "").strip()
            if not ente:
                continue
            try:
                qnas = json.loads(row["qnas"] or "{}")
                if not isinstance(qnas, dict):
                    qnas = {}
            except Exception:
                qnas = {}
            qnas_por_ente[ente] = set(qnas.keys())

        entes = list(qnas_por_ente.keys())
        entes_con_cruce = set()
        for i in range(len(entes)):
            for j in range(i + 1, len(entes)):
                e1, e2 = entes[i], entes[j]
                if qnas_por_ente[e1].intersection(qnas_por_ente[e2]):
                    entes_con_cruce.update([e1, e2])

        return sorted(list(entes_con_cruce))

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

    def get_solventaciones_por_rfcs(self, rfcs):
        rfcs = [str(r).strip().upper() for r in (rfcs or []) if str(r).strip()]
        if not rfcs:
            return {}

        conn = self._connect()
        cur = conn.cursor()
        placeholders = ",".join(["?"] * len(rfcs))
        cur.execute(f"""
            SELECT rfc, ente, estado, comentario
            FROM solventaciones
            WHERE UPPER(rfc) IN ({placeholders})
        """, rfcs)

        data = {}
        for row in cur.fetchall():
            rfc = (row["rfc"] or "").strip().upper()
            ente = row["ente"] or ""
            if not rfc or not ente:
                continue
            data.setdefault(rfc, {})[ente] = {
                "estado": row["estado"] or "Sin valoración",
                "comentario": row["comentario"] or ""
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

    def guardar_prevalidacion_duplicado(
        self,
        rfc,
        ente,
        estado,
        comentario="",
        catalogo="",
        otro_texto="",
        usuario="luis",
    ):
        estado = estado or "Sin valoración"
        ente_norm = self.normalizar_ente_clave(ente) or ente

        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            SELECT estado, comentario, catalogo, otro_texto
            FROM prevalidaciones
            WHERE rfc=? AND ente=?
            LIMIT 1
        """, (rfc, ente_norm))
        prev = cur.fetchone()

        cur.execute("""
            INSERT INTO prevalidaciones
                (rfc, ente, estado, comentario, catalogo, otro_texto, actualizado, usuario)
            VALUES
                (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(rfc, ente) DO UPDATE SET
                estado=excluded.estado,
                comentario=excluded.comentario,
                catalogo=excluded.catalogo,
                otro_texto=excluded.otro_texto,
                actualizado=CURRENT_TIMESTAMP,
                usuario=excluded.usuario
        """, (rfc, ente_norm, estado, comentario, catalogo, otro_texto, usuario))

        cur.execute("""
            INSERT INTO prevalidaciones_historial
                (rfc, ente,
                 estado_anterior, comentario_anterior, catalogo_anterior, otro_texto_anterior,
                 estado_nuevo, comentario_nuevo, catalogo_nuevo, otro_texto_nuevo,
                 accion, usuario)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rfc,
            ente_norm,
            prev["estado"] if prev else None,
            prev["comentario"] if prev else None,
            prev["catalogo"] if prev else None,
            prev["otro_texto"] if prev else None,
            estado,
            comentario,
            catalogo,
            otro_texto,
            "cancelar_solventacion" if estado == "Sin valoración" else "solventar",
            usuario,
        ))

        filas = cur.rowcount
        conn.commit()
        conn.close()
        return filas

    def get_prevalidaciones_por_rfc(self, rfc):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT ente, estado, comentario, catalogo, otro_texto
            FROM prevalidaciones
            WHERE rfc=?
        """, (rfc,))
        data = {}
        for row in cur.fetchall():
            data[row["ente"]] = {
                "estado": row["estado"] or "Sin valoración",
                "comentario": row["comentario"] or "",
                "catalogo": row["catalogo"] or "",
                "otro_texto": row["otro_texto"] or "",
            }
        conn.close()
        return data

    def get_prevalidaciones_por_rfcs(self, rfcs):
        rfcs = [str(r).strip().upper() for r in (rfcs or []) if str(r).strip()]
        if not rfcs:
            return {}

        conn = self._connect()
        cur = conn.cursor()
        placeholders = ",".join(["?"] * len(rfcs))
        cur.execute(f"""
            SELECT rfc, ente, estado, comentario, catalogo, otro_texto
            FROM prevalidaciones
            WHERE UPPER(rfc) IN ({placeholders})
        """, rfcs)

        data = {}
        for row in cur.fetchall():
            rfc = (row["rfc"] or "").strip().upper()
            ente = row["ente"] or ""
            if not rfc or not ente:
                continue
            data.setdefault(rfc, {})[ente] = {
                "estado": row["estado"] or "Sin valoración",
                "comentario": row["comentario"] or "",
                "catalogo": row["catalogo"] or "",
                "otro_texto": row["otro_texto"] or "",
            }

        conn.close()
        return data

    def resultados_validados(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT valor
            FROM workflow_estado
            WHERE clave='validacion_resultados'
            LIMIT 1
        """)
        row = cur.fetchone()
        conn.close()
        valor = (row["valor"] if row else "borrador") or "borrador"
        return str(valor).strip().lower() == "validados"

    def marcar_resultados_validados(self, usuario="luis"):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO workflow_estado (clave, valor, actualizado_por)
            VALUES ('validacion_resultados', 'validados', ?)
            ON CONFLICT(clave) DO UPDATE SET
                valor='validados',
                actualizado=CURRENT_TIMESTAMP,
                actualizado_por=excluded.actualizado_por
        """, (usuario,))
        conn.commit()
        conn.close()

    def desmarcar_resultados_validados(self, usuario="luis"):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO workflow_estado (clave, valor, actualizado_por)
            VALUES ('validacion_resultados', 'borrador', ?)
            ON CONFLICT(clave) DO UPDATE SET
                valor='borrador',
                actualizado=CURRENT_TIMESTAMP,
                actualizado_por=excluded.actualizado_por
        """, (usuario,))
        conn.commit()
        conn.close()

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


class DataProcessor:
    """
    Procesador de archivos Excel laborales.

    Funcionalidad principal:
    - Lee archivos Excel con datos de empleados por ente público
    - Detecta empleados activos en múltiples entes en la misma quincena
    - Genera registros de cruces (duplicaciones) y empleados únicos
    """

    def __init__(self, db_manager=None, db_path="scil.db"):
        self.db = db_manager or DatabaseManager(db_path)
        self.mapa_siglas = self.db.get_mapa_siglas()
        self.mapa_inverso = self.db.get_mapa_claves_inverso()

    # -------------------------------------------------------
    # Limpieza y normalización
    # -------------------------------------------------------
    def limpiar_rfc(self, rfc):
        if pd.isna(rfc):
            return None
        s = re.sub(r"[^A-Z0-9]", "", str(rfc).strip().upper())
        return s if 10 <= len(s) <= 13 else None

    def limpiar_fecha(self, fecha):
        if pd.isna(fecha):
            return None
        if isinstance(fecha, (datetime, date)):
            return fecha.strftime("%Y-%m-%d")
        s = str(fecha).strip()
        if s.lower() in {"", "nan", "nat", "none", "null"}:
            return None
        f = pd.to_datetime(s, errors="coerce", dayfirst=True)
        return f.strftime("%Y-%m-%d") if not pd.isna(f) else None

    def normalizar_ente_clave(self, etiqueta):
        if not etiqueta:
            return None
        val = _normalize_ente_alias(etiqueta)
        if val in self.mapa_siglas:
            return self.mapa_siglas[val]
        return self.db.normalizar_ente_clave(val)

    # -------------------------------------------------------
    # Procesamiento principal
    # -------------------------------------------------------
    def extraer_registros_individuales(self, archivos):
        """
        Extrae TODOS los registros individuales (RFC+ENTE) sin procesar cruces.
        Esto permite guardarlos/actualizarlos en la BD sin duplicar.

        Returns:
            (registros_individuales, alertas)
        """
        print(f"📊 Procesando {len(archivos)} archivo(s) laborales...")
        registros = []
        alertas = []

        for f in archivos:
            nombre_archivo = getattr(f, "filename", getattr(f, "name", "archivo.xlsx"))
            print(f"📘 Leyendo archivo: {nombre_archivo}")
            xl = pd.ExcelFile(f)

            for hoja in xl.sheet_names:
                ente_label = hoja.strip().upper()
                clave_ente = self.normalizar_ente_clave(ente_label)

                if not clave_ente:
                    alerta = f"⚠️ Hoja '{hoja}' no encontrada en catálogo de entes. Verifique el nombre."
                    print(alerta)
                    alertas.append({
                        "tipo": "ente_no_encontrado",
                        "mensaje": alerta,
                        "hoja": hoja,
                        "archivo": nombre_archivo
                    })
                    continue

                df = xl.parse(hoja).rename(columns=lambda x: str(x).strip().upper().replace(" ", "_"))
                columnas_base = {"RFC", "NOMBRE", "PUESTO", "FECHA_ALTA", "FECHA_BAJA"}

                if not columnas_base.issubset(df.columns):
                    alerta = f"⚠️ Hoja '{hoja}' omitida: faltan columnas requeridas."
                    print(alerta)
                    alertas.append({
                        "tipo": "columnas_faltantes",
                        "mensaje": alerta,
                        "hoja": hoja,
                        "archivo": nombre_archivo
                    })
                    continue

                qnas = [c for c in df.columns if re.match(r"^QNA([1-9]|1[0-9]|2[0-4])$", c)]
                registros_validos = 0

                for _, row in df.iterrows():
                    rfc = self.limpiar_rfc(row.get("RFC"))
                    if not rfc:
                        continue

                    qnas_activas = {q: row.get(q) for q in qnas if self._es_activo(row.get(q))}

                    # Agregar registro individual
                    registros.append({
                        "rfc": rfc,
                        "ente": clave_ente,
                        "nombre": str(row.get("NOMBRE", "")).strip(),
                        "puesto": str(row.get("PUESTO", "")).strip(),
                        "fecha_ingreso": self.limpiar_fecha(row.get("FECHA_ALTA")),
                        "fecha_egreso": self.limpiar_fecha(row.get("FECHA_BAJA")),
                        "qnas": qnas_activas,
                        "monto": row.get("TOT_PERC"),
                    })
                    registros_validos += 1

                print(f"✅ Hoja '{hoja}': {registros_validos} registros procesados.")

        print(f"📈 {len(registros)} registros individuales extraídos.")
        return registros, alertas

    def procesar_archivos(self, archivos):
        print(f"📊 Procesando {len(archivos)} archivo(s) laborales...")
        entes_rfc = defaultdict(list)
        alertas = []

        for f in archivos:
            nombre_archivo = getattr(f, "filename", getattr(f, "name", "archivo.xlsx"))
            print(f"📘 Leyendo archivo: {nombre_archivo}")
            xl = pd.ExcelFile(f)

            for hoja in xl.sheet_names:
                ente_label = hoja.strip().upper()
                clave_ente = self.normalizar_ente_clave(ente_label)

                if not clave_ente:
                    alerta = f"⚠️ Hoja '{hoja}' no encontrada en catálogo de entes. Verifique el nombre."
                    print(alerta)
                    alertas.append({
                        "tipo": "ente_no_encontrado",
                        "mensaje": alerta,
                        "hoja": hoja,
                        "archivo": nombre_archivo
                    })
                    continue

                df = xl.parse(hoja).rename(columns=lambda x: str(x).strip().upper().replace(" ", "_"))
                columnas_base = {"RFC", "NOMBRE", "PUESTO", "FECHA_ALTA", "FECHA_BAJA"}

                if not columnas_base.issubset(df.columns):
                    alerta = f"⚠️ Hoja '{hoja}' omitida: faltan columnas requeridas."
                    print(alerta)
                    alertas.append({
                        "tipo": "columnas_faltantes",
                        "mensaje": alerta,
                        "hoja": hoja,
                        "archivo": nombre_archivo
                    })
                    continue

                qnas = [c for c in df.columns if re.match(r"^QNA([1-9]|1[0-9]|2[0-4])$", c)]
                registros_validos = 0

                for _, row in df.iterrows():
                    rfc = self.limpiar_rfc(row.get("RFC"))
                    if not rfc:
                        continue

                    qnas_activas = {q: row.get(q) for q in qnas if self._es_activo(row.get(q))}

                    entes_rfc[rfc].append({
                        "ente": clave_ente,
                        "nombre": str(row.get("NOMBRE", "")).strip(),
                        "puesto": str(row.get("PUESTO", "")).strip(),
                        "fecha_ingreso": self.limpiar_fecha(row.get("FECHA_ALTA")),
                        "fecha_egreso": self.limpiar_fecha(row.get("FECHA_BAJA")),
                        "qnas": qnas_activas,
                        "monto": row.get("TOT_PERC"),
                    })
                    registros_validos += 1

                print(f"✅ Hoja '{hoja}': {registros_validos} registros procesados.")

        resultados = self._cruces_quincenales(entes_rfc)
        sin_cruce = self._empleados_sin_cruce(entes_rfc, resultados)
        resultados.extend(sin_cruce)

        print(f"📈 {len(resultados)} registros totales (incluye no duplicados).")
        return resultados, alertas

    # -------------------------------------------------------
    # Empleados sin cruce
    # -------------------------------------------------------
    def _empleados_sin_cruce(self, entes_rfc, hallazgos):
        hallados = {h["rfc"] for h in hallazgos}
        faltantes = []
        for rfc, registros in entes_rfc.items():
            if rfc in hallados:
                continue
            faltantes.append({
                "rfc": rfc,
                "nombre": registros[0].get("nombre", ""),
                "entes": sorted({r["ente"] for r in registros}),
                "tipo_patron": "SIN_DUPLICIDAD",
                "descripcion": "Empleado sin cruce detectado",
                "registros": registros,
                "estado": "Sin valoración",
                "solventacion": ""
            })
        return faltantes

    # -------------------------------------------------------
    # Cruces: VERSIÓN CORREGIDA
    # -------------------------------------------------------
    def _es_activo(self, valor):
        if pd.isna(valor):
            return False
        s = str(valor).strip().upper()
        return s not in {"", "0", "0.0", "NO", "N/A", "NA", "NONE"}

    def _cruces_quincenales(self, entes_rfc):
        hallazgos = []
        año_actual = datetime.now().year

        for rfc, registros in entes_rfc.items():
            # Verificar si hay al menos 2 registros (diferentes entes)
            if len(registros) < 2:
                continue

            # Mapear QNAs por ente para detectar cruces
            qna_map = defaultdict(list)

            for reg in registros:
                for qna, valor in reg["qnas"].items():
                    if self._es_activo(valor):
                        qna_map[qna].append(reg)

            # Verificar si hay al menos una QNA con cruce real (2+ entes)
            qnas_con_cruce = []
            entes_involucrados = set()

            for qna, regs_activos in qna_map.items():
                entes_en_qna = {r["ente"] for r in regs_activos}
                if len(entes_en_qna) > 1:
                    qnas_con_cruce.append(qna)
                    entes_involucrados.update(entes_en_qna)

            # Si NO hay cruces reales, saltar este RFC
            if not qnas_con_cruce:
                continue

            # Crear UN SOLO hallazgo consolidado para este RFC
            # Incluir todos los entes involucrados en cualquier cruce
            entes_list = sorted(list(entes_involucrados))

            hallazgos.append({
                "rfc": rfc,
                "nombre": registros[0].get("nombre", ""),
                "entes": entes_list,
                "qnas_cruce": sorted(qnas_con_cruce),  # Lista de QNAs con cruce
                "tipo_patron": "CRUCE_ENTRE_ENTES_QNA",
                "descripcion": f"Activo en {len(entes_list)} entes durante {len(qnas_con_cruce)} quincena(s) simultáneas.",
                "registros": registros,  # TODOS los registros del RFC
                "estado": "Sin valoración",
                "solventacion": ""
            })

        return hallazgos


_db_manager = None


def set_db_manager(db_manager):
    global _db_manager
    _db_manager = db_manager
    _entes_cache.cache_clear()


def ordenar_quincenas(qnas):
    """Ordena quincenas (QNA1, QNA2, ..., QNA24) numéricamente."""
    if not qnas:
        return []

    def extraer_numero(qna):
        match = re.search(r"\d+", str(qna))
        return int(match.group()) if match else 0

    return sorted(qnas, key=extraer_numero)


def _sanitize_text(s):
    return _normalize_ente_alias(s)


def _allowed_all(entes_usuario):
    """
    Devuelve:
    - 'ALL'         → ENTES + MUNICIPIOS
    - 'ENTES'       → Solo entes
    - 'MUNICIPIOS'  → Solo municipios
    - None          → Sin acceso especial
    """
    tiene_todos = False
    tiene_entes = False
    tiene_munis = False

    for e in entes_usuario:
        s = _sanitize_text(e)
        if s == "TODOS":
            tiene_todos = True
        if "TODOS" in s and "ENTE" in s:
            tiene_entes = True
        if "TODOS" in s and "MUNICIP" in s:
            tiene_munis = True

    if tiene_todos or (tiene_entes and tiene_munis):
        return "ALL"
    if tiene_entes:
        return "ENTES"
    if tiene_munis:
        return "MUNICIPIOS"
    return None


def _estatus_label(v):
    v = (v or "").strip().lower()
    if not v:
        return "Sin valoración"
    if "no" in v:
        return "No Solventado"
    if "solvent" in v:
        return "Solventado"
    return "Sin valoración"


@lru_cache(maxsize=1)
def _entes_cache():
    """
    Devuelve diccionario unificado de ENTES + MUNICIPIOS:
    { clave_normalizada: {siglas, nombre, tipo} }
    """
    if _db_manager is None:
        raise RuntimeError("Database manager not set")

    conn = _db_manager._connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT clave, siglas, nombre, 'ENTE' AS tipo FROM entes
        UNION ALL
        SELECT clave, siglas, nombre, 'MUNICIPIO' AS tipo FROM municipios
    """)

    data = {}
    for r in cur.fetchall():
        clave = _sanitize_text(r["clave"])
        siglas_raw = str(r["siglas"] or "").strip()
        nombre_raw = str(r["nombre"] or "").strip()
        data[clave] = {
            "siglas": _sanitize_text(siglas_raw),
            "nombre": _sanitize_text(nombre_raw),
            "siglas_raw": siglas_raw,
            "nombre_raw": nombre_raw,
            "tipo": r["tipo"]
        }

    conn.close()
    return data


def _ente_match(ente_usuario, clave_lista):
    """
    Permisos correctos:
    - El usuario puede tener sigla (ACUAMANALA) y el registro tener clave (MUN_1)
    - O nombre, o clave directamente.
    """
    euser = _sanitize_text(ente_usuario)

    for c in clave_lista:
        c_norm = _sanitize_text(c)

        for k, d in _entes_cache().items():
            if euser in {d["siglas"], d["nombre"], k}:
                if c_norm in {d["siglas"], d["nombre"], k}:
                    return True

    return False


def _ente_sigla(clave):
    if not clave:
        return ""
    s = _sanitize_text(clave)
    for k, d in _entes_cache().items():
        if s in {k, d["siglas"], d["nombre"]}:
            return d.get("siglas_raw") or d.get("nombre_raw") or d["siglas"] or d["nombre"] or s
    return s


def _ente_display(v):
    if not v:
        return "Sin Ente"
    s = _sanitize_text(v)
    for k, d in _entes_cache().items():
        if s in {k, d["siglas"], d["nombre"]}:
            return d.get("siglas_raw") or d.get("nombre_raw") or d["siglas"] or d["nombre"] or v
    return v


def _filtrar_duplicados_reales(resultados):
    """
    Filtra resultados para incluir SOLO registros con duplicidad real:
    - Mismos RFC en múltiples entes
    - Con intersección de QNAs (mismo periodo activo en ambos entes)
    """
    resultados_filtrados = []

    for r in resultados:
        registros_rfc = r.get("registros", [])
        qnas_por_ente = {}

        for reg in registros_rfc:
            ente = reg.get("ente")
            qnas = set(reg.get("qnas", {}).keys())
            qnas_por_ente[ente] = qnas

        duplicidad_real = False
        entes_cruce_real = set()

        entes_lista = list(qnas_por_ente.keys())
        for i in range(len(entes_lista)):
            for j in range(i + 1, len(entes_lista)):
                e1, e2 = entes_lista[i], entes_lista[j]
                if qnas_por_ente[e1].intersection(qnas_por_ente[e2]):
                    duplicidad_real = True
                    entes_cruce_real.update([e1, e2])

        if not duplicidad_real:
            continue

        r_filtrado = r.copy()
        r_filtrado["entes_cruce_real"] = list(entes_cruce_real)
        resultados_filtrados.append(r_filtrado)

    return resultados_filtrados


def _construir_filas_export(resultados):
    if _db_manager is None:
        raise RuntimeError("Database manager not set")

    agregados = {}
    for r in resultados:
        registros = r.get("registros") or []

        qnas_por_ente = {}
        for reg in registros:
            ente = reg.get("ente")
            qnas = set(reg.get("qnas", {}).keys())
            qnas_por_ente[ente] = qnas

        for reg in registros:
            ente_origen = reg.get("ente") or "Sin Ente"
            key = (
                r.get("rfc"),
                _sanitize_text(ente_origen),
                reg.get("puesto"),
                reg.get("fecha_ingreso"),
                reg.get("fecha_egreso"),
                reg.get("monto"),
            )

            if key not in agregados:
                agregados[key] = {
                    "RFC": r.get("rfc"),
                    "Nombre": r.get("nombre"),
                    "Puesto": reg.get("puesto"),
                    "Fecha Alta": reg.get("fecha_ingreso"),
                    "Fecha Baja": reg.get("fecha_egreso"),
                    "Total Percepciones": reg.get("monto"),
                    "Ente Origen": _ente_display(ente_origen),
                    "_ente_origen_raw": ente_origen,
                    "_entes_incomp_set": set(),
                    "_qnas_set": set(),
                    "_estado_base": _estatus_label(r.get("estado")),
                    "_solventacion": r.get("solventacion", "")
                }

            qnas_ente_actual = qnas_por_ente.get(ente_origen, set())

            for otro_ente, qnas_otro in qnas_por_ente.items():
                if _sanitize_text(otro_ente) != _sanitize_text(ente_origen):
                    interseccion = qnas_ente_actual.intersection(qnas_otro)
                    if interseccion:
                        agregados[key]["_entes_incomp_set"].add(otro_ente)
                        for qna in interseccion:
                            qnum = qna.replace("QNA", "").strip()
                            if qnum.isdigit():
                                agregados[key]["_qnas_set"].add(int(qnum))

    conn = _db_manager._connect()
    cur = conn.cursor()
    cur.execute("SELECT rfc, ente, comentario FROM solventaciones")
    comentarios = cur.fetchall()
    conn.close()

    mapa_coment = {
        (c["rfc"], c["ente"]): c["comentario"]
        for c in comentarios
    }

    filas = []
    for key, item in agregados.items():
        if len(item["_qnas_set"]) >= 24:
            quincenas = "Activo en Todo el Ejercicio"
        elif item["_qnas_set"]:
            quincenas = ", ".join(f"QNA{q}" for q in sorted(item["_qnas_set"]))
        else:
            quincenas = "N/A"

        entes_incomp = ", ".join(
            sorted({_ente_sigla(e) for e in item["_entes_incomp_set"]})
        ) or "Sin otros entes"

        ente_clave = _db_manager.normalizar_ente_clave(item["_ente_origen_raw"])
        est_ente = _db_manager.get_estado_rfc_ente(item["RFC"], ente_clave)
        est_final = est_ente or item["_estado_base"]

        comentario_real = mapa_coment.get((item["RFC"], ente_clave))
        solventacion_final = comentario_real or item["_solventacion"]

        filas.append({
            "RFC": item["RFC"],
            "Nombre": item["Nombre"],
            "Puesto": item["Puesto"],
            "Fecha Alta": item["Fecha Alta"],
            "Fecha Baja": item["Fecha Baja"],
            "Total Percepciones": item["Total Percepciones"],
            "Ente Origen": item["Ente Origen"],
            "Entes Incompatibilidad": entes_incomp,
            "Quincenas": quincenas,
            "Estatus": est_final,
            "Solventación": solventacion_final
        })
    return filas
