# ===========================================================
# app.py — SASP / SCIL 2025
# Sistema de Auditoría de Servicios Personales
# Órgano de Fiscalización Superior del Congreso del Estado de Tlaxcala
# ===========================================================

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, send_file, send_from_directory
)
from werkzeug.exceptions import RequestEntityTooLarge
import os
import sys
import logging
import json
import pandas as pd
from io import BytesIO
from itertools import combinations
from datetime import datetime, date
from scripts.utils import (
    DataProcessor,
    DatabaseManager,
    ordenar_quincenas,
    set_db_manager,
    _allowed_all,
    _construir_filas_export,
    _ente_display,
    _ente_match,
    _ente_sigla,
    _entes_cache,
    _estatus_label,
    _filtrar_duplicados_reales,
    _sanitize_text,
)

# -----------------------------------------------------------
# Logging
# -----------------------------------------------------------
from pathlib import Path
import config
from logging.handlers import RotatingFileHandler

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from shared_user_catalog import ordered_users

log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        RotatingFileHandler('logs/app.log', maxBytes=10*1024*1024, backupCount=10),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("SCIL")

# -----------------------------------------------------------
# Configuración
# -----------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY")
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("MAX_CONTENT_LENGTH", 50 * 1024 * 1024))

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.environ.get("SCIL_DB", str(BASE_DIR / "scil.db"))
db_manager = DatabaseManager(DB_PATH)
set_db_manager(db_manager)
data_processor = DataProcessor(db_manager=db_manager)

log.info("Iniciando SCIL | CWD=%s | DB=%s", os.getcwd(), DB_PATH)

# -----------------------------------------------------------
# Filtros de Jinja2
# -----------------------------------------------------------
app.add_template_filter(ordenar_quincenas, "ordenar_quincenas")

# -----------------------------------------------------------
# Middleware
# -----------------------------------------------------------
@app.before_request
def verificar_autenticacion():
    libres = {"login", "static", "health_check"}
    if request.endpoint not in libres and not session.get("autenticado"):
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"error": "Sesión expirada o no autorizada"}), 403
        return redirect(url_for("login"))


@app.errorhandler(RequestEntityTooLarge)
def manejar_archivo_muy_grande(_error):
    msg = (
        "El archivo o conjunto de archivos excede el límite permitido de carga "
        f"({app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB)."
    )
    if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"error": msg}), 413
    return msg, 413

# -----------------------------------------------------------
# HEALTHCHECK
# -----------------------------------------------------------
@app.route("/api/health")
@app.route("/health")
def health_check():
    return jsonify({"status": "ok", "service": "sasp"}), 200


# -----------------------------------------------------------
# LOGIN / LOGOUT
# -----------------------------------------------------------
LOGIN_USER_PRIORITY = {"luis": 0, "odilia": 1, "felipe": 2, "gabo": 3}
WEEK_DAYS = [
    (0, "Lunes"),
    (1, "Martes"),
    (2, "Miércoles"),
    (3, "Jueves"),
    (4, "Viernes"),
    (5, "Sábado"),
    (6, "Domingo"),
]


def _safe_next_url(raw_url):
    url = str(raw_url or "").strip()
    if not url.startswith("/") or url.startswith("//"):
        return ""
    return url


def _normalize_login_display(value):
    return " ".join(str(value or "").strip().lower().split())


def _get_login_users():
    users = ordered_users("05-sasp", priority=LOGIN_USER_PRIORITY)
    deduped_users = []
    seen_displays = set()

    for user in users:
        display_key = _normalize_login_display(user.get("display_name") or user.get("username"))
        if display_key and display_key in seen_displays:
            continue
        if display_key:
            seen_displays.add(display_key)
        deduped_users.append(user)

    return deduped_users


def _normalize_session_entes(entes):
    entes_norm = []
    for ente in entes or []:
        ente_txt = str(ente or "").strip()
        if not ente_txt:
            continue
        if _sanitize_text(ente_txt) == "TODOS":
            return ["TODOS"]
        clave_norm = db_manager.normalizar_ente_clave(ente_txt)
        entes_norm.append(clave_norm or ente_txt)
    return entes_norm


def _build_dashboard_context():
    conn = db_manager._connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) AS total_registros,
            COUNT(DISTINCT rfc) AS total_rfcs,
            COUNT(DISTINCT ente) AS entes_detectados,
            MAX(fecha_actualizacion) AS ultima_actualizacion
        FROM registros_laborales
    """)
    row = cur.fetchone() or {}
    conn.close()

    catalog_rows = db_manager._get_catalog_snapshot()["rows"]
    dashboard_entes = []
    seen_catalog_keys = set()
    for row_meta in catalog_rows:
        clave = str(row_meta.get("clave") or "").strip()
        if not clave or clave in seen_catalog_keys:
            continue
        seen_catalog_keys.add(clave)
        dashboard_entes.append({
            "clave": clave,
            "nombre": str(row_meta.get("nombre") or "").strip(),
            "siglas": str(row_meta.get("siglas") or "").strip(),
            "tipo": str(row_meta.get("tipo_tabla") or "").strip(),
        })

    total_registros = int(row["total_registros"] or 0)
    entes_detectados = int(row["entes_detectados"] or 0)
    resultados_validados = db_manager.resultados_validados()
    ultima_actualizacion = row["ultima_actualizacion"] or ""

    if total_registros:
        carga_estado = "completada"
        carga_texto = "Base operativa con registros cargados."
        cruce_estado = "lista"
        cruce_texto = "Cruces por RFC disponibles para revisión."
        observaciones_estado = "lista"
        observaciones_texto = "Monitoreo listo para advertencias y hallazgos de carga."
        guardado_estado = "completada" if resultados_validados else "lista"
        guardado_texto = (
            "Resultados marcados como validados."
            if resultados_validados
            else "Datos operativos guardados y listos para cierre."
        )
    else:
        carga_estado = "pendiente"
        carga_texto = "Seleccione archivos para iniciar la actualización."
        cruce_estado = "pendiente"
        cruce_texto = "Se habilita cuando existan registros operativos."
        observaciones_estado = "pendiente"
        observaciones_texto = "Aparecen cuando el sistema detecta incidencias o advertencias."
        guardado_estado = "pendiente"
        guardado_texto = "Se activa después de procesar una carga válida."

    dashboard_crosses = _build_dashboard_cross_summary()
    processed_files = _build_dashboard_processed_files()
    processed_drawer = _build_dashboard_processed_drawer()
    horarios_preview = _build_dashboard_horarios_preview()

    return {
        "dashboard_summary": {
            "total_registros": total_registros,
            "total_rfcs": int(row["total_rfcs"] or 0),
            "entes_detectados": entes_detectados,
            "ultima_actualizacion": ultima_actualizacion,
            "resultados_validados": resultados_validados,
        },
        "dashboard_monitor": {
            "observaciones_total": 0,
            "observaciones_error": 0,
            "observaciones_warning": 0,
            "observaciones_info": 0,
            "observaciones_estado": "Sin observaciones",
            "guardados_total": total_registros,
            "procesados_total": total_registros,
            "pendientes_total": 0,
            "errores_total": 0,
            "guardado_estado": (
                "Validado" if resultados_validados else ("Base cargada" if total_registros else "Sin actividad")
            ),
            "ultima_actualizacion": ultima_actualizacion,
        },
        "dashboard_steps": [
            {
                "id": "carga",
                "title": "Carga",
                "status": carga_estado,
                "description": carga_texto,
            },
            {
                "id": "cruce",
                "title": "Cruce y validación",
                "status": cruce_estado,
                "description": cruce_texto,
            },
            {
                "id": "observaciones",
                "title": "Observaciones",
                "status": observaciones_estado,
                "description": observaciones_texto,
            },
            {
                "id": "guardado",
                "title": "Guardado y cierre",
                "status": guardado_estado,
                "description": guardado_texto,
            },
        ],
        "dashboard_entes": sorted(
            dashboard_entes,
            key=lambda item: (
                item["tipo"] != "ENTE",
                (item["siglas"] or item["nombre"] or item["clave"]).lower(),
            ),
        ),
        "dashboard_crosses": dashboard_crosses,
        "dashboard_processed_files": processed_files,
        "dashboard_processed_drawer": processed_drawer,
        "dashboard_horarios_preview": horarios_preview,
        "dashboard_horarios_status": _build_dashboard_horarios_status(),
    }


def _build_dashboard_cross_summary():
    catalogo = db_manager.listar_entes() + db_manager.listar_municipios()
    catalogo_index = _indexar_catalogo(catalogo)
    cruces = db_manager.obtener_cruces_reales()

    summary = {
        "total": len(cruces),
        "ente_ente": {"label": "Ente - ente", "count": 0, "examples": []},
        "ente_municipio": {"label": "Ente - municipio", "count": 0, "examples": []},
        "municipio_municipio": {"label": "Municipio - municipio", "count": 0, "examples": []},
    }

    def _cross_bucket(entes):
        tipos = []
        for ente_ref in entes or []:
            meta = catalogo_index.get(_sanitize_text(ente_ref)) or {}
            tipo = str(meta.get("ambito") or _tipo_ente(ente_ref)).upper()
            tipos.append("MUNICIPIO" if "MUNIC" in tipo else "ENTE")
        if tipos and all(tipo == "MUNICIPIO" for tipo in tipos):
            return "municipio_municipio"
        if "MUNICIPIO" in tipos and "ENTE" in tipos:
            return "ente_municipio"
        return "ente_ente"

    for cruce in cruces:
        bucket = _cross_bucket(cruce.get("entes") or [])
        summary[bucket]["count"] += 1
        if len(summary[bucket]["examples"]) >= 4:
            continue
        entes_display = [_ente_display(ente_ref) for ente_ref in (cruce.get("entes") or [])]
        summary[bucket]["examples"].append({
            "rfc": str(cruce.get("rfc", "")).strip(),
            "nombre": str(cruce.get("nombre", "")).strip(),
            "entes": entes_display,
            "qnas": list(cruce.get("qnas_cruce") or []),
            "descripcion": str(cruce.get("descripcion", "")).strip(),
        })

    return summary


def _build_dashboard_processed_files():
    conn = db_manager._connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            ente,
            COUNT(*) AS total_registros,
            COUNT(DISTINCT rfc) AS total_rfcs,
            MAX(fecha_actualizacion) AS ultima_actualizacion
        FROM registros_laborales
        GROUP BY ente
        ORDER BY MAX(fecha_actualizacion) DESC, ente
        LIMIT 8
    """)
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return [
        {
            "ente": row["ente"],
            "ente_display": _ente_display(row["ente"]),
            "total_registros": int(row["total_registros"] or 0),
            "total_rfcs": int(row["total_rfcs"] or 0),
            "ultima_actualizacion": row["ultima_actualizacion"] or "",
        }
        for row in rows
    ]


def _build_dashboard_processed_drawer():
    conn = db_manager._connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            ente,
            COUNT(*) AS total_registros,
            COUNT(DISTINCT rfc) AS total_rfcs,
            MAX(fecha_actualizacion) AS ultima_actualizacion
        FROM registros_laborales
        GROUP BY ente
        ORDER BY MAX(fecha_actualizacion) DESC, ente
        LIMIT 12
    """)
    trabajadores = [{
        "label": f"Carga {_ente_display(row['ente'])}",
        "secondary": f"{int(row['total_registros'] or 0)} registros · {int(row['total_rfcs'] or 0)} RFC",
        "fecha": row["ultima_actualizacion"] or "",
        "tipo": "Trabajadores",
        "href": url_for("reporte_por_ente", ente=_ente_display(row["ente"])),
        "cta": "Ver",
    } for row in cur.fetchall()]

    cur.execute("""
        SELECT
            id,
            rfc,
            nombre,
            ente,
            hora_inicio,
            hora_fin,
            actualizado,
            creado
        FROM horarios_persona
        ORDER BY COALESCE(actualizado, creado) DESC, id DESC
        LIMIT 12
    """)
    horarios = [{
        "label": str(row["nombre"] or row["rfc"] or "Horario").strip(),
        "secondary": f"{_ente_display(row['ente'])} · {str(row['hora_inicio'] or '').strip()} - {str(row['hora_fin'] or '').strip()}",
        "fecha": row["actualizado"] or row["creado"] or "",
        "tipo": "Horarios",
        "href": url_for("horarios_home", rfc=row["rfc"]),
        "cta": "Abrir",
    } for row in cur.fetchall()]
    conn.close()

    return {
        "trabajadores": trabajadores,
        "horarios": horarios,
    }


def _build_dashboard_horarios_preview():
    horarios = _filtrar_horarios_visibles(db_manager.listar_horarios_persona())
    preview = []
    for horario in horarios[:8]:
        dia_semana = int(horario.get("dia_semana", 0))
        preview.append({
            "id": horario.get("id"),
            "rfc": horario.get("rfc"),
            "nombre": horario.get("nombre"),
            "ente": horario.get("ente"),
            "ente_display": _ente_display(horario.get("ente")),
            "dia_label": WEEK_DAYS[dia_semana][1] if 0 <= dia_semana < len(WEEK_DAYS) else "",
            "horario": f"{horario.get('hora_inicio', '')} - {horario.get('hora_fin', '')}",
            "estatus": horario.get("estatus") or "activo",
        })
    return preview


def _build_dashboard_horarios_status():
    horarios = _filtrar_horarios_visibles(db_manager.listar_horarios_persona())
    if not horarios:
        return {"total": 0, "ultima_actualizacion": "", "estado": "Sin archivo cargado"}
    ultima_actualizacion = max(str(item.get("actualizado") or item.get("creado") or "") for item in horarios)
    return {
        "total": len(horarios),
        "ultima_actualizacion": ultima_actualizacion,
        "estado": "Archivo cargado",
    }


def _normalize_upload_header(value):
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
        .replace("ñ", "n")
    )


def _dia_semana_desde_valor(value):
    texto = _normalize_upload_header(value).replace(".", "").replace(",", "")
    if texto == "":
        return None
    if texto.isdigit():
        numero = int(texto)
        if 0 <= numero <= 6:
            return numero
        if 1 <= numero <= 7:
            return numero - 1
        return None
    mapa = {
        "lunes": 0,
        "martes": 1,
        "miercoles": 2,
        "jueves": 3,
        "viernes": 4,
        "sabado": 5,
        "domingo": 6,
    }
    return mapa.get(texto)


def _hora_texto_desde_valor(value):
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%H:%M")
        except Exception:
            pass
    texto = str(value).strip()
    hora = _parse_time(texto)
    return hora.strftime("%H:%M") if hora else texto


def _fecha_texto_desde_valor(value):
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    texto = str(value).strip()
    fecha = _parse_date(texto)
    return fecha.isoformat() if fecha else texto


def _extract_horarios_from_files(files):
    column_aliases = {
        "rfc": {"rfc"},
        "nombre": {"nombre", "persona", "trabajador", "profesor", "servidor publico"},
        "ente": {"ente", "institucion", "dependencia", "municipio"},
        "cargo": {"cargo", "plaza", "puesto", "asignatura", "descripcion", "descripcion del servicio", "servicio"},
        "dia_semana": {"dia", "dia_semana", "dia de la semana"},
        "hora_inicio": {"hora_inicio", "hora de inicio", "inicio", "entrada", "hora entrada"},
        "hora_fin": {"hora_fin", "hora de fin", "fin", "salida", "hora salida"},
        "fecha_inicio_vigencia": {"fecha_inicio_vigencia", "fecha inicio", "inicio vigencia", "vigencia inicio"},
        "fecha_fin_vigencia": {"fecha_fin_vigencia", "fecha fin", "fin vigencia", "vigencia fin"},
        "periodo": {"periodo", "ciclo", "quincena"},
        "observaciones": {"observaciones", "observacion", "comentarios", "comentario"},
        "estatus": {"estatus", "estado"},
    }

    horarios = []
    alertas = []

    for file_storage in files:
        nombre_archivo = getattr(file_storage, "filename", "horarios.xlsx")
        try:
            excel = pd.ExcelFile(file_storage)
        except Exception as exc:
            alertas.append({"archivo": nombre_archivo, "mensaje": f"No fue posible leer el archivo de horarios: {exc}"})
            continue

        for sheet_name in excel.sheet_names:
            try:
                df = pd.read_excel(excel, sheet_name=sheet_name)
            except Exception as exc:
                alertas.append({"archivo": nombre_archivo, "mensaje": f"No fue posible leer la hoja '{sheet_name}': {exc}"})
                continue

            if df.empty:
                continue

            normalized_columns = {_normalize_upload_header(column): column for column in df.columns}
            resolved = {}
            for target, aliases in column_aliases.items():
                for alias in aliases:
                    if alias in normalized_columns:
                        resolved[target] = normalized_columns[alias]
                        break

            required = ["rfc", "ente", "dia_semana", "hora_inicio", "hora_fin", "fecha_inicio_vigencia"]
            missing = [field for field in required if field not in resolved]
            if missing:
                alertas.append({
                    "archivo": nombre_archivo,
                    "mensaje": f"La hoja '{sheet_name}' no contiene columnas requeridas: {', '.join(missing)}.",
                })
                continue

            for _, row in df.iterrows():
                rfc = str(row.get(resolved["rfc"], "") or "").strip().upper()
                ente = str(row.get(resolved["ente"], "") or "").strip()
                dia_semana = _dia_semana_desde_valor(row.get(resolved["dia_semana"]))
                hora_inicio = _hora_texto_desde_valor(row.get(resolved["hora_inicio"]))
                hora_fin = _hora_texto_desde_valor(row.get(resolved["hora_fin"]))
                fecha_inicio_vigencia = _fecha_texto_desde_valor(row.get(resolved["fecha_inicio_vigencia"]))

                if not rfc or not ente or dia_semana is None or not hora_inicio or not hora_fin or not fecha_inicio_vigencia:
                    continue

                horarios.append({
                    "rfc": rfc,
                    "nombre": str(row.get(resolved.get("nombre", ""), "") or "").strip(),
                    "ente": ente,
                    "cargo": str(row.get(resolved.get("cargo", ""), "") or "").strip(),
                    "dia_semana": dia_semana,
                    "hora_inicio": hora_inicio,
                    "hora_fin": hora_fin,
                    "fecha_inicio_vigencia": fecha_inicio_vigencia,
                    "fecha_fin_vigencia": _fecha_texto_desde_valor(row.get(resolved.get("fecha_fin_vigencia", ""), "")),
                    "periodo": str(row.get(resolved.get("periodo", ""), "") or "").strip(),
                    "observaciones": str(row.get(resolved.get("observaciones", ""), "") or "").strip(),
                    "estatus": str(row.get(resolved.get("estatus", ""), "") or "activo").strip().lower() or "activo",
                    "origen": "archivo",
                })

    return horarios, alertas


def _demo_qnas(*numeros):
    return {f"QNA{int(numero)}": 1 for numero in numeros}


def _build_demo_records():
    return [
        {
            "rfc": "DEMJ800101AAA",
            "ente": "SEPE",
            "nombre": "JUAN PEREZ DEMO",
            "puesto": "ANALISTA ADMINISTRATIVO",
            "fecha_ingreso": "2024-01-01",
            "fecha_egreso": None,
            "monto": 182450.0,
            "qnas": _demo_qnas(1, 2, 3),
        },
        {
            "rfc": "DEMJ800101AAA",
            "ente": "USET",
            "nombre": "JUAN PEREZ DEMO",
            "puesto": "ENLACE OPERATIVO",
            "fecha_ingreso": "2024-01-15",
            "fecha_egreso": None,
            "monto": 169880.0,
            "qnas": _demo_qnas(2, 3, 4),
        },
        {
            "rfc": "DEMM820202BBB",
            "ente": "USET",
            "nombre": "MARIA LOPEZ DEMO",
            "puesto": "CAPTURISTA",
            "fecha_ingreso": "2024-02-01",
            "fecha_egreso": None,
            "monto": 138400.0,
            "qnas": _demo_qnas(5, 6),
        },
        {
            "rfc": "DEMM820202BBB",
            "ente": "TOCATLÁN",
            "nombre": "MARIA LOPEZ DEMO",
            "puesto": "AUXILIAR CONTABLE",
            "fecha_ingreso": "2024-02-16",
            "fecha_egreso": None,
            "monto": 94450.0,
            "qnas": _demo_qnas(6, 7),
        },
        {
            "rfc": "DEMP830303CCC",
            "ente": "APIZACO",
            "nombre": "PEDRO HERNANDEZ DEMO",
            "puesto": "SUPERVISOR",
            "fecha_ingreso": "2024-03-01",
            "fecha_egreso": None,
            "monto": 123000.0,
            "qnas": _demo_qnas(8, 9),
        },
        {
            "rfc": "DEMP830303CCC",
            "ente": "TLAXCALA",
            "nombre": "PEDRO HERNANDEZ DEMO",
            "puesto": "COORDINADOR MUNICIPAL",
            "fecha_ingreso": "2024-03-01",
            "fecha_egreso": None,
            "monto": 117500.0,
            "qnas": _demo_qnas(9, 10),
        },
        {
            "rfc": "DEML840404DDD",
            "ente": "OMG",
            "nombre": "LAURA GARCIA DEMO",
            "puesto": "JEFA DE DEPARTAMENTO",
            "fecha_ingreso": "2024-01-10",
            "fecha_egreso": None,
            "monto": 214320.0,
            "qnas": _demo_qnas(11, 12),
        },
        {
            "rfc": "DEML840404DDD",
            "ente": "SMYT",
            "nombre": "LAURA GARCIA DEMO",
            "puesto": "ASESORA TECNICA",
            "fecha_ingreso": "2024-01-10",
            "fecha_egreso": None,
            "monto": 186250.0,
            "qnas": _demo_qnas(12, 13),
        },
        {
            "rfc": "DEMS850505EEE",
            "ente": "SOTYV",
            "nombre": "SAUL ORTEGA DEMO",
            "puesto": "REVISOR DE OBRA",
            "fecha_ingreso": "2024-04-01",
            "fecha_egreso": None,
            "monto": 145000.0,
            "qnas": _demo_qnas(14, 15),
        },
        {
            "rfc": "DEMS850505EEE",
            "ente": "CHIAUTEMPAN",
            "nombre": "SAUL ORTEGA DEMO",
            "puesto": "SUPERVISOR MUNICIPAL",
            "fecha_ingreso": "2024-04-01",
            "fecha_egreso": None,
            "monto": 90800.0,
            "qnas": _demo_qnas(15, 16),
        },
        {
            "rfc": "DEMA860606FFF",
            "ente": "SEPE",
            "nombre": "ANA MARTINEZ DEMO",
            "puesto": "ENLACE ACADEMICO",
            "fecha_ingreso": "2024-05-01",
            "fecha_egreso": None,
            "monto": 152300.0,
            "qnas": _demo_qnas(17),
        },
        {
            "rfc": "DEMA860606FFF",
            "ente": "USET",
            "nombre": "ANA MARTINEZ DEMO",
            "puesto": "COORDINADORA EDUCATIVA",
            "fecha_ingreso": "2024-05-01",
            "fecha_egreso": None,
            "monto": 161700.0,
            "qnas": _demo_qnas(17, 18),
        },
        {
            "rfc": "DEMA860606FFF",
            "ente": "HUAMANTLA",
            "nombre": "ANA MARTINEZ DEMO",
            "puesto": "ASESORA EXTERNA",
            "fecha_ingreso": "2024-05-15",
            "fecha_egreso": None,
            "monto": 87000.0,
            "qnas": _demo_qnas(18),
        },
        {
            "rfc": "DEMN870707GGG",
            "ente": "SSC",
            "nombre": "NORA CASTILLO DEMO",
            "puesto": "ANALISTA DE CONTROL",
            "fecha_ingreso": "2024-06-01",
            "fecha_egreso": None,
            "monto": 132000.0,
            "qnas": _demo_qnas(19, 20),
        },
        {
            "rfc": "DEMN870707GGG",
            "ente": "APIZACO",
            "nombre": "NORA CASTILLO DEMO",
            "puesto": "CONSULTORA",
            "fecha_ingreso": "2024-06-15",
            "fecha_egreso": None,
            "monto": 65000.0,
            "qnas": _demo_qnas(21, 22),
        },
    ]


def _seed_demo_records():
    demo_records = []
    for record in _build_demo_records():
        ente_clave = db_manager.normalizar_ente_clave(record["ente"]) or record["ente"]
        demo_records.append({**record, "ente": ente_clave})
    return db_manager.guardar_registros_individuales(demo_records)


def _parse_date(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_time(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def hay_traslape_de_horas(inicio_a, fin_a, inicio_b, fin_b):
    return inicio_a < fin_b and inicio_b < fin_a


def hay_traslape_de_fechas(inicio_a, fin_a, inicio_b, fin_b):
    fecha_fin_a = fin_a or date.max
    fecha_fin_b = fin_b or date.max
    return inicio_a <= fecha_fin_b and inicio_b <= fecha_fin_a


def calcular_minutos_traslapados(inicio_a, fin_a, inicio_b, fin_b):
    dt_base = date(2000, 1, 1)
    left_start = datetime.combine(dt_base, inicio_a)
    left_end = datetime.combine(dt_base, fin_a)
    right_start = datetime.combine(dt_base, inicio_b)
    right_end = datetime.combine(dt_base, fin_b)
    overlap_start = max(left_start, right_start)
    overlap_end = min(left_end, right_end)
    minutes = int((overlap_end - overlap_start).total_seconds() // 60)
    return max(minutes, 0)


def calcular_severidad_cruce(minutos_traslape):
    if minutos_traslape > 120:
        return "alta"
    if minutos_traslape >= 31:
        return "media"
    if minutos_traslape >= 1:
        return "baja"
    return "sin-cruce"


def generar_observacion_de_cruce(conflicto):
    texto = (
        f"Se detectó que la persona {conflicto['nombre']} cuenta con horarios activos en más de un ente "
        f"durante el mismo día y rango horario. El día {conflicto['dia_label']}, presenta un cruce entre "
        f"{conflicto['ente_a_display']} de {conflicto['hora_inicio_a']} a {conflicto['hora_fin_a']} y "
        f"{conflicto['ente_b_display']} de {conflicto['hora_inicio_b']} a {conflicto['hora_fin_b']}, "
        f"con un traslape aproximado de {conflicto['minutos_traslape']} minutos. "
        f"Se recomienda revisar la compatibilidad del horario y solicitar aclaración documental."
    )
    if conflicto["severidad"] == "alta":
        recomendacion = "Revisión inmediata y requerimiento documental prioritario."
    elif conflicto["severidad"] == "media":
        recomendacion = "Validar soporte institucional y compatibilidad de jornada."
    else:
        recomendacion = "Registrar evidencia y confirmar distribución operativa."
    return texto, recomendacion


def _build_conflicto_hash(conflicto):
    partes = [
        conflicto["rfc"],
        str(conflicto["dia_semana"]),
        "|".join(sorted([conflicto["ente_a"], conflicto["ente_b"]])),
        conflicto["hora_inicio_a"],
        conflicto["hora_fin_a"],
        conflicto["hora_inicio_b"],
        conflicto["hora_fin_b"],
        conflicto["fecha_inicio_a"] or "",
        conflicto["fecha_fin_a"] or "",
        conflicto["fecha_inicio_b"] or "",
        conflicto["fecha_fin_b"] or "",
    ]
    return "|".join(partes)


def detectar_cruces_de_horarios(horarios):
    horarios_activos = []
    for horario in horarios or []:
        if str(horario.get("estatus") or "activo").strip().lower() == "inactivo":
            continue
        hora_inicio = _parse_time(horario.get("hora_inicio"))
        hora_fin = _parse_time(horario.get("hora_fin"))
        fecha_inicio = _parse_date(horario.get("fecha_inicio_vigencia"))
        fecha_fin = _parse_date(horario.get("fecha_fin_vigencia"))
        if not hora_inicio or not hora_fin or not fecha_inicio or hora_inicio >= hora_fin:
            continue
        horarios_activos.append({**horario, "_hora_inicio": hora_inicio, "_hora_fin": hora_fin, "_fecha_inicio": fecha_inicio, "_fecha_fin": fecha_fin})

    conflictos = []
    seen_conflicts = set()
    grouped = {}

    for left, right in combinations(horarios_activos, 2):
        if str(left.get("rfc") or "").strip().upper() != str(right.get("rfc") or "").strip().upper():
            continue
        if int(left.get("dia_semana", -1)) != int(right.get("dia_semana", -1)):
            continue
        if (left.get("ente") or "") == (right.get("ente") or "") and (left.get("permite_traslape_interno") or right.get("permite_traslape_interno")):
            continue
        if not hay_traslape_de_fechas(left["_fecha_inicio"], left["_fecha_fin"], right["_fecha_inicio"], right["_fecha_fin"]):
            continue
        if not hay_traslape_de_horas(left["_hora_inicio"], left["_hora_fin"], right["_hora_inicio"], right["_hora_fin"]):
            continue

        minutos = calcular_minutos_traslapados(left["_hora_inicio"], left["_hora_fin"], right["_hora_inicio"], right["_hora_fin"])
        if minutos <= 0:
            continue

        ente_a, ente_b = sorted([left.get("ente") or "", right.get("ente") or ""])
        first = left if (left.get("ente") or "") == ente_a else right
        second = right if first is left else left

        conflicto = {
            "rfc": str(first.get("rfc") or "").strip().upper(),
            "nombre": str(first.get("nombre") or second.get("nombre") or "").strip(),
            "ente_a": ente_a,
            "ente_b": ente_b,
            "ente_a_display": _ente_display(ente_a),
            "ente_b_display": _ente_display(ente_b),
            "dia_semana": int(first.get("dia_semana", 0)),
            "dia_label": WEEK_DAYS[int(first.get("dia_semana", 0))][1],
            "hora_inicio_a": str(first.get("hora_inicio") or "").strip(),
            "hora_fin_a": str(first.get("hora_fin") or "").strip(),
            "hora_inicio_b": str(second.get("hora_inicio") or "").strip(),
            "hora_fin_b": str(second.get("hora_fin") or "").strip(),
            "cargo_a": str(first.get("cargo") or "").strip(),
            "cargo_b": str(second.get("cargo") or "").strip(),
            "fecha_inicio_a": str(first.get("fecha_inicio_vigencia") or "").strip(),
            "fecha_fin_a": str(first.get("fecha_fin_vigencia") or "").strip(),
            "fecha_inicio_b": str(second.get("fecha_inicio_vigencia") or "").strip(),
            "fecha_fin_b": str(second.get("fecha_fin_vigencia") or "").strip(),
            "periodo_a": str(first.get("periodo") or "").strip(),
            "periodo_b": str(second.get("periodo") or "").strip(),
            "horario_a_id": first.get("id"),
            "horario_b_id": second.get("id"),
            "minutos_traslape": minutos,
            "severidad": calcular_severidad_cruce(minutos),
            "estatus_revision": "pendiente",
        }
        conflicto["conflicto_hash"] = _build_conflicto_hash(conflicto)
        if conflicto["conflicto_hash"] in seen_conflicts:
            continue
        seen_conflicts.add(conflicto["conflicto_hash"])
        texto, recomendacion = generar_observacion_de_cruce(conflicto)
        conflicto["observacion_automatica"] = texto
        conflicto["recomendacion"] = recomendacion
        conflictos.append(conflicto)
        grouped.setdefault(conflicto["rfc"], {
            "rfc": conflicto["rfc"],
            "nombre": conflicto["nombre"],
            "entes": set(),
            "horarios": 0,
            "conflictos": [],
            "severidad_maxima": "baja",
        })
        grouped[conflicto["rfc"]]["entes"].update([conflicto["ente_a_display"], conflicto["ente_b_display"]])
        grouped[conflicto["rfc"]]["conflictos"].append(conflicto)
        grouped[conflicto["rfc"]]["horarios"] += 2
        severidad_actual = {"baja": 1, "media": 2, "alta": 3}
        if severidad_actual.get(conflicto["severidad"], 0) > severidad_actual.get(grouped[conflicto["rfc"]]["severidad_maxima"], 0):
            grouped[conflicto["rfc"]]["severidad_maxima"] = conflicto["severidad"]

    personas = []
    for person in grouped.values():
        personas.append({
            "rfc": person["rfc"],
            "nombre": person["nombre"],
            "entes": sorted(person["entes"]),
            "numero_entes": len(person["entes"]),
            "numero_horarios": len({
                item
                for conflicto in person["conflictos"]
                for item in (conflicto["horario_a_id"], conflicto["horario_b_id"])
            }),
            "numero_conflictos": len(person["conflictos"]),
            "severidad_maxima": person["severidad_maxima"],
            "conflictos": person["conflictos"],
        })

    personas.sort(key=lambda item: (-item["numero_conflictos"], item["nombre"], item["rfc"]))
    conflictos.sort(key=lambda item: ({"alta": 0, "media": 1, "baja": 2}.get(item["severidad"], 3), item["nombre"], item["dia_semana"]))
    return {"conflictos": conflictos, "personas": personas}


def calcular_periodo_quincenal(fecha_valor):
    fecha = _parse_date(fecha_valor) if not isinstance(fecha_valor, date) else fecha_valor
    if not fecha:
        return None
    periodos = db_manager.listar_periodos_quincenales(ejercicio=fecha.year)
    for periodo in periodos:
        inicio = _parse_date(periodo.get("fecha_inicio"))
        fin = _parse_date(periodo.get("fecha_fin"))
        if inicio and fin and inicio <= fecha <= fin:
            return periodo
    return None


def calcular_pago_pdp(registro, periodo_quincenal, conflicto_hash=None):
    sueldo_base = float(registro.get("monto") or 0)
    monto_pdp = round(sueldo_base / 24.0, 2) if sueldo_base else 0.0
    deducciones = float(registro.get("deducciones") or 0)
    percepciones_adicionales = float(registro.get("percepciones_adicionales") or 0)
    total = round(monto_pdp - deducciones + percepciones_adicionales, 2)
    estatus = "observado" if conflicto_hash else "calculado"
    observaciones = "Pago marcado con observación por conflicto de horario en el periodo." if conflicto_hash else ""
    return {
        "rfc": str(registro.get("rfc") or "").strip().upper(),
        "nombre": str(registro.get("nombre") or "").strip(),
        "ente": registro.get("ente"),
        "periodo_quincenal": periodo_quincenal["etiqueta"],
        "fecha_inicio_periodo": periodo_quincenal["fecha_inicio"],
        "fecha_fin_periodo": periodo_quincenal["fecha_fin"],
        "sueldo_base": sueldo_base,
        "monto_pdp": monto_pdp,
        "deducciones": deducciones,
        "percepciones_adicionales": percepciones_adicionales,
        "total_calculado": total,
        "estatus": estatus,
        "observaciones": observaciones,
        "conflicto_hash": conflicto_hash,
    }


def marcar_pago_observado_por_cruce(pago, conflicto_hash):
    pago = dict(pago)
    pago["estatus"] = "observado"
    pago["conflicto_hash"] = conflicto_hash
    pago["observaciones"] = "Pago con observación derivada de conflicto de horario."
    return pago


def _query_param(name, default=""):
    return str(request.args.get(name, default) or default).strip()


def _visible_catalog_rows():
    rows = db_manager._get_catalog_snapshot()["rows"]
    entes_usuario = session.get("entes", [])
    modo_permiso = "ALL" if _es_usuario_luis() else _allowed_all(entes_usuario)
    visibles = []
    for row in rows:
        clave = str(row.get("clave") or "").strip()
        if not clave:
            continue
        if _puede_ver_ente(clave, entes_usuario, modo_permiso):
            visibles.append(row)
    return visibles


def _horarios_filters_from_request():
    filtros = {
        "rfc": _query_param("rfc"),
        "ente": _query_param("ente"),
        "dia_semana": _query_param("dia_semana"),
        "estatus": _query_param("estatus"),
        "nombre": _query_param("nombre"),
        "fecha_desde": _query_param("fecha_desde"),
        "fecha_hasta": _query_param("fecha_hasta"),
    }
    if filtros["dia_semana"] == "":
        filtros["dia_semana"] = None
    return filtros


def _filtrar_horarios_visibles(horarios):
    entes_usuario = session.get("entes", [])
    modo_permiso = "ALL" if _es_usuario_luis() else _allowed_all(entes_usuario)
    visibles = []
    for horario in horarios or []:
        ente = str(horario.get("ente") or "").strip()
        if ente and _puede_ver_ente(ente, entes_usuario, modo_permiso):
            visibles.append(horario)
    return visibles


def _listar_registros_operativos(filtros=None):
    filtros = filtros or {}
    clauses = []
    params = []
    if filtros.get("rfc"):
        clauses.append("UPPER(rfc)=UPPER(?)")
        params.append(str(filtros["rfc"]).strip())
    if filtros.get("ente"):
        ente_clave = db_manager.normalizar_ente_clave(filtros["ente"]) or str(filtros["ente"]).strip()
        clauses.append("ente=?")
        params.append(ente_clave)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    conn = db_manager._connect()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT rfc, ente, nombre, puesto, fecha_ingreso, fecha_egreso, monto, qnas, fecha_actualizacion
        FROM registros_laborales
        {where_sql}
        ORDER BY nombre, rfc, ente
    """, params)
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    for row in rows:
        try:
            row["qnas"] = json.loads(row.get("qnas") or "{}")
        except Exception:
            row["qnas"] = {}
    return rows


def _group_conflictos_por_hash(observaciones):
    return {
        str(item.get("conflicto_hash") or "").strip(): item
        for item in observaciones or []
        if str(item.get("conflicto_hash") or "").strip()
    }


def _sync_observaciones_de_conflictos(conflictos):
    existentes = _group_conflictos_por_hash(db_manager.listar_observaciones_cruce())
    for conflicto in conflictos or []:
        actual = existentes.get(conflicto["conflicto_hash"], {})
        db_manager.upsert_observacion_cruce({
            "conflicto_hash": conflicto["conflicto_hash"],
            "rfc": conflicto["rfc"],
            "nombre": conflicto["nombre"],
            "ente_a": conflicto["ente_a"],
            "ente_b": conflicto["ente_b"],
            "dia_semana": conflicto["dia_semana"],
            "horario_a": f"{conflicto['hora_inicio_a']} - {conflicto['hora_fin_a']}",
            "horario_b": f"{conflicto['hora_inicio_b']} - {conflicto['hora_fin_b']}",
            "minutos_traslape": conflicto["minutos_traslape"],
            "fecha_inicio_a": conflicto["fecha_inicio_a"],
            "fecha_fin_a": conflicto["fecha_fin_a"],
            "fecha_inicio_b": conflicto["fecha_inicio_b"],
            "fecha_fin_b": conflicto["fecha_fin_b"],
            "severidad": conflicto["severidad"],
            "texto_observacion": conflicto["observacion_automatica"],
            "recomendacion": conflicto["recomendacion"],
            "estatus": str(actual.get("estatus") or "pendiente").strip().lower(),
            "comentarios_adicionales": actual.get("comentarios_adicionales", ""),
            "referencia_documental": actual.get("referencia_documental", ""),
            "creado_por": actual.get("creado_por", session.get("usuario", "")),
        }, usuario=session.get("usuario", ""))


def _enriquecer_conflictos_con_observaciones(conflictos):
    observaciones = _group_conflictos_por_hash(db_manager.listar_observaciones_cruce())
    enriched = []
    for conflicto in conflictos or []:
        observacion = observaciones.get(conflicto["conflicto_hash"], {})
        enriched.append({
            **conflicto,
            "estatus_revision": str(observacion.get("estatus") or "pendiente").strip().lower(),
            "comentarios_adicionales": str(observacion.get("comentarios_adicionales") or "").strip(),
            "referencia_documental": str(observacion.get("referencia_documental") or "").strip(),
            "fecha_revision": observacion.get("actualizado") or observacion.get("creado") or "",
        })
    return enriched


def _resumen_observaciones(observaciones):
    conteo = {"pendiente": 0, "revisada": 0, "solventada": 0, "descartada": 0}
    severidad = {"alta": 0, "media": 0, "baja": 0}
    for item in observaciones or []:
        estatus = str(item.get("estatus") or "pendiente").strip().lower()
        sev = str(item.get("severidad") or "baja").strip().lower()
        conteo[estatus] = conteo.get(estatus, 0) + 1
        severidad[sev] = severidad.get(sev, 0) + 1
    return {"estatus": conteo, "severidad": severidad, "total": len(observaciones or [])}


def _conflicto_aplica_a_periodo(conflicto, periodo):
    inicio_periodo = _parse_date(periodo.get("fecha_inicio"))
    fin_periodo = _parse_date(periodo.get("fecha_fin"))
    if not inicio_periodo or not fin_periodo:
        return False
    inicio_a = _parse_date(conflicto.get("fecha_inicio_a"))
    fin_a = _parse_date(conflicto.get("fecha_fin_a"))
    inicio_b = _parse_date(conflicto.get("fecha_inicio_b"))
    fin_b = _parse_date(conflicto.get("fecha_fin_b"))
    return (
        inicio_a
        and inicio_b
        and hay_traslape_de_fechas(inicio_periodo, fin_periodo, inicio_a, fin_a)
        and hay_traslape_de_fechas(inicio_periodo, fin_periodo, inicio_b, fin_b)
    )


def _calcular_pagos_pdp_para_periodo(periodo, filtros=None, persistir=False):
    filtros = filtros or {}
    registros = _listar_registros_operativos(filtros=filtros)
    horarios = _filtrar_horarios_visibles(db_manager.listar_horarios_persona())
    conflictos_data = detectar_cruces_de_horarios(horarios)
    conflictos_periodo = [
        conflicto
        for conflicto in conflictos_data["conflictos"]
        if _conflicto_aplica_a_periodo(conflicto, periodo)
    ]

    conflictos_por_rfc = {}
    for conflicto in conflictos_periodo:
        conflictos_por_rfc.setdefault(conflicto["rfc"], []).append(conflicto)

    pagos = []
    for registro in registros:
        rfc = str(registro.get("rfc") or "").strip().upper()
        conflicto_hash = ""
        for conflicto in conflictos_por_rfc.get(rfc, []):
            if registro.get("ente") in {conflicto.get("ente_a"), conflicto.get("ente_b")}:
                conflicto_hash = conflicto["conflicto_hash"]
                break
        pago = calcular_pago_pdp(registro, periodo, conflicto_hash=conflicto_hash or None)
        pagos.append(pago)
        if persistir:
            db_manager.upsert_pago_pdp(pago, usuario=session.get("usuario", ""))
    return pagos, conflictos_periodo


def _build_horarios_context(filtros, edit_id=None):
    horarios = _filtrar_horarios_visibles(db_manager.listar_horarios_persona(filtros=filtros))
    persona_horarios = {}
    ente_horarios = {}
    for horario in horarios:
        persona_horarios.setdefault(horario["rfc"], []).append(horario)
        ente_horarios.setdefault(horario["ente"], []).append(horario)

    active_rows = [h for h in horarios if str(h.get("estatus") or "activo").lower() == "activo"]
    edit_item = db_manager.obtener_horario_persona(edit_id) if edit_id else None
    if edit_item and not _can_edit_ente(edit_item.get("ente")):
        edit_item = None

    selected_rfc = filtros.get("rfc") or (edit_item or {}).get("rfc") or ""
    persona_detalle = {
        "rfc": selected_rfc,
        "nombre": db_manager.obtener_nombre_persona_por_rfc(selected_rfc) if selected_rfc else "",
        "horarios": persona_horarios.get(selected_rfc, []),
    } if selected_rfc else None

    ente_ref = filtros.get("ente") or ""
    ente_clave = db_manager.normalizar_ente_clave(ente_ref) if ente_ref else ""
    ente_detalle = {
        "ente": ente_clave,
        "ente_display": _ente_display(ente_clave),
        "horarios": ente_horarios.get(ente_clave, []),
    } if ente_clave else None

    return {
        "filtros": filtros,
        "horarios": horarios,
        "horarios_stats": {
            "total": len(horarios),
            "activos": len(active_rows),
            "personas": len(persona_horarios),
            "entes": len(ente_horarios),
        },
        "catalogo_visible": _visible_catalog_rows(),
        "week_days": WEEK_DAYS,
        "edit_horario": edit_item,
        "persona_detalle": persona_detalle,
        "ente_detalle": ente_detalle,
    }


def _build_cruces_context(filtros):
    horarios = _filtrar_horarios_visibles(db_manager.listar_horarios_persona())
    data = detectar_cruces_de_horarios(horarios)
    _sync_observaciones_de_conflictos(data["conflictos"])
    conflictos = _enriquecer_conflictos_con_observaciones(data["conflictos"])

    if filtros.get("rfc"):
        conflictos = [c for c in conflictos if c["rfc"] == filtros["rfc"].upper()]
    if filtros.get("ente"):
        ente_clave = db_manager.normalizar_ente_clave(filtros["ente"]) or filtros["ente"]
        conflictos = [c for c in conflictos if ente_clave in {c["ente_a"], c["ente_b"]}]
    if filtros.get("severidad"):
        conflictos = [c for c in conflictos if c["severidad"] == filtros["severidad"]]
    if filtros.get("estatus"):
        conflictos = [c for c in conflictos if c["estatus_revision"] == filtros["estatus"]]
    if filtros.get("periodo"):
        periodo_sel = db_manager.get_periodo_quincenal(filtros["periodo"])
        if periodo_sel:
            conflictos = [c for c in conflictos if _conflicto_aplica_a_periodo(c, periodo_sel)]

    personas_index = {}
    for conflicto in conflictos:
        persona = personas_index.setdefault(conflicto["rfc"], {
            "rfc": conflicto["rfc"],
            "nombre": conflicto["nombre"],
            "entes": set(),
            "numero_horarios": 0,
            "numero_conflictos": 0,
            "severidad_maxima": "baja",
            "ultima_revision": conflicto.get("fecha_revision") or "",
            "estatus_revision": conflicto["estatus_revision"],
            "conflictos": [],
        })
        persona["entes"].update([conflicto["ente_a_display"], conflicto["ente_b_display"]])
        persona["numero_conflictos"] += 1
        persona["conflictos"].append(conflicto)
        persona["numero_horarios"] = len({
            item
            for registro in persona["conflictos"]
            for item in (registro["horario_a_id"], registro["horario_b_id"])
        })
        if conflicto.get("fecha_revision"):
            persona["ultima_revision"] = max(persona["ultima_revision"], conflicto["fecha_revision"])
        ranking = {"baja": 1, "media": 2, "alta": 3}
        if ranking.get(conflicto["severidad"], 0) > ranking.get(persona["severidad_maxima"], 0):
            persona["severidad_maxima"] = conflicto["severidad"]
        if persona["estatus_revision"] != conflicto["estatus_revision"]:
            persona["estatus_revision"] = "mixto"

    personas = []
    for persona in personas_index.values():
        persona["entes"] = sorted(persona["entes"])
        persona["numero_entes"] = len(persona["entes"])
        personas.append(persona)
    personas.sort(key=lambda item: (-item["numero_conflictos"], item["nombre"], item["rfc"]))

    return {
        "filtros": filtros,
        "personas": personas,
        "conflictos": conflictos,
        "periodos_quincenales": db_manager.listar_periodos_quincenales(),
        "catalogo_visible": _visible_catalog_rows(),
        "resumen": {
            "personas": len(personas),
            "conflictos": len(conflictos),
            "alta": sum(1 for c in conflictos if c["severidad"] == "alta"),
            "media": sum(1 for c in conflictos if c["severidad"] == "media"),
            "baja": sum(1 for c in conflictos if c["severidad"] == "baja"),
        },
    }


def _build_observaciones_context(filtros):
    rows = db_manager.listar_observaciones_cruce(filtros=filtros)
    entes_usuario = session.get("entes", [])
    modo_permiso = "ALL" if _es_usuario_luis() else _allowed_all(entes_usuario)
    rows = [
        row for row in rows
        if _puede_ver_ente(row.get("ente_a"), entes_usuario, modo_permiso)
        or _puede_ver_ente(row.get("ente_b"), entes_usuario, modo_permiso)
    ]
    return {
        "filtros": filtros,
        "observaciones": rows,
        "catalogo_visible": _visible_catalog_rows(),
        "resumen": _resumen_observaciones(rows),
    }


def _build_pdp_context(filtros, recalcular=False):
    periodo_sel = filtros.get("periodo_quincenal") or ""
    periodo = db_manager.get_periodo_quincenal(periodo_sel) if periodo_sel else calcular_periodo_quincenal(date.today())
    pagos = []
    conflictos_periodo = []
    if periodo:
        if recalcular:
            pagos, conflictos_periodo = _calcular_pagos_pdp_para_periodo(periodo, filtros=filtros, persistir=True)
        pagos = db_manager.listar_pagos_pdp({
            "periodo_quincenal": periodo["etiqueta"],
            "ente": filtros.get("ente"),
            "rfc": filtros.get("rfc"),
            "estatus": filtros.get("estatus"),
        })
        if not pagos:
            pagos, conflictos_periodo = _calcular_pagos_pdp_para_periodo(periodo, filtros=filtros, persistir=False)
        else:
            conflictos_periodo = [
                conflicto
                for conflicto in detectar_cruces_de_horarios(_filtrar_horarios_visibles(db_manager.listar_horarios_persona()))["conflictos"]
                if _conflicto_aplica_a_periodo(conflicto, periodo)
            ]

    return {
        "filtros": filtros,
        "periodo_actual": periodo,
        "periodos_quincenales": db_manager.listar_periodos_quincenales(),
        "pagos": pagos,
        "conflictos_periodo": conflictos_periodo,
        "catalogo_visible": _visible_catalog_rows(),
        "resumen": {
            "total": len(pagos),
            "observados": sum(1 for pago in pagos if str(pago.get("estatus") or "").lower() == "observado"),
            "calculados": sum(1 for pago in pagos if str(pago.get("estatus") or "").lower() == "calculado"),
            "monto_total": round(sum(float(pago.get("total_calculado") or 0) for pago in pagos), 2),
        },
    }


def _build_reportes_context(filtros):
    cruces_context = _build_cruces_context({
        "rfc": filtros.get("rfc", ""),
        "ente": filtros.get("ente", ""),
        "severidad": filtros.get("severidad", ""),
        "estatus": filtros.get("estatus", ""),
        "periodo": filtros.get("periodo_quincenal", ""),
    })
    observaciones_context = _build_observaciones_context({
        "rfc": filtros.get("rfc", ""),
        "ente": filtros.get("ente", ""),
        "severidad": filtros.get("severidad", ""),
        "estatus": filtros.get("estatus", ""),
    })
    pdp_context = _build_pdp_context({
        "periodo_quincenal": filtros.get("periodo_quincenal", ""),
        "ente": filtros.get("ente", ""),
        "rfc": filtros.get("rfc", ""),
        "estatus": filtros.get("estatus_pago", ""),
    })

    por_ente = {}
    for conflicto in cruces_context["conflictos"]:
        for ente in (conflicto["ente_a"], conflicto["ente_b"]):
            card = por_ente.setdefault(ente, {"ente": ente, "personas": set(), "conflictos": 0, "severidad": "baja"})
            card["personas"].add(conflicto["rfc"])
            card["conflictos"] += 1
            if {"baja": 1, "media": 2, "alta": 3}.get(conflicto["severidad"], 0) > {"baja": 1, "media": 2, "alta": 3}.get(card["severidad"], 0):
                card["severidad"] = conflicto["severidad"]
    por_ente_rows = [
        {
            "ente": key,
            "ente_display": _ente_display(key),
            "personas": len(value["personas"]),
            "conflictos": value["conflictos"],
            "severidad": value["severidad"],
        }
        for key, value in por_ente.items()
    ]
    por_ente_rows.sort(key=lambda item: (-item["conflictos"], item["ente_display"]))

    return {
        "filtros": filtros,
        "catalogo_visible": _visible_catalog_rows(),
        "periodos_quincenales": db_manager.listar_periodos_quincenales(),
        "cruces": cruces_context,
        "observaciones": observaciones_context,
        "pdp": pdp_context,
        "por_ente": por_ente_rows,
        "generado_en": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "responsable": session.get("nombre") or session.get("usuario") or "SISTEMA",
    }


def _build_resultados_monitor():
    context = _build_reportes_context({
        "periodo_quincenal": "",
        "ente": "",
        "rfc": "",
        "severidad": "",
        "estatus": "",
        "estatus_pago": "",
    })
    return {
        "resumen": {
            "cruces_horario": context["cruces"]["resumen"]["conflictos"],
            "personas_con_cruce": context["cruces"]["resumen"]["personas"],
            "observaciones": context["observaciones"]["resumen"]["total"],
            "pagos_observados": context["pdp"]["resumen"]["observados"],
        },
        "personas": context["cruces"]["personas"][:5],
        "observaciones": context["observaciones"]["observaciones"][:5],
        "pdp": context["pdp"]["pagos"][:5],
    }


@app.route("/", methods=["GET", "POST"])
def login():
    usuarios_activos = _get_login_users()
    preferred_user_display = {
        user["username"]: user["display_name"]
        for user in usuarios_activos
    }
    selected_username = ""
    selected_display = "usuario"
    next_url = _safe_next_url(request.values.get("next", ""))

    if request.method == "POST":
        usuario = (
            request.form.get("username")
            or request.form.get("usuario")
            or ""
        ).strip().lower()
        clave = (
            request.form.get("password")
            or request.form.get("clave")
            or ""
        ).strip()
        selected_username = usuario
        selected_display = preferred_user_display.get(usuario, usuario or "usuario")
        if usuario not in preferred_user_display:
            log.warning("Intento de acceso fuera de catalogo SASP usuario=%s", usuario)
            return render_template(
                "login.html",
                error="Usuario no autorizado para SASP",
                usuarios_activos=usuarios_activos,
                selected_username=selected_username,
                selected_display=selected_display,
                preferred_user_display=preferred_user_display,
                next_url=next_url,
            )
        user = db_manager.get_usuario(usuario, clave)
        if not user:
            log.warning("Login fallido para usuario=%s", usuario)
            return render_template(
                "login.html",
                error="Credenciales inválidas",
                usuarios_activos=usuarios_activos,
                selected_username=selected_username,
                selected_display=selected_display,
                preferred_user_display=preferred_user_display,
                next_url=next_url,
            )

        session.update({
            "usuario": user["usuario"],
            "nombre": user["nombre"],
            "autenticado": True
        })
        session["entes"] = _normalize_session_entes(user["entes"])

        log.info("Login ok usuario=%s entes=%s", user["usuario"], ",".join(session["entes"]))
        return redirect(next_url or url_for("dashboard"))

    return render_template(
        "login.html",
        usuarios_activos=usuarios_activos,
        selected_username=selected_username,
        selected_display=selected_display,
        preferred_user_display=preferred_user_display,
        next_url=next_url,
    )


@app.route("/logout")
def logout():
    usuario = session.get("usuario")
    session.clear()
    log.info("Logout usuario=%s", usuario)
    return redirect(url_for("login"))

# -----------------------------------------------------------
# DASHBOARD
# -----------------------------------------------------------
@app.route("/dashboard")
def dashboard():
    dashboard_context = _build_dashboard_context()
    return render_template(
        "dashboard.html",
        nombre=session.get("nombre"),
        usuario=session.get("usuario"),
        **dashboard_context,
    )


@app.route("/api/dashboard/archivos-procesados")
def dashboard_processed_files_api():
    if not session.get("autenticado"):
        return jsonify({"error": "No autorizado"}), 403
    items = _build_dashboard_processed_files()
    drawer = _build_dashboard_processed_drawer()
    return jsonify({
        "items": items,
        "total": len(items),
        "secciones": drawer,
    })


@app.route("/dashboard/seed-demo", methods=["POST"])
def seed_demo_dashboard():
    if not session.get("autenticado"):
        return jsonify({"error": "No autorizado"}), 403

    try:
        insertados, actualizados = _seed_demo_records()
        summary = _build_dashboard_cross_summary()
        return jsonify({
            "mensaje": "Datos de ejemplo cargados correctamente.",
            "insertados": insertados,
            "actualizados": actualizados,
            "cruces_totales": summary["total"],
            "ente_ente": summary["ente_ente"]["count"],
            "ente_municipio": summary["ente_municipio"]["count"],
            "municipio_municipio": summary["municipio_municipio"]["count"],
        })
    except Exception as exc:
        log.exception("Error al cargar datos demo")
        return jsonify({"error": f"No fue posible cargar los datos demo: {exc}"}), 500

# -----------------------------------------------------------
# CARGA MASIVA (DataProcessor cruza por RFC y QNAs)
# -----------------------------------------------------------
@app.route("/upload_laboral", methods=["POST"])
def upload_laboral():
    if not session.get("autenticado"):
        return jsonify({"error": "No autorizado"}), 403

    try:
        files = request.files.getlist("files")
        if not files:
            return jsonify({"error": "No se enviaron archivos"})

        nombres = [getattr(f, "filename", "archivo.xlsx") for f in files]
        log.info("Upload recibido: %s", nombres)

        # Procesar archivos y extraer TODOS los registros individuales
        registros_individuales, alertas = data_processor.extraer_registros_individuales(files)
        log.info("Registros individuales extraídos=%d | Alertas=%d", len(registros_individuales), len(alertas))

        # Guardar/actualizar registros individuales (sin duplicar RFC+ENTE)
        n_insertados, n_actualizados = db_manager.guardar_registros_individuales(registros_individuales)

        log.info("Insertados=%d | Actualizados=%d", n_insertados, n_actualizados)

        observaciones_total = len(alertas)
        observaciones_warning = observaciones_total
        guardados_total = n_insertados + n_actualizados
        guardado_estado = "guardado parcial" if observaciones_total else "guardado completo"

        response = {
            "mensaje": f"Procesamiento completado. {n_insertados} nuevos registros, {n_actualizados} actualizados.",
            "total_procesados": len(registros_individuales),
            "insertados": n_insertados,
            "actualizados": n_actualizados,
            "guardados_total": guardados_total,
            "pendientes_total": max(len(registros_individuales) - guardados_total, 0),
            "errores_total": 0,
            "guardado_estado": guardado_estado,
            "alertas": alertas,
            "observaciones_total": observaciones_total,
            "observaciones_por_tipo": {
                "error": 0,
                "warning": observaciones_warning,
                "info": 0,
            },
            "archivos_recibidos": len(files),
            "entes_detectados": sorted({
                str(registro.get("ente") or "").strip()
                for registro in registros_individuales
                if str(registro.get("ente") or "").strip()
            }),
            "ultima_actualizacion": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "estado": "completed_with_alerts" if alertas else "completed",
        }

        return jsonify(response)

    except Exception as e:
        log.exception("Error en upload_laboral")
        return jsonify({"error": f"Error al procesar archivos: {e}"}), 500


@app.route("/upload_horarios", methods=["POST"])
def upload_horarios():
    if not session.get("autenticado"):
        return jsonify({"error": "No autorizado"}), 403

    try:
        files = request.files.getlist("files")
        if not files:
            return jsonify({"error": "No se enviaron archivos"})

        horarios, alertas = _extract_horarios_from_files(files)
        if not horarios and alertas:
            return jsonify({
                "error": "No fue posible procesar el archivo de horarios.",
                "alertas": alertas,
            }), 400

        guardados = 0
        for horario in horarios:
            db_manager.guardar_horario_persona(horario, usuario=session.get("usuario", ""))
            guardados += 1

        response = {
            "mensaje": f"Carga de horarios completada. {guardados} horario(s) registrados.",
            "total_procesados": len(horarios),
            "guardados_total": guardados,
            "errores_total": 0,
            "alertas": alertas,
            "observaciones_total": len(alertas),
            "estado": "completed_with_alerts" if alertas else "completed",
            "ultima_actualizacion": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        return jsonify(response)
    except Exception as exc:
        log.exception("Error en upload_horarios")
        return jsonify({"error": f"Error al procesar horarios: {exc}"}), 500


# -----------------------------------------------------------
# HORARIOS / CRUCES / OBSERVACIONES / PDP / REPORTES
# -----------------------------------------------------------
@app.route("/horarios")
def horarios_home():
    filtros = _horarios_filters_from_request()
    edit_id = request.args.get("edit_id", type=int)
    return render_template("horarios.html", **_build_horarios_context(filtros, edit_id=edit_id))


@app.route("/horarios/guardar", methods=["POST"])
def guardar_horario_persona():
    horario = {
        "id": request.form.get("id") or None,
        "rfc": request.form.get("rfc", ""),
        "nombre": request.form.get("nombre", ""),
        "ente": request.form.get("ente", ""),
        "cargo": request.form.get("cargo", ""),
        "dia_semana": request.form.get("dia_semana", ""),
        "hora_inicio": request.form.get("hora_inicio", ""),
        "hora_fin": request.form.get("hora_fin", ""),
        "fecha_inicio_vigencia": request.form.get("fecha_inicio_vigencia", ""),
        "fecha_fin_vigencia": request.form.get("fecha_fin_vigencia", ""),
        "periodo": request.form.get("periodo", ""),
        "observaciones": request.form.get("observaciones", ""),
        "estatus": request.form.get("estatus", "activo"),
        "permite_traslape_interno": request.form.get("permite_traslape_interno") == "1",
    }
    try:
        db_manager.guardar_horario_persona(horario, usuario=session.get("usuario", ""))
    except ValueError as exc:
        filtros = _horarios_filters_from_request()
        context = _build_horarios_context(filtros)
        context["form_error"] = str(exc)
        context["edit_horario"] = horario
        return render_template("horarios.html", **context), 400
    return redirect(url_for("horarios_home", rfc=horario["rfc"]))


@app.route("/horarios/<int:horario_id>/desactivar", methods=["POST"])
def desactivar_horario_persona(horario_id):
    item = db_manager.obtener_horario_persona(horario_id)
    if not item or not _can_edit_ente(item.get("ente")):
        return "No autorizado", 403
    db_manager.desactivar_horario_persona(horario_id, usuario=session.get("usuario", ""))
    return redirect(url_for("horarios_home", rfc=item.get("rfc", "")))


@app.route("/horarios/<int:horario_id>/eliminar", methods=["POST"])
def eliminar_horario_persona(horario_id):
    item = db_manager.obtener_horario_persona(horario_id)
    if not item or not _can_edit_ente(item.get("ente")):
        return "No autorizado", 403
    db_manager.eliminar_horario_persona(horario_id)
    return redirect(url_for("horarios_home", rfc=item.get("rfc", "")))


@app.route("/horarios/persona/<rfc>")
def horarios_por_persona(rfc):
    return redirect(url_for("horarios_home", rfc=rfc))


@app.route("/horarios/ente/<ente>")
def horarios_por_ente(ente):
    return redirect(url_for("horarios_home", ente=ente))


@app.route("/cruce-entes")
def cruce_entes():
    filtros = {
        "rfc": _query_param("rfc"),
        "ente": _query_param("ente"),
        "severidad": _query_param("severidad"),
        "estatus": _query_param("estatus"),
        "periodo": _query_param("periodo"),
    }
    return render_template("cruces_entes.html", **_build_cruces_context(filtros))


@app.route("/observaciones")
def observaciones_home():
    filtros = {
        "rfc": _query_param("rfc"),
        "ente": _query_param("ente"),
        "severidad": _query_param("severidad"),
        "estatus": _query_param("estatus"),
    }
    return render_template("observaciones.html", **_build_observaciones_context(filtros))


@app.route("/observaciones/actualizar", methods=["POST"])
def actualizar_observacion_cruce():
    conflicto_hash = request.form.get("conflicto_hash", "").strip()
    estatus = request.form.get("estatus", "pendiente").strip().lower()
    comentarios = request.form.get("comentarios_adicionales", "").strip()
    if not conflicto_hash:
        return redirect(url_for("observaciones_home"))
    db_manager.actualizar_estatus_observacion_cruce(
        conflicto_hash,
        estatus,
        comentarios=comentarios,
        usuario=session.get("usuario", ""),
    )
    destino = request.form.get("redirect_to", "observaciones").strip()
    if destino == "cruces":
        return redirect(url_for("cruce_entes"))
    return redirect(url_for("observaciones_home"))


@app.route("/pdp")
def pagos_pdp_home():
    filtros = {
        "periodo_quincenal": _query_param("periodo_quincenal"),
        "ente": _query_param("ente"),
        "rfc": _query_param("rfc"),
        "estatus": _query_param("estatus"),
    }
    return render_template("pdp.html", **_build_pdp_context(filtros))


@app.route("/pdp/calcular", methods=["POST"])
def calcular_pagos_pdp_route():
    filtros = {
        "periodo_quincenal": str(request.form.get("periodo_quincenal", "")).strip(),
        "ente": str(request.form.get("ente", "")).strip(),
        "rfc": str(request.form.get("rfc", "")).strip(),
        "estatus": str(request.form.get("estatus", "")).strip(),
    }
    return render_template("pdp.html", **_build_pdp_context(filtros, recalcular=True))


@app.route("/reportes-operativos")
def reportes_operativos():
    filtros = {
        "periodo_quincenal": _query_param("periodo_quincenal"),
        "ente": _query_param("ente"),
        "rfc": _query_param("rfc"),
        "severidad": _query_param("severidad"),
        "estatus": _query_param("estatus"),
        "estatus_pago": _query_param("estatus_pago"),
    }
    return render_template("reportes_operativos.html", **_build_reportes_context(filtros))


@app.route("/reportes-operativos/exportar")
def exportar_reportes_operativos():
    tipo = _query_param("tipo", "cruces").lower()
    filtros = {
        "periodo_quincenal": _query_param("periodo_quincenal"),
        "ente": _query_param("ente"),
        "rfc": _query_param("rfc"),
        "severidad": _query_param("severidad"),
        "estatus": _query_param("estatus"),
        "estatus_pago": _query_param("estatus_pago"),
    }
    context = _build_reportes_context(filtros)

    if tipo == "cruces":
        filas = [{
            "RFC": item["rfc"],
            "Nombre": item["nombre"],
            "Ente A": item["ente_a_display"],
            "Ente B": item["ente_b_display"],
            "Día": item["dia_label"],
            "Horario A": f"{item['hora_inicio_a']} - {item['hora_fin_a']}",
            "Horario B": f"{item['hora_inicio_b']} - {item['hora_fin_b']}",
            "Traslape Minutos": item["minutos_traslape"],
            "Severidad": item["severidad"],
            "Estatus": item["estatus_revision"],
        } for item in context["cruces"]["conflictos"]]
        nombre_archivo = "SASP_Reporte_Cruces_Horario.xlsx"
        hoja = "Cruces_Horario"
    elif tipo == "ente":
        filas = [{
            "Ente": item["ente_display"],
            "Personas con cruce": item["personas"],
            "Conflictos": item["conflictos"],
            "Severidad máxima": item["severidad"],
        } for item in context["por_ente"]]
        nombre_archivo = "SASP_Reporte_por_Ente.xlsx"
        hoja = "Cruces_por_Ente"
    elif tipo == "persona":
        filas = [{
            "RFC": item["rfc"],
            "Nombre": item["nombre"],
            "Entes relacionados": ", ".join(item["entes"]),
            "Horarios": item["numero_horarios"],
            "Conflictos": item["numero_conflictos"],
            "Severidad máxima": item["severidad_maxima"],
            "Última revisión": item["ultima_revision"],
            "Estatus": item["estatus_revision"],
        } for item in context["cruces"]["personas"]]
        nombre_archivo = "SASP_Reporte_por_Persona.xlsx"
        hoja = "Cruces_por_Persona"
    elif tipo == "severidad":
        filas = [{
            "Severidad": key.capitalize(),
            "Conflictos": context["cruces"]["resumen"][key],
        } for key in ("alta", "media", "baja")]
        nombre_archivo = "SASP_Reporte_por_Severidad.xlsx"
        hoja = "Cruces_por_Severidad"
    elif tipo == "periodo":
        filas = [{
            "Periodo": context["pdp"]["periodo_actual"]["etiqueta"] if context["pdp"]["periodo_actual"] else "",
            "RFC": item["rfc"],
            "Nombre": item["nombre"],
            "Ente": _ente_display(item["ente"]),
            "Estatus": item["estatus"],
            "Total calculado": item["total_calculado"],
        } for item in context["pdp"]["pagos"]]
        nombre_archivo = "SASP_Reporte_Quincenal.xlsx"
        hoja = "Periodo_Quincenal"
    elif tipo == "observaciones":
        filas = [{
            "RFC": item["rfc"],
            "Nombre": item["nombre"],
            "Entes": f"{_ente_display(item['ente_a'])} / {_ente_display(item['ente_b'])}",
            "Severidad": item["severidad"],
            "Estatus": item["estatus"],
            "Observación": item["texto_observacion"],
            "Recomendación": item["recomendacion"],
            "Actualizado": item["actualizado"],
        } for item in context["observaciones"]["observaciones"]]
        nombre_archivo = "SASP_Reporte_Observaciones.xlsx"
        hoja = "Observaciones"
    else:
        filas = [{
            "RFC": item["rfc"],
            "Nombre": item["nombre"],
            "Ente": _ente_display(item["ente"]),
            "Periodo": item["periodo_quincenal"],
            "Sueldo base": item["sueldo_base"],
            "Monto PDP": item["monto_pdp"],
            "Total calculado": item["total_calculado"],
            "Estatus": item["estatus"],
            "Observaciones": item["observaciones"],
        } for item in context["pdp"]["pagos"]]
        nombre_archivo = "SASP_Reporte_Pagos_PDP.xlsx"
        hoja = "Pagos_PDP"

    df = pd.DataFrame(filas)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=hoja)
    output.seek(0)
    return send_file(output, download_name=nombre_archivo, as_attachment=True)

# -----------------------------------------------------------
# RESULTADOS AGRUPADOS
# -----------------------------------------------------------
def _es_usuario_luis():
    entes_usuario = [_sanitize_text(v) for v in session.get("entes", [])]
    return "TODOS" in entes_usuario


def _es_usuario_validador():
    return session.get("usuario", "").strip().lower() == "luis"


AMBITOS_REPORTE = {"estatales", "municipios", "mixtos"}
AMBITO_RFC_LABELS = {
    "estatales": "Estatales",
    "municipios": "Municipales",
    "mixtos": "Mixtos estado-municipio",
}


def _normalizar_ambito(ambito_sel):
    ambito = str(ambito_sel or "").strip().lower()
    return ambito if ambito in AMBITOS_REPORTE else "estatales"


def _ambito_rfc_label(ambito):
    return AMBITO_RFC_LABELS.get(ambito, "Sin clasificar")


def _monto_num(value):
    if value in (None, ""):
        return 0.0
    try:
        return float(
            str(value)
            .replace(",", "")
            .replace("$", "")
            .replace("MXN", "")
            .replace("mxn", "")
            .strip()
        )
    except (TypeError, ValueError):
        return 0.0


def _weekly_amount(value):
    monto = _monto_num(value)
    return round(monto / 52.0, 2) if monto else 0.0


def _pdp_amount(value):
    monto = _monto_num(value)
    return round(monto / 365.0, 2) if monto else 0.0


def _time_to_minutes(value):
    text = str(value or "").strip()
    if not text or ":" not in text:
        return None
    try:
        hours, minutes = text.split(":", 1)
        return int(hours) * 60 + int(minutes)
    except ValueError:
        return None


def _format_minutes(total_minutes):
    if total_minutes <= 0:
        return "0 h"
    hours = total_minutes // 60
    minutes = total_minutes % 60
    if minutes:
        return f"{hours} h {minutes:02d} min"
    return f"{hours} h"


def _can_edit_ente(ente_ref):
    entes_usuario = session.get("entes", [])
    ente_norm = db_manager.normalizar_ente_clave(ente_ref) or str(ente_ref or "").strip()
    for ente_usuario in entes_usuario:
        if _sanitize_text(ente_usuario) == "TODOS":
            return True
        if _sanitize_text(ente_usuario) == _sanitize_text(ente_norm):
            return True
    return False


def _build_horarios_stage(rfc, registros):
    horarios_guardados = db_manager.get_horarios_por_rfc(rfc)
    observaciones_beta = db_manager.get_observaciones_beta_por_rfc(rfc)
    per_ente = []
    total_weekly = 0.0
    total_annual = 0.0
    total_pdp = 0.0

    for reg in registros or []:
        ente_clave = db_manager.normalizar_ente_clave(reg.get("ente")) or reg.get("ente")
        annual_amount = _monto_num(reg.get("monto"))
        weekly_amount = _weekly_amount(reg.get("monto"))
        pdp_amount = _pdp_amount(reg.get("monto"))
        total_annual += annual_amount
        total_weekly += weekly_amount
        total_pdp += pdp_amount

        day_rows = {day_number: {"hora_inicio": "", "hora_fin": "", "observaciones": ""} for day_number, _ in WEEK_DAYS}
        weekly_minutes = 0

        for row in horarios_guardados.get(ente_clave, []):
            dia_semana = int(row.get("dia_semana", -1))
            if dia_semana not in day_rows:
                continue
            day_rows[dia_semana] = {
                "hora_inicio": row.get("hora_inicio", ""),
                "hora_fin": row.get("hora_fin", ""),
                "observaciones": row.get("observaciones", ""),
            }
            inicio = _time_to_minutes(row.get("hora_inicio"))
            fin = _time_to_minutes(row.get("hora_fin"))
            if inicio is not None and fin is not None and fin > inicio:
                weekly_minutes += fin - inicio

        per_ente.append({
            "ente_clave": ente_clave,
            "ente_display": _ente_display(ente_clave),
            "puesto": reg.get("puesto") or "Sin puesto",
            "annual_amount": annual_amount,
            "weekly_amount": weekly_amount,
            "pdp_amount": pdp_amount,
            "weekly_minutes": weekly_minutes,
            "weekly_hours_label": _format_minutes(weekly_minutes),
            "can_edit": _can_edit_ente(ente_clave),
            "beta_observacion": (observaciones_beta.get(ente_clave) or {}).get("observacion", ""),
            "beta_estatus": (observaciones_beta.get(ente_clave) or {}).get("estatus", "Borrador"),
            "days": [
                {
                    "number": day_number,
                    "label": day_label,
                    **day_rows[day_number],
                }
                for day_number, day_label in WEEK_DAYS
            ],
        })

    conflicts = []
    for left, right in combinations(per_ente, 2):
        for day_number, day_label in WEEK_DAYS:
            left_day = next(day for day in left["days"] if day["number"] == day_number)
            right_day = next(day for day in right["days"] if day["number"] == day_number)
            left_start = _time_to_minutes(left_day["hora_inicio"])
            left_end = _time_to_minutes(left_day["hora_fin"])
            right_start = _time_to_minutes(right_day["hora_inicio"])
            right_end = _time_to_minutes(right_day["hora_fin"])

            if None in {left_start, left_end, right_start, right_end}:
                continue

            overlap_start = max(left_start, right_start)
            overlap_end = min(left_end, right_end)
            if overlap_end > overlap_start:
                conflicts.append({
                    "day_label": day_label,
                    "left_ente": left["ente_display"],
                    "right_ente": right["ente_display"],
                    "range": f"{left_day['hora_inicio']}-{left_day['hora_fin']} vs {right_day['hora_inicio']}-{right_day['hora_fin']}",
                })

    return {
        "per_ente": per_ente,
        "conflicts": conflicts,
        "total_weekly": round(total_weekly, 2),
        "total_annual": round(total_annual, 2),
        "total_pdp": round(total_pdp, 2),
        "has_complete_schedule": any(
            any(day["hora_inicio"] and day["hora_fin"] for day in ente["days"])
            for ente in per_ente
        ),
    }


def _tipo_ente(ente_clave):
    ref = _sanitize_text(ente_clave)
    cache = _entes_cache()
    info = cache.get(ref)
    if info:
        return (info.get("tipo") or "ENTE").upper()
    for k, d in cache.items():
        if ref in {_sanitize_text(k), _sanitize_text(d.get("siglas")), _sanitize_text(d.get("nombre"))}:
            return (d.get("tipo") or "ENTE").upper()
    return "ENTE"


def _coincide_ambito(ambito_sel, tipo_ente):
    tipo = (tipo_ente or "ENTE").upper()
    if ambito_sel == "municipios":
        return "MUNIC" in tipo
    if ambito_sel == "mixtos":
        return True
    return "MUNIC" not in tipo


def _puede_ver_ente(ente_clave, entes_usuario, modo_permiso=None):
    if _es_usuario_luis():
        return True
    modo = modo_permiso if modo_permiso is not None else _allowed_all(entes_usuario)
    tipo_ente = _tipo_ente(ente_clave)

    if modo == "ALL":
        return True
    if modo == "ENTES":
        return tipo_ente == "ENTE"
    if modo == "MUNICIPIOS":
        return tipo_ente == "MUNICIPIO"
    return any(_ente_match(eu, [ente_clave]) for eu in entes_usuario)


def _es_prevalidado_oculto(mapa_pre, ente_clave):
    clave = db_manager.normalizar_ente_clave(ente_clave) or ente_clave
    estado = str((mapa_pre.get(clave) or {}).get("estado", "Sin valoración")).strip().upper()
    return estado in {"SOLVENTADO", "NO SOLVENTADO"}


def _filtrar_duplicados_con_visibilidad(resultados):
    filtrados = _filtrar_duplicados_reales(resultados)
    out = []
    for r in filtrados:
        mapa_pre = db_manager.get_prevalidaciones_por_rfc(str(r.get("rfc", "")))
        entes_visibles = [e for e in (r.get("entes") or []) if not _es_prevalidado_oculto(mapa_pre, e)]
        entes_visibles = sorted(set(entes_visibles))
        if len(entes_visibles) < 2:
            continue
        r2 = dict(r)
        r2["entes"] = entes_visibles
        out.append(r2)
    return out


def _clasificar_resultado_ambito(resultado, entes_usuario, modo_permiso):
    entes_visibles = []
    for ente in (resultado.get("entes") or []):
        ente_txt = str(ente).strip()
        if ente_txt and _puede_ver_ente(ente_txt, entes_usuario, modo_permiso):
            entes_visibles.append(ente_txt)

    entes_visibles = sorted(set(entes_visibles))
    entes_estatales = [e for e in entes_visibles if _coincide_ambito("estatales", _tipo_ente(e))]
    entes_municipales = [e for e in entes_visibles if _coincide_ambito("municipios", _tipo_ente(e))]

    categoria = None
    entes_ambito = []
    if entes_estatales and entes_municipales:
        categoria = "mixtos"
        entes_ambito = entes_visibles
    elif len(entes_estatales) >= 2:
        categoria = "estatales"
        entes_ambito = entes_estatales
    elif len(entes_municipales) >= 2:
        categoria = "municipios"
        entes_ambito = entes_municipales

    return {
        "categoria": categoria,
        "visibles": entes_visibles,
        "estatales": entes_estatales,
        "municipios": entes_municipales,
        "entes_ambito": entes_ambito,
    }


def _resumen_duplicidad_por_ambito(resultados, entes_usuario, modo_permiso, filtro_ente=""):
    rfcs_por_ambito = {ambito: set() for ambito in sorted(AMBITOS_REPORTE)}
    entes_por_ambito = {ambito: set() for ambito in sorted(AMBITOS_REPORTE)}
    ambito_por_rfc = {}

    for resultado in resultados:
        info_ambito = _clasificar_resultado_ambito(resultado, entes_usuario, modo_permiso)
        categoria = info_ambito["categoria"]
        if not categoria:
            continue

        entes_ambito = info_ambito["entes_ambito"]
        if filtro_ente and not any(_ente_display(ente) == filtro_ente for ente in entes_ambito):
            continue

        rfc = str(resultado.get("rfc", "")).strip().upper()
        if rfc:
            rfcs_por_ambito[categoria].add(rfc)
            ambito_por_rfc[rfc] = categoria

        for ente in entes_ambito:
            entes_por_ambito[categoria].add(_ente_display(ente))

    conteos = {ambito: len(rfcs) for ambito, rfcs in rfcs_por_ambito.items()}
    return {
        "rfcs_por_ambito": rfcs_por_ambito,
        "entes_por_ambito": entes_por_ambito,
        "ambito_por_rfc": ambito_por_rfc,
        "conteos": conteos,
        "total_general": sum(conteos.values()),
    }


def _indexar_catalogo(catalogo):
    index = {}
    for ente in catalogo:
        for valor in (ente.get("clave"), ente.get("siglas"), ente.get("nombre")):
            clave = _sanitize_text(valor)
            if clave:
                index[clave] = ente
    return index


def _asegurar_ente_info(entes_info, ente_ref, trabajadores_por_ente_map, catalogo_index):
    display = _ente_display(ente_ref)
    if display in entes_info:
        return display

    meta = catalogo_index.get(_sanitize_text(ente_ref)) or {}
    clave = meta.get("clave") or db_manager.normalizar_ente_clave(ente_ref) or ente_ref
    entes_info[display] = {
        "num": meta.get("num"),
        "siglas": meta.get("siglas") or display,
        "nombre_completo": meta.get("nombre") or display,
        "total": trabajadores_por_ente_map.get(clave, 0),
        "duplicados": 0,
        "tipo": str(meta.get("ambito") or _tipo_ente(ente_ref)).upper(),
    }
    return display


def _ambito_ente_origen(ente_ref):
    return "Municipal" if _coincide_ambito("municipios", _tipo_ente(ente_ref)) else "Estatal"


def _resolver_texto_solventacion(pre, solv, fallback=""):
    catalogo = str((pre or {}).get("catalogo", "")).strip()
    otro = str((pre or {}).get("otro_texto", "")).strip()
    comentario = str((pre or {}).get("comentario", "")).strip()

    motivo = ""
    if catalogo:
        motivo = otro if catalogo == "Otro" and otro else catalogo

    if motivo and comentario:
        return f"{motivo} - {comentario}"
    if motivo:
        return motivo
    if comentario:
        return comentario

    comentario_solv = str((solv or {}).get("comentario", "")).strip()
    if comentario_solv:
        return comentario_solv
    return str(fallback or "").strip()


def _construir_detalle_solventados(resultados, filtro_ente, ambito_sel, entes_usuario, modo_permiso):
    rfc_solventados = set()
    registros_solventados = set()
    detalle_agrupado = {}

    def _monto_num(v):
        try:
            txt = str(v or "").strip().replace(",", "").replace("$", "")
            txt = txt.replace("MXN", "").replace("mxn", "").strip()
            return float(txt) if txt else 0.0
        except Exception:
            return 0.0

    rfcs = sorted({str(r.get("rfc", "")).strip().upper() for r in resultados if str(r.get("rfc", "")).strip()})
    prevalidaciones_por_rfc = db_manager.get_prevalidaciones_por_rfcs(rfcs)

    for r in resultados:
        rfc_actual = str(r.get("rfc", "")).strip().upper()
        mapa_pre = prevalidaciones_por_rfc.get(rfc_actual, {})
        info_ambito = _clasificar_resultado_ambito(r, entes_usuario, modo_permiso)
        if info_ambito["categoria"] != ambito_sel:
            continue
        monto_por_ente = {}
        for reg in (r.get("registros") or []):
            ente_reg = reg.get("ente")
            ente_key = db_manager.normalizar_ente_clave(ente_reg) or str(ente_reg or "").strip()
            if not ente_key:
                continue
            monto_por_ente[ente_key] = monto_por_ente.get(ente_key, 0.0) + _monto_num(reg.get("monto"))

        for ente_clave in info_ambito["entes_ambito"]:
            if not _puede_ver_ente(ente_clave, entes_usuario, modo_permiso):
                continue

            display = _ente_display(ente_clave)
            if filtro_ente and display != filtro_ente:
                continue

            clave_norm = db_manager.normalizar_ente_clave(ente_clave) or ente_clave
            pre = mapa_pre.get(clave_norm, {})
            pre_estado = str(pre.get("estado", "Sin valoración")).strip().upper()
            if pre_estado != "SOLVENTADO":
                continue

            rfc = str(r.get("rfc", "")).strip()
            rfc_solventados.add(rfc)
            registros_solventados.add(f"{rfc}|{clave_norm}")

            catalogo = str(pre.get("catalogo", "")).strip()
            otro = str(pre.get("otro_texto", "")).strip()
            comentario = str(pre.get("comentario", "")).strip()
            motivo = "Sin motivo"
            if catalogo:
                motivo = otro if catalogo == "Otro" and otro else catalogo

            key = f"{rfc}|{motivo}"
            if key not in detalle_agrupado:
                detalle_agrupado[key] = {
                    "rfc": rfc,
                    "nombre": str(r.get("nombre", "")),
                    "motivo": motivo,
                    "observacion": comentario,
                    "entes": set(),
                    "entes_clave": set(),
                    "total_percepciones_anuales": 0.0,
                }
            if not detalle_agrupado[key]["observacion"] and comentario:
                detalle_agrupado[key]["observacion"] = comentario
            detalle_agrupado[key]["entes"].add(display)
            if clave_norm not in detalle_agrupado[key]["entes_clave"]:
                detalle_agrupado[key]["entes_clave"].add(clave_norm)
                detalle_agrupado[key]["total_percepciones_anuales"] += monto_por_ente.get(clave_norm, 0.0)

    detalle = []
    for item in detalle_agrupado.values():
        entes = sorted(item["entes"])
        detalle.append({
            "rfc": item["rfc"],
            "nombre": item["nombre"],
            "ente": ", ".join(entes),
            "motivo": item["motivo"],
            "observacion": item["observacion"],
            "total_percepciones_anuales": item["total_percepciones_anuales"],
        })
    detalle.sort(key=lambda x: (x["rfc"], x["motivo"]))

    return {
        "resumen": {
            "rfc_solventados": len(rfc_solventados),
            "registros_solventados": len(registros_solventados),
        },
        "detalle": detalle,
    }


def _build_validacion_error_message(exc, accion):
    base = "No fue posible cancelar la validación." if accion == "cancelar" else "No fue posible validar los datos."
    msg = str(exc).lower()
    db_path = DB_PATH
    if "readonly" in msg or "attempt to write a readonly database" in msg:
        db_writable = os.access(db_path, os.W_OK)
        dir_writable = os.access(os.path.dirname(db_path), os.W_OK)
        return (
            f"{base} SQLite está en solo lectura. DB: {db_path}. "
            f"¿DB escribible?: {'sí' if db_writable else 'no'}. "
            f"¿Carpeta escribible?: {'sí' if dir_writable else 'no'}."
        )
    return f"{base} Error técnico: {exc}"


@app.route("/resultados")
def reporte_por_ente():
    filtro_ente = request.args.get("ente", "").strip()
    ambito_sel = _normalizar_ambito(request.args.get("ambito", "estatales"))

    validacion_error = request.args.get("validacion_error", "") == "1"
    validacion_error_msg = ""
    if validacion_error:
        validacion_error_msg = session.pop("validacion_error_msg", "No fue posible validar los datos.")

    entes_usuario = session.get("entes", [])
    es_luis = _es_usuario_luis()
    es_validador = _es_usuario_validador()
    resultados_validados = db_manager.resultados_validados()
    mostrar_duplicados = es_luis or resultados_validados
    mostrar_metricas = es_validador
    modo_permiso = "ALL" if es_luis else _allowed_all(entes_usuario)

    resultados_base = db_manager.obtener_cruces_reales()
    resultados = (
        _filtrar_duplicados_reales(resultados_base)
        if es_luis else _filtrar_duplicados_con_visibilidad(resultados_base)
    )
    trabajadores_por_ente_map = db_manager.contar_trabajadores_por_ente()
    trabajadores_detallados = db_manager.obtener_trabajadores_por_ente()
    catalogo = db_manager.listar_entes() + db_manager.listar_municipios()
    catalogo_index = _indexar_catalogo(catalogo)
    resumen_ambitos = _resumen_duplicidad_por_ambito(resultados, entes_usuario, modo_permiso, filtro_ente=filtro_ente)

    agrupado = {}
    entes_info = {}

    if ambito_sel != "mixtos":
        for ente in catalogo:
            display = ente.get("siglas") or ente.get("nombre")
            clave = ente.get("clave")
            tipo = str(ente.get("ambito") or "ENTE").upper()

            if not _puede_ver_ente(clave, entes_usuario, modo_permiso):
                continue
            if not _coincide_ambito(ambito_sel, tipo):
                continue

            agrupado.setdefault(display, [])
            entes_info[display] = {
                "num": ente.get("num"),
                "siglas": ente.get("siglas"),
                "nombre_completo": ente.get("nombre"),
                "total": trabajadores_por_ente_map.get(clave, 0),
                "duplicados": 0,
                "tipo": tipo,
            }

    resumen_prevalidacion = {"rfc_solventados": 0, "registros_solventados": 0}
    detalle_solventados = []

    if mostrar_duplicados:
        solventados = _construir_detalle_solventados(resultados, filtro_ente, ambito_sel, entes_usuario, modo_permiso)
        resumen_prevalidacion = solventados["resumen"]
        detalle_solventados = solventados["detalle"]

        rfcs_resultados = sorted({
            str(r.get("rfc", "")).strip().upper()
            for r in resultados
            if str(r.get("rfc", "")).strip()
        })
        solventaciones_por_rfc = db_manager.get_solventaciones_por_rfcs(rfcs_resultados)
        prevalidaciones_por_rfc = db_manager.get_prevalidaciones_por_rfcs(rfcs_resultados)

        for r in resultados:
            rfc_actual = str(r.get("rfc", "")).strip().upper()
            mapa_solvs = solventaciones_por_rfc.get(rfc_actual, {})
            mapa_pre = prevalidaciones_por_rfc.get(rfc_actual, {})
            info_ambito = _clasificar_resultado_ambito(r, entes_usuario, modo_permiso)
            if info_ambito["categoria"] != ambito_sel:
                continue

            for ente_clave in info_ambito["entes_ambito"]:
                if not es_luis and _es_prevalidado_oculto(mapa_pre, ente_clave):
                    continue
                if not _puede_ver_ente(ente_clave, entes_usuario, modo_permiso):
                    continue

                display = _ente_display(ente_clave)
                if filtro_ente and display != filtro_ente:
                    continue
                if ambito_sel == "mixtos":
                    _asegurar_ente_info(entes_info, ente_clave, trabajadores_por_ente_map, catalogo_index)
                    agrupado.setdefault(display, [])
                if display not in entes_info:
                    continue

                otros_entes = []
                for e in info_ambito["entes_ambito"]:
                    if _sanitize_text(e) != _sanitize_text(ente_clave):
                        s = _ente_sigla(e)
                        if s not in otros_entes:
                            otros_entes.append(s)

                estado_default = r.get("estado", "Sin valoración")
                estado_entes = {}
                for en in info_ambito["entes_ambito"]:
                    clave_norm = db_manager.normalizar_ente_clave(en) or en
                    estado_entes[_ente_sigla(en)] = (mapa_solvs.get(clave_norm) or {}).get("estado", estado_default)

                clave_actual = db_manager.normalizar_ente_clave(ente_clave) or ente_clave
                pre = mapa_pre.get(clave_actual, {})
                pre_estado = str(pre.get("estado", "Sin valoración"))

                puesto = (
                    r.get("puesto")
                    or ", ".join(sorted({
                        (reg.get("puesto") or "").strip()
                        for reg in (r.get("registros") or [])
                        if (reg.get("puesto") or "").strip()
                    }))
                    or "Sin puesto"
                )
                monto_total = sum(_monto_num(reg.get("monto")) for reg in (r.get("registros") or []))
                monto_pdp = round(monto_total / 365.0, 2) if monto_total else 0.0

                agrupado[display].append({
                    "rfc": r.get("rfc"),
                    "nombre": r.get("nombre"),
                    "puesto": puesto,
                    "entes": otros_entes,
                    "estado": estado_default,
                    "estado_entes": estado_entes,
                    "ente_origen": ente_clave,
                    "pre_estado": pre_estado,
                    "pre_valoracion": pre.get("comentario", ""),
                    "pre_catalogo": pre.get("catalogo", ""),
                    "pre_otro_texto": pre.get("otro_texto", ""),
                    "entes_completos": [_ente_display(e) for e in info_ambito["entes_ambito"]],
                    "monto_total": monto_total,
                    "monto_pdp": monto_pdp,
                })

    for display, info in entes_info.items():
        info["duplicados"] = len(agrupado.get(display, []))

    def _orden_por_num(item):
        info = item[1]
        num_str = str(info.get("num", "999")).strip().rstrip(".")
        partes = []
        for parte in num_str.split("."):
            try:
                partes.append(int(parte))
            except ValueError:
                partes.append(999)
        while len(partes) < 5:
            partes.append(0)
        return tuple(partes)

    entes_info_ordenado = {}
    for k, v in sorted(entes_info.items(), key=_orden_por_num):
        entes_info_ordenado[k] = v

    agrupado_final = {}
    for k, v in agrupado.items():
        if filtro_ente and k != filtro_ente:
            continue
        agrupado_final[k] = v

    trabajadores_por_ente_final = {}
    rfc_procesados = set()
    registros_cargados = 0
    for ente_clave, trabajadores in trabajadores_detallados.items():
        if not _puede_ver_ente(str(ente_clave), entes_usuario, modo_permiso):
            continue
        if ambito_sel != "mixtos" and not _coincide_ambito(ambito_sel, _tipo_ente(str(ente_clave))):
            continue

        display = _ente_display(str(ente_clave))
        if ambito_sel == "mixtos" and display not in entes_info:
            continue
        if filtro_ente and display != filtro_ente:
            continue

        for trab in trabajadores:
            trabajadores_por_ente_final.setdefault(display, []).append(trab)
            rfc = str(trab.get("rfc", "")).strip().upper()
            if rfc:
                rfc_procesados.add(rfc)
            registros_cargados += 1

    entes_visibles = sum(1 for nombre in entes_info_ordenado if (not filtro_ente or nombre == filtro_ente))
    trabajadores_procesados = len(rfc_procesados)
    conteos_ambito = resumen_ambitos["conteos"]
    duplicados_detectados = conteos_ambito.get(ambito_sel, 0)
    indice_duplicidad = round((duplicados_detectados / trabajadores_procesados) * 100, 2) if trabajadores_procesados else 0.0
    entes_con_duplicidad = len(resumen_ambitos["entes_por_ambito"].get(ambito_sel, set()))

    resumen_auditoria = [
        {"m": "Entes analizados", "v": str(entes_visibles)},
        {"m": "Trabajadores analizados (RFC únicos)", "v": f"{trabajadores_procesados:,}"},
        {"m": f"Casos de duplicidad ({_ambito_rfc_label(ambito_sel)} - RFC únicos)", "v": str(duplicados_detectados)},
        {"m": "Casos estatales (RFC únicos)", "v": str(conteos_ambito.get("estatales", 0))},
        {"m": "Casos municipales (RFC únicos)", "v": str(conteos_ambito.get("municipios", 0))},
        {"m": "Casos mixtos estado-municipio (RFC únicos)", "v": str(conteos_ambito.get("mixtos", 0))},
        {"m": "Total general de duplicidad (RFC únicos)", "v": str(resumen_ambitos["total_general"])},
        {"m": "Entes con duplicidad", "v": str(entes_con_duplicidad)},
        {"m": "Índice de trabajadores duplicados", "v": f"{indice_duplicidad:.2f}%"},
    ]
    resultados_monitor = _build_resultados_monitor()

    return render_template(
        "resultados.html",
        resultados=agrupado_final,
        trabajadores_por_ente=trabajadores_por_ente_final,
        entes_info=entes_info_ordenado,
        filtro_ente=filtro_ente,
        ambito_sel=ambito_sel,
        es_luis=es_luis,
        es_validador=es_validador,
        resultados_validados=resultados_validados,
        mostrar_duplicados=mostrar_duplicados,
        mostrar_metricas=mostrar_metricas,
        validacion_error=validacion_error,
        validacion_error_msg=validacion_error_msg,
        resumen_auditoria=resumen_auditoria,
        resumen_prevalidacion=resumen_prevalidacion,
        detalle_solventados=detalle_solventados,
        resumen={
            "entes_visibles": entes_visibles,
            "registros_cargados": registros_cargados,
            "trabajadores_procesados": trabajadores_procesados,
            "duplicados_detectados": duplicados_detectados,
        },
        resultados_monitor=resultados_monitor,
    )

# -----------------------------------------------------------
# DETALLE POR RFC
# -----------------------------------------------------------
@app.route("/resultados/<rfc>")
def resultados_por_rfc(rfc):
    es_luis = _es_usuario_luis()

    if not es_luis and not db_manager.resultados_validados():
        return redirect(url_for("reporte_por_ente"))

    info = db_manager.obtener_resultados_por_rfc(rfc)
    if not info:
        return render_template("empty.html", mensaje="No hay registros del trabajador.")

    if not es_luis:
        mapa_pre = db_manager.get_prevalidaciones_por_rfc(rfc)
        if mapa_pre:
            registros_visibles = []
            entes_visibles = set()
            for reg in info.get("registros", []):
                ente_reg = reg.get("ente", "")
                if ente_reg and _es_prevalidado_oculto(mapa_pre, ente_reg):
                    continue
                registros_visibles.append(reg)
                if ente_reg:
                    entes_visibles.add(ente_reg)
            info["registros"] = registros_visibles
            info["entes"] = sorted(entes_visibles)

        if len(info.get("entes", [])) < 2:
            return render_template("empty.html", mensaje="Este RFC no presenta ninguna incompatibilidad")

    mapa_solvs = db_manager.get_solventaciones_por_rfc(rfc)
    if mapa_solvs and info.get("registros"):
        for reg in info["registros"]:
            ente_clave = db_manager.normalizar_ente_clave(reg.get("ente"))
            if ente_clave in mapa_solvs:
                reg["estado_ente"] = mapa_solvs[ente_clave]["estado"]
                reg["comentario_ente"] = mapa_solvs[ente_clave]["comentario"]

    horarios_stage = _build_horarios_stage(rfc, info.get("registros", []))
    return render_template(
        "detalle_rfc.html",
        rfc=rfc,
        info=info,
        es_luis=es_luis,
        horarios_stage=horarios_stage,
        week_days=WEEK_DAYS,
        db_manager=db_manager,
        _sanitize_text=_sanitize_text,
    )


@app.route("/resultados/<rfc>/horarios", methods=["POST"])
def guardar_horarios_rfc(rfc):
    if not session.get("autenticado"):
        return redirect(url_for("login"))

    ente = request.form.get("ente", "")
    if not _can_edit_ente(ente):
        return redirect(url_for("resultados_por_rfc", rfc=rfc))

    horarios = []
    for day_number, _day_label in WEEK_DAYS:
        horarios.append({
            "dia_semana": day_number,
            "hora_inicio": request.form.get(f"hora_inicio_{day_number}", ""),
            "hora_fin": request.form.get(f"hora_fin_{day_number}", ""),
            "observaciones": request.form.get(f"observaciones_{day_number}", ""),
        })

    filas = db_manager.guardar_horarios_laborales(
        rfc,
        ente,
        horarios,
        usuario=session.get("usuario", ""),
    )
    log.info("Horarios actualizados rfc=%s ente=%s filas=%s", rfc, ente, filas)
    return redirect(url_for("resultados_por_rfc", rfc=rfc, _anchor="etapa2"))


@app.route("/resultados/<rfc>/observaciones-beta", methods=["POST"])
def guardar_observacion_beta_rfc(rfc):
    if not session.get("autenticado"):
        return redirect(url_for("login"))

    ente = request.form.get("ente", "")
    if not _can_edit_ente(ente):
        return redirect(url_for("resultados_por_rfc", rfc=rfc))

    observacion = request.form.get("observacion_beta", "")
    estatus = request.form.get("estatus_beta", "Borrador")
    filas = db_manager.guardar_observacion_beta(
        rfc,
        ente,
        observacion,
        estatus=estatus,
        usuario=session.get("usuario", ""),
    )
    log.info("Observacion beta actualizada rfc=%s ente=%s filas=%s", rfc, ente, filas)
    return redirect(url_for("resultados_por_rfc", rfc=rfc, _anchor="beta-observaciones"))


@app.route("/solventacion/<rfc>", methods=["GET", "POST"])
def solventacion_detalle(rfc):
    if not _es_usuario_luis():
        return redirect(url_for("reporte_por_ente"))

    ente_sel = request.args.get("ente")

    if request.method == "POST":
        estado = request.form.get("estado")
        comentario = request.form.get("valoracion") or request.form.get("solventacion", "")
        catalogo = request.form.get("catalogo")
        otro_texto = request.form.get("otro_texto")
        ente_post = request.form.get("ente") or ente_sel
        filas = db_manager.actualizar_solventacion(rfc, estado, comentario, catalogo=catalogo, otro_texto=otro_texto, ente=ente_post)
        log.info("Solventación rfc=%s ente=%s filas=%s", rfc, ente_post, filas)
        return redirect(url_for("resultados_por_rfc", rfc=rfc))

    info = db_manager.obtener_resultados_por_rfc(rfc)
    if not info:
        return render_template("empty.html", mensaje="No hay registros para este RFC.")

    # --- Agregar solventación previa (si existe) ---
    conn = db_manager._connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT estado, comentario, catalogo, otro_texto FROM solventaciones WHERE rfc=? AND ente=?",
        (rfc, db_manager.normalizar_ente_clave(ente_sel or "GENERAL"))
    )
    row = cur.fetchone()
    conn.close()

    estado_prev = row["estado"] if row else info.get("estado")
    valoracion_prev = row["comentario"] if row else info.get("solventacion", "")
    catalogo_prev = row["catalogo"] if row else ""
    otro_texto_prev = row["otro_texto"] if row else ""

    return render_template(
        "solventacion.html",
        rfc=rfc,
        info=info,
        estado_prev=estado_prev,
        valoracion_prev=valoracion_prev,
        catalogo_prev=catalogo_prev,
        otro_texto_prev=otro_texto_prev
    )

# -----------------------------------------------------------
# ACTUALIZAR ESTADO (AJAX)
# -----------------------------------------------------------
@app.route("/actualizar_estado", methods=["POST"])
def actualizar_estado():
    if not _es_usuario_luis():
        return jsonify({"error": "No autorizado"}), 403

    data = request.get_json(silent=True) or {}
    rfc = data.get("rfc")
    estado = data.get("estado")
    # Aceptar tanto "valoracion" como "solventacion" para compatibilidad
    comentario = data.get("valoracion") or data.get("solventacion", "")
    catalogo = data.get("catalogo")
    otro_texto = data.get("otro_texto")
    ente = data.get("ente")  # opcional

    if not rfc:
        return jsonify({"error": "Falta el RFC"}), 400
    try:
        filas = db_manager.actualizar_solventacion(rfc, estado, comentario, catalogo=catalogo, otro_texto=otro_texto, ente=ente)
        log.info("AJAX solventación rfc=%s ente=%s -> %s", rfc, ente, estado)
        return jsonify({"mensaje": f"Registro actualizado ({filas} filas)", "estatus": estado})
    except Exception as e:
        log.exception("Error en actualizar_estado")
        return jsonify({"error": str(e)}), 500


@app.route("/prevalidar_duplicado", methods=["POST"])
def prevalidar_duplicado():
    if not _es_usuario_luis():
        return jsonify({"error": "No autorizado"}), 403

    data = request.get_json(silent=True) or {}
    rfc = str(data.get("rfc", "")).strip()
    ente = str(data.get("ente", "")).strip()
    estado = str(data.get("estado", "Sin valoración")).strip() or "Sin valoración"
    comentario = str(data.get("valoracion", "")).strip()
    catalogo = str(data.get("catalogo", "")).strip()
    otro_texto = str(data.get("otro_texto", "")).strip()

    if not rfc or not ente:
        return jsonify({"error": "Faltan RFC o ente"}), 400
    if estado not in {"Sin valoración", "Solventado"}:
        return jsonify({"error": "Estado de pre-validación no permitido"}), 400
    if estado == "Solventado" and not catalogo:
        return jsonify({"error": "Selecciona una opción de catálogo"}), 400
    if catalogo == "Otro" and not otro_texto:
        return jsonify({"error": "Debes especificar texto para opción Otro"}), 400

    if estado == "Sin valoración":
        comentario, catalogo, otro_texto = "", "", ""

    try:
        usuario = session.get("usuario", "luis")
        entes_cruce = db_manager.obtener_entes_con_cruce_por_rfc(rfc)
        if not entes_cruce:
            entes_cruce = [ente]

        entes_afectados = []
        filas = 0
        for ente_obj in sorted(set(entes_cruce)):
            ente_norm = db_manager.normalizar_ente_clave(ente_obj) or ente_obj
            entes_afectados.append(ente_norm)
            filas += db_manager.guardar_prevalidacion_duplicado(
                rfc, ente_norm, estado, comentario, catalogo, otro_texto, usuario
            )

        return jsonify({
            "mensaje": f"Pre-validación aplicada en {len(entes_afectados)} ente(s)",
            "filas": filas,
            "entes_afectados": entes_afectados,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/validar_datos", methods=["POST"])
def validar_datos():
    if not _es_usuario_validador():
        return jsonify({"error": "No autorizado"}), 403
    try:
        db_manager.marcar_resultados_validados(session.get("usuario", "luis"))
        return redirect(url_for("reporte_por_ente"))
    except Exception as e:
        log.exception("Error al validar datos")
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"error": "No fue posible validar los datos."}), 500
        session["validacion_error_msg"] = _build_validacion_error_message(e, "validar")
        return redirect(url_for("reporte_por_ente", validacion_error=1))


@app.route("/cancelar_validacion", methods=["POST"])
def cancelar_validacion():
    if not _es_usuario_validador():
        return jsonify({"error": "No autorizado"}), 403
    try:
        db_manager.desmarcar_resultados_validados(session.get("usuario", "luis"))
        return redirect(url_for("reporte_por_ente"))
    except Exception as e:
        log.exception("Error al cancelar validación")
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"error": "No fue posible cancelar la validación."}), 500
        session["validacion_error_msg"] = _build_validacion_error_message(e, "cancelar")
        return redirect(url_for("reporte_por_ente", validacion_error=1))

# -----------------------------------------------------------
# EXPORTAR POR ENTE (JSON + Excel)
# -----------------------------------------------------------
@app.route("/exportar_por_ente")
def exportar_por_ente():
    ente_sel = request.args.get("ente", "").strip()
    formato = request.args.get("formato", "").lower()
    entes_usuario = session.get("entes", [])
    es_luis = _es_usuario_luis()
    modo_permiso = "ALL" if es_luis else _allowed_all(entes_usuario)

    if not es_luis and not db_manager.resultados_validados():
        return redirect(url_for("reporte_por_ente"))
    if not ente_sel:
        return redirect(url_for("reporte_por_ente"))

    resultados_base = db_manager.obtener_cruces_reales()
    resultados = (
        _filtrar_duplicados_reales(resultados_base)
        if es_luis else _filtrar_duplicados_con_visibilidad(resultados_base)
    )
    permitidos = []
    for r in resultados:
        for ente in (r.get("entes") or []):
            if _ente_display(ente) == ente_sel and _puede_ver_ente(ente, entes_usuario, modo_permiso):
                permitidos.append(r)
                break
    resumen_ambitos = _resumen_duplicidad_por_ambito(permitidos, entes_usuario, modo_permiso)
    ambito_por_rfc = resumen_ambitos["ambito_por_rfc"]
    filas = _construir_filas_export(permitidos)

    # Filtrar registros con N/A en Quincenas (sin intersección temporal)
    filas = [f for f in filas if f.get("Quincenas") != "N/A"]

    for fila in filas:
        rfc = str(fila.get("RFC", "")).strip().upper()
        fila["Ámbito RFC"] = _ambito_rfc_label(ambito_por_rfc.get(rfc))
        fila["Ámbito Ente Origen"] = _ambito_ente_origen(fila.get("Ente Origen", ""))

    # Filtrar por ente seleccionado
    filas = [f for f in filas if _ente_match(ente_sel, [f["Ente Origen"]])]
    if not filas:
        return jsonify({"error": "No se encontraron registros para el ente seleccionado."}), 404

    if formato == "json" or request.is_json:
        if not es_luis:
            for fila in filas:
                fila.pop("Total Percepciones", None)
        return jsonify({"ente": ente_sel, "total_registros": len(filas), "datos": filas})

    columnas_export = [
        "RFC", "Nombre", "Puesto", "Fecha Alta", "Fecha Baja",
        "Ente Origen", "Ámbito Ente Origen", "Ámbito RFC",
        "Entes Incompatibilidad", "Quincenas", "Estatus", "Solventación"
    ]
    if es_luis:
        columnas_export.append("Total Percepciones")
    else:
        for fila in filas:
            fila.pop("Total Percepciones", None)

    df = pd.DataFrame(filas)[columnas_export]
    df.sort_values(by=["Ente Origen", "RFC"], inplace=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        hoja = f"{_ente_sigla(ente_sel)}"[:31]
        df.to_excel(writer, index=False, sheet_name=hoja)

    output.seek(0)
    nombre = f"SASP_{_ente_sigla(ente_sel)}_Duplicidades.xlsx"
    return send_file(output, download_name=nombre, as_attachment=True)

# -----------------------------------------------------------
# EXPORTAR GENERAL (JSON + Excel)
# -----------------------------------------------------------
@app.route("/exportar_general")
def exportar_excel_general():
    formato = request.args.get("formato", "").lower()
    es_luis = _es_usuario_luis()
    entes_usuario = session.get("entes", [])
    modo_permiso = "ALL" if es_luis else _allowed_all(entes_usuario)

    if not es_luis and not db_manager.resultados_validados():
        return redirect(url_for("reporte_por_ente"))

    resultados_base = db_manager.obtener_cruces_reales()
    resultados = (
        _filtrar_duplicados_reales(resultados_base)
        if es_luis else _filtrar_duplicados_con_visibilidad(resultados_base)
    )
    filas = _construir_filas_export(resultados)
    resumen_ambitos = _resumen_duplicidad_por_ambito(resultados, entes_usuario, modo_permiso)
    ambito_por_rfc = resumen_ambitos["ambito_por_rfc"]

    # Filtrar registros con N/A en Quincenas (sin intersección temporal)
    filas = [f for f in filas if f.get("Quincenas") != "N/A"]

    exporta_json = (formato == "json" or request.is_json)

    # Respetar visibilidad por usuario:
    # - incluir filas cuando el Ente Origen esté a su cargo, o bien cuando
    #   exista al menos un Ente Incompatibilidad relacionado con su cargo.
    # - excluir filas sin relación con los entes del usuario.
    # - en JSON conservar filtro por visibilidad para Entes Incompatibilidad.
    # - en Excel conservar la lista completa de Entes Incompatibilidad.
    if not es_luis:
        filas_visibles = []
        for fila in filas:
            ente_origen = str(fila.get("Ente Origen", "")).strip()
            entes_incompat = [
                e.strip()
                for e in str(fila.get("Entes Incompatibilidad", "")).split(",")
                if e.strip()
            ]
            origen_visible = _puede_ver_ente(ente_origen, entes_usuario, modo_permiso)
            incompat_visibles = [
                e for e in entes_incompat
                if _puede_ver_ente(e, entes_usuario, modo_permiso)
            ]
            fila_relacionada = origen_visible or bool(incompat_visibles)
            if not fila_relacionada:
                continue

            fila_segura = dict(fila)
            if exporta_json:
                fila_segura["Entes Incompatibilidad"] = (
                    ", ".join(incompat_visibles) if incompat_visibles else "Sin otros entes"
                )

            filas_visibles.append(fila_segura)

        filas = filas_visibles

    for fila in filas:
        rfc = str(fila.get("RFC", "")).strip().upper()
        fila["Ámbito RFC"] = _ambito_rfc_label(ambito_por_rfc.get(rfc))
        fila["Ámbito Ente Origen"] = _ambito_ente_origen(fila.get("Ente Origen", ""))

    if not filas:
        return jsonify({"error": "Sin datos para exportar."}), 404

    if exporta_json:
        if not es_luis:
            for fila in filas:
                fila.pop("Total Percepciones", None)
        return jsonify({"total_registros": len(filas), "datos": filas})

    columnas_export = [
        "RFC", "Nombre", "Puesto", "Fecha Alta", "Fecha Baja",
        "Ente Origen", "Ámbito Ente Origen", "Ámbito RFC",
        "Entes Incompatibilidad", "Quincenas", "Estatus", "Solventación"
    ]
    if es_luis:
        columnas_export.append("Total Percepciones")
    else:
        for fila in filas:
            fila.pop("Total Percepciones", None)

    df = pd.DataFrame(filas)[columnas_export]
    if es_luis:
        df.rename(columns={"Total Percepciones": "Total de Percepciones Anual"}, inplace=True)
    else:
        df["Importe por cuantificar por auditor"] = ""
    df.sort_values(by=["RFC", "Ente Origen"], inplace=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Duplicidades_Generales")
        resumen_metricas = pd.DataFrame([
            {"Metrica": "Casos de duplicidad estatales (RFC unicos)", "Valor": resumen_ambitos["conteos"].get("estatales", 0)},
            {"Metrica": "Casos de duplicidad municipales (RFC unicos)", "Valor": resumen_ambitos["conteos"].get("municipios", 0)},
            {"Metrica": "Casos de duplicidad mixtos estado-municipio (RFC unicos)", "Valor": resumen_ambitos["conteos"].get("mixtos", 0)},
            {"Metrica": "Total general de duplicidad (RFC unicos)", "Valor": resumen_ambitos["total_general"]},
        ])
        resumen_metricas.to_excel(writer, index=False, sheet_name="Resumen_Metricas")
        resumen = (
            df.groupby(["Ámbito Ente Origen", "Ente Origen"]).agg(RFC_unicos_ente=("RFC", "nunique"))
              .reset_index().sort_values(["Ámbito Ente Origen", "Ente Origen"])
        )
        resumen["Nota"] = "No sumable entre entes; usa Resumen_Metricas."
        resumen.to_excel(writer, index=False, sheet_name="Resumen_por_Ente")

    output.seek(0)
    return send_file(output, download_name="SASP_Duplicidades_Generales.xlsx", as_attachment=True)


@app.route("/exportar_solventados")
def exportar_solventados():
    if not _es_usuario_luis():
        return "No autorizado", 403

    filtro_ente = request.args.get("ente", "").strip()
    ambito_sel = _normalizar_ambito(request.args.get("ambito", "estatales"))

    resultados = _filtrar_duplicados_reales(db_manager.obtener_cruces_reales())
    resumen_ambitos = _resumen_duplicidad_por_ambito(resultados, [], "ALL")
    ambito_por_rfc = resumen_ambitos["ambito_por_rfc"]
    filas_general = _construir_filas_export(resultados)
    filas_general = [f for f in filas_general if f.get("Quincenas") != "N/A"]

    rfcs = sorted({
        str(f.get("RFC", "")).strip().upper()
        for f in filas_general
        if str(f.get("RFC", "")).strip()
    })
    prevalidaciones_por_rfc = db_manager.get_prevalidaciones_por_rfcs(rfcs)

    filas = []
    for fila in filas_general:
        rfc = str(fila.get("RFC", "")).strip().upper()
        ente_origen = str(fila.get("Ente Origen", "")).strip()
        if not rfc or not ente_origen:
            continue
        if filtro_ente and ente_origen != filtro_ente:
            continue
        if ambito_por_rfc.get(rfc) != ambito_sel:
            continue

        ente_clave = db_manager.normalizar_ente_clave(ente_origen) or ente_origen
        pre = prevalidaciones_por_rfc.get(rfc, {}).get(ente_clave, {})
        pre_estado = str(pre.get("estado", "Sin valoración")).strip().upper()
        if pre_estado != "SOLVENTADO":
            continue

        filas.append({
            "RFC": fila.get("RFC", ""),
            "Nombre": fila.get("Nombre", ""),
            "Puesto": fila.get("Puesto", ""),
            "Ente Origen": fila.get("Ente Origen", ""),
            "Ámbito RFC": _ambito_rfc_label(ambito_por_rfc.get(rfc)),
            "Fecha Alta": fila.get("Fecha Alta", ""),
            "Fecha Baja": fila.get("Fecha Baja", ""),
            "Total Percepciones Anual": fila.get("Total Percepciones", ""),
            "Entes Incompatibilidad": fila.get("Entes Incompatibilidad", ""),
            "Quincenas Cruce": fila.get("Quincenas", ""),
            "Estatus": fila.get("Estatus", ""),
            "Solventacion": fila.get("Solventación", ""),
        })

    df = pd.DataFrame(filas, columns=[
        "RFC",
        "Nombre",
        "Puesto",
        "Ente Origen",
        "Ámbito RFC",
        "Entes Incompatibilidad",
        "Fecha Alta",
        "Fecha Baja",
        "Quincenas Cruce",
        "Estatus",
        "Solventacion",
        "Total Percepciones Anual",
    ])
    df.sort_values(by=["RFC", "Ente Origen"], inplace=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Solventados")
    output.seek(0)
    return send_file(output, download_name="SASP_Solventados.xlsx", as_attachment=True)

# -----------------------------------------------------------
# CATÁLOGOS
# -----------------------------------------------------------
@app.route("/catalogos")
def catalogos_home():
    entes = db_manager.listar_entes()
    municipios = db_manager.listar_municipios()
    return render_template("catalogos.html", entes=entes, municipios=municipios)

# -----------------------------------------------------------
# CONTEXTO GLOBAL
# -----------------------------------------------------------
@app.context_processor
def inject_helpers():
    return {
        "_sanitize_text": _sanitize_text,
        "_ente_display": _ente_display,
        "_ente_sigla": _ente_sigla,
        "db_manager": db_manager
    }

@app.route('/descargar-plantilla')
def descargar_plantilla():
    ruta = os.path.join(app.root_path, 'static')
    return send_from_directory(ruta, 'Plantilla_Quincenas.xlsx', as_attachment=True)


@app.route("/descargar-plantilla-horarios")
def descargar_plantilla_horarios():
    df = pd.DataFrame([{
        "RFC": "ABCD900101XXX",
        "Nombre": "NOMBRE DE PRUEBA",
        "Ente": "SEPE",
        "Cargo": "DOCENTE",
        "Dia": "Lunes",
        "Hora_inicio": "08:00",
        "Hora_fin": "12:00",
        "Fecha_inicio_vigencia": date.today().isoformat(),
        "Fecha_fin_vigencia": "",
        "Periodo": "2026-A",
        "Observaciones": "",
        "Estatus": "activo",
    }])

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Horarios")
    output.seek(0)
    return send_file(output, download_name="Plantilla_Horarios.xlsx", as_attachment=True)

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    port = config.PORT
    log.info("Levantando Flask en 0.0.0.0:%s (debug=%s)", port, True)
    app.run(host="0.0.0.0", port=port, debug=True)
