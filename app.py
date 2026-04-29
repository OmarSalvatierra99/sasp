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
import pandas as pd
from io import BytesIO
from itertools import combinations
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


def _get_login_users():
    return ordered_users("05-sasp", priority=LOGIN_USER_PRIORITY)


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
    return render_template(
        "dashboard.html",
        nombre=session.get("nombre"),
        usuario=session.get("usuario"),
    )

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

        response = {
            "mensaje": f"Procesamiento completado. {n_insertados} nuevos registros, {n_actualizados} actualizados.",
            "total_procesados": len(registros_individuales),
            "insertados": n_insertados,
            "actualizados": n_actualizados,
            "alertas": alertas
        }

        return jsonify(response)

    except Exception as e:
        log.exception("Error en upload_laboral")
        return jsonify({"error": f"Error al procesar archivos: {e}"}), 500

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
    per_ente = []
    total_weekly = 0.0
    total_annual = 0.0

    for reg in registros or []:
        ente_clave = db_manager.normalizar_ente_clave(reg.get("ente")) or reg.get("ente")
        annual_amount = _monto_num(reg.get("monto"))
        weekly_amount = _weekly_amount(reg.get("monto"))
        total_annual += annual_amount
        total_weekly += weekly_amount

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
            "weekly_minutes": weekly_minutes,
            "weekly_hours_label": _format_minutes(weekly_minutes),
            "can_edit": _can_edit_ente(ente_clave),
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
    return send_from_directory(ruta, 'Plantilla.xlsx', as_attachment=True)

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    port = config.PORT
    log.info("Levantando Flask en 0.0.0.0:%s (debug=%s)", port, True)
    app.run(host="0.0.0.0", port=port, debug=True)
