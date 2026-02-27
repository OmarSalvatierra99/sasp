# ===========================================================
# app.py — SASP / SCIL 2025
# Sistema de Auditoría de Servicios Personales
# Órgano de Fiscalización Superior del Estado de Tlaxcala
# ===========================================================

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, send_file, send_from_directory
)
import os
import logging
import pandas as pd
from io import BytesIO
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

log_dir = Path('log')
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        RotatingFileHandler('log/app.log', maxBytes=10*1024*1024, backupCount=10),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("SCIL")

# -----------------------------------------------------------
# Configuración
# -----------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "ofs_sasp_2025")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.environ.get("SCIL_DB", str(BASE_DIR / "scil.db"))
db_manager = DatabaseManager(DB_PATH)
set_db_manager(db_manager)
data_processor = DataProcessor()  # usa el mismo db_path por defecto

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
    libres = {"login", "static"}
    if request.endpoint not in libres and not session.get("autenticado"):
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"error": "Sesión expirada o no autorizada"}), 403
        return redirect(url_for("login"))

# -----------------------------------------------------------
# LOGIN / LOGOUT
# -----------------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        clave = request.form.get("clave", "").strip()
        user = db_manager.get_usuario(usuario, clave)
        if not user:
            log.warning("Login fallido para usuario=%s", usuario)
            return render_template("login.html", error="Credenciales inválidas")

        session.update({
            "usuario": user["usuario"],
            "nombre": user["nombre"],
            "autenticado": True
        })

        # Normalizar entes del usuario a CLAVE oficial cuando aplique
        entes_norm = []
        for e in user["entes"]:
            clave_norm = db_manager.normalizar_ente_clave(e)
            if clave_norm:
                entes_norm.append(clave_norm)
            else:
                entes_norm.append(e)

        # Asignar permisos especiales
        if user["usuario"].lower() in {"odilia", "luis", "felipe"}:
            # Superusuarios: acceso total
            session["entes"] = ["TODOS"]
        else:
            session["entes"] = entes_norm

        log.info("Login ok usuario=%s entes=%s", user["usuario"], ",".join(session["entes"]))
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/logout")
def logout():
    usuario = session.get("usuario")
    session.clear()
    log.info("Logout usuario=%s", usuario)
    return redirect("http://192.168.1.248/SIFEET-2025/")

# -----------------------------------------------------------
# DASHBOARD
# -----------------------------------------------------------
@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html", nombre=session.get("nombre"))

# -----------------------------------------------------------
# CARGA MASIVA (DataProcessor cruza por RFC y QNAs)
# -----------------------------------------------------------
@app.route("/upload_laboral", methods=["POST"])
def upload_laboral():
    if not session.get("autenticado"):
        return jsonify({"error": "No autorizado"}), 403

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No se enviaron archivos"})

    try:
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
    return session.get("usuario", "").strip().lower() in {"luis", "odilia", "felipe"}


def _es_usuario_validador():
    return session.get("usuario", "").strip().lower() == "luis"


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

    rfcs = sorted({str(r.get("rfc", "")).strip().upper() for r in resultados if str(r.get("rfc", "")).strip()})
    prevalidaciones_por_rfc = db_manager.get_prevalidaciones_por_rfcs(rfcs)

    for r in resultados:
        rfc_actual = str(r.get("rfc", "")).strip().upper()
        mapa_pre = prevalidaciones_por_rfc.get(rfc_actual, {})
        for ente_clave in (r.get("entes") or []):
            if not _puede_ver_ente(ente_clave, entes_usuario, modo_permiso):
                continue
            if not _coincide_ambito(ambito_sel, _tipo_ente(ente_clave)):
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
                }
            if not detalle_agrupado[key]["observacion"] and comentario:
                detalle_agrupado[key]["observacion"] = comentario
            detalle_agrupado[key]["entes"].add(display)

    detalle = []
    for item in detalle_agrupado.values():
        entes = sorted(item["entes"])
        detalle.append({
            "rfc": item["rfc"],
            "nombre": item["nombre"],
            "ente": ", ".join(entes),
            "motivo": item["motivo"],
            "observacion": item["observacion"],
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
    ambito_sel = request.args.get("ambito", "estatales").strip().lower()
    if ambito_sel not in {"estatales", "municipios"}:
        ambito_sel = "estatales"

    validacion_error = request.args.get("validacion_error", "") == "1"
    validacion_error_msg = ""
    if validacion_error:
        validacion_error_msg = session.pop("validacion_error_msg", "No fue posible validar los datos.")

    entes_usuario = session.get("entes", [])
    es_luis = _es_usuario_luis()
    es_validador = _es_usuario_validador()
    resultados_validados = db_manager.resultados_validados()
    mostrar_duplicados = es_luis or resultados_validados
    mostrar_metricas = es_validador or resultados_validados
    modo_permiso = "ALL" if es_luis else _allowed_all(entes_usuario)

    resultados_base = db_manager.obtener_cruces_reales()
    resultados = (
        _filtrar_duplicados_reales(resultados_base)
        if es_luis else _filtrar_duplicados_con_visibilidad(resultados_base)
    )
    trabajadores_por_ente_map = db_manager.contar_trabajadores_por_ente()
    trabajadores_detallados = db_manager.obtener_trabajadores_por_ente()
    catalogo = db_manager.listar_entes() + db_manager.listar_municipios()

    agrupado = {}
    entes_info = {}

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

            for ente_clave in (r.get("entes") or []):
                if not es_luis and _es_prevalidado_oculto(mapa_pre, ente_clave):
                    continue
                if not _puede_ver_ente(ente_clave, entes_usuario, modo_permiso):
                    continue
                if not _coincide_ambito(ambito_sel, _tipo_ente(ente_clave)):
                    continue

                display = _ente_display(ente_clave)
                if filtro_ente and display != filtro_ente:
                    continue
                if display not in entes_info:
                    continue

                otros_entes = []
                for e in (r.get("entes") or []):
                    if _sanitize_text(e) != _sanitize_text(ente_clave):
                        s = _ente_sigla(e)
                        if s not in otros_entes:
                            otros_entes.append(s)

                estado_default = r.get("estado", "Sin valoración")
                estado_entes = {}
                for en in (r.get("entes") or []):
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
        if not _coincide_ambito(ambito_sel, _tipo_ente(str(ente_clave))):
            continue

        display = _ente_display(str(ente_clave))
        if filtro_ente and display != filtro_ente:
            continue

        for trab in trabajadores:
            trabajadores_por_ente_final.setdefault(display, []).append(trab)
            rfc = str(trab.get("rfc", "")).strip().upper()
            if rfc:
                rfc_procesados.add(rfc)
            registros_cargados += 1

    rfc_duplicados = set()
    for r in resultados:
        entes_visibles = []
        for ente_cruce in (r.get("entes") or []):
            if _puede_ver_ente(str(ente_cruce), entes_usuario, modo_permiso):
                if not _coincide_ambito(ambito_sel, _tipo_ente(str(ente_cruce))):
                    continue
                entes_visibles.append(str(ente_cruce))
        entes_visibles = sorted(set(entes_visibles))
        if len(entes_visibles) < 2:
            continue

        if filtro_ente:
            if not any(_ente_display(e) == filtro_ente for e in entes_visibles):
                continue

        rfc = str(r.get("rfc", "")).strip().upper()
        if rfc:
            rfc_duplicados.add(rfc)

    entes_visibles = sum(1 for nombre in entes_info_ordenado if (not filtro_ente or nombre == filtro_ente))
    trabajadores_procesados = len(rfc_procesados)
    duplicados_detectados = len(rfc_duplicados)
    indice_duplicidad = round((duplicados_detectados / trabajadores_procesados) * 100, 2) if trabajadores_procesados else 0.0
    entes_con_duplicidad = sum(
        1
        for nombre, info in entes_info_ordenado.items()
        if (not filtro_ente or nombre == filtro_ente) and int(info.get("duplicados", 0)) > 0
    )

    resumen_auditoria = [
        {"m": "Entes analizados", "v": str(entes_visibles)},
        {"m": "Trabajadores analizados (RFC únicos)", "v": f"{trabajadores_procesados:,}"},
        {"m": "Casos de duplicidad (RFC únicos)", "v": str(duplicados_detectados)},
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

    return render_template("detalle_rfc.html", rfc=rfc, info=info, es_luis=es_luis)


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
    filas = _construir_filas_export(permitidos)

    # Filtrar registros con N/A en Quincenas (sin intersección temporal)
    filas = [f for f in filas if f.get("Quincenas") != "N/A"]

    # Filtrar por ente seleccionado
    filas = [f for f in filas if _ente_match(ente_sel, [f["Ente Origen"]])]
    if not filas:
        return jsonify({"error": "No se encontraron registros para el ente seleccionado."}), 404

    if formato == "json" or request.is_json:
        return jsonify({"ente": ente_sel, "total_registros": len(filas), "datos": filas})

    df = pd.DataFrame(filas)[[
        "RFC", "Nombre", "Puesto", "Fecha Alta", "Fecha Baja", "Total Percepciones",
        "Ente Origen", "Entes Incompatibilidad", "Quincenas", "Estatus", "Solventación"
    ]]
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

    # Filtrar registros con N/A en Quincenas (sin intersección temporal)
    filas = [f for f in filas if f.get("Quincenas") != "N/A"]

    # Respetar visibilidad por usuario: no exportar entes/municipios fuera de su cargo.
    if not es_luis:
        filas_visibles = []
        for fila in filas:
            ente_origen = str(fila.get("Ente Origen", "")).strip()
            if not _puede_ver_ente(ente_origen, entes_usuario, modo_permiso):
                continue

            entes_incompat = [
                e.strip()
                for e in str(fila.get("Entes Incompatibilidad", "")).split(",")
                if e.strip()
            ]
            entes_incompat_visibles = [
                e for e in entes_incompat
                if _puede_ver_ente(e, entes_usuario, modo_permiso)
            ]

            fila_segura = dict(fila)
            fila_segura["Entes Incompatibilidad"] = (
                ", ".join(entes_incompat_visibles) if entes_incompat_visibles else "Sin otros entes"
            )
            filas_visibles.append(fila_segura)

        filas = filas_visibles

    if not filas:
        return jsonify({"error": "Sin datos para exportar."}), 404

    if formato == "json" or request.is_json:
        return jsonify({"total_registros": len(filas), "datos": filas})

    df = pd.DataFrame(filas)[[
        "RFC", "Nombre", "Puesto", "Fecha Alta", "Fecha Baja", "Total Percepciones",
        "Ente Origen", "Entes Incompatibilidad", "Quincenas", "Estatus", "Solventación"
    ]]
    df.rename(columns={"Total Percepciones": "Total de Percepciones Anual"}, inplace=True)
    df.sort_values(by=["RFC", "Ente Origen"], inplace=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Duplicidades_Generales")
        resumen = (
            df.groupby("Ente Origen").agg(Total_RFCs=("RFC", "nunique"))
              .reset_index().sort_values("Ente Origen")
        )
        resumen.to_excel(writer, index=False, sheet_name="Resumen_por_Ente")

    output.seek(0)
    return send_file(output, download_name="SASP_Duplicidades_Generales.xlsx", as_attachment=True)


@app.route("/exportar_solventados")
def exportar_solventados():
    if not _es_usuario_luis():
        return "No autorizado", 403

    filtro_ente = request.args.get("ente", "").strip()
    ambito_sel = request.args.get("ambito", "estatales").strip().lower()
    if ambito_sel not in {"estatales", "municipios"}:
        ambito_sel = "estatales"

    resultados = _filtrar_duplicados_reales(db_manager.obtener_cruces_reales())
    detalle = _construir_detalle_solventados(
        resultados,
        filtro_ente,
        ambito_sel,
        session.get("entes", []),
        "ALL",
    )["detalle"]

    filas_general = _construir_filas_export(resultados)
    qnas_por_rfc_ente = {}
    for fila in filas_general:
        qnas_txt = str(fila.get("Quincenas", "")).strip()
        if not qnas_txt or qnas_txt == "N/A":
            continue
        rfc_key = str(fila.get("RFC", "")).strip().upper()
        ente_key = str(fila.get("Ente Origen", "")).strip()
        if not rfc_key or not ente_key:
            continue
        qnas_por_rfc_ente.setdefault((rfc_key, ente_key), set()).add(qnas_txt)

    filas = []
    for item in detalle:
        rfc_item = str(item.get("rfc", "")).strip().upper()
        entes_item = [e.strip() for e in str(item.get("ente", "")).split(",") if e.strip()]
        qnas_item = set()
        for ente in entes_item:
            qnas_item.update(qnas_por_rfc_ente.get((rfc_item, ente), set()))
        quincenas_incompat = ", ".join(sorted(qnas_item)) if qnas_item else "N/A"

        filas.append({
            "RFC": item["rfc"],
            "Nombre": item["nombre"],
            "Ente Origen": item["ente"],
            "Estatus": "Solventado",
            "Motivo de Solventación": item["motivo"],
            "Observación": item["observacion"],
            "Quincenas de incompatibilidad": quincenas_incompat,
        })

    df = pd.DataFrame(filas, columns=[
        "RFC",
        "Nombre",
        "Ente Origen",
        "Estatus",
        "Motivo de Solventación",
        "Observación",
        "Quincenas de incompatibilidad",
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
