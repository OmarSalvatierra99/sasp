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
from functools import lru_cache
from core.database import DatabaseManager
from core.data_processor import DataProcessor

# -----------------------------------------------------------
# Logging
# -----------------------------------------------------------
from pathlib import Path
from logging.handlers import RotatingFileHandler

log_dir = Path('log')
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        RotatingFileHandler('log/sasp.log', maxBytes=10*1024*1024, backupCount=10),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("SCIL")

# -----------------------------------------------------------
# Configuración
# -----------------------------------------------------------
app = Flask(__name__)
app.secret_key = "ofs_sasp_2025"

DB_PATH = os.environ.get("SCIL_DB", "scil.db")
db_manager = DatabaseManager(DB_PATH)
data_processor = DataProcessor()  # usa el mismo db_path por defecto

log.info("Iniciando SCIL | CWD=%s | DB=%s", os.getcwd(), DB_PATH)

# -----------------------------------------------------------
# Filtros de Jinja2
# -----------------------------------------------------------
@app.template_filter('ordenar_quincenas')
def ordenar_quincenas(qnas):
    """Ordena quincenas (QNA1, QNA2, ..., QNA24) numéricamente."""
    import re
    if not qnas:
        return []
    # Extraer número de cada QNA y ordenar
    def extraer_numero(qna):
        match = re.search(r'\d+', str(qna))
        return int(match.group()) if match else 0
    return sorted(qnas, key=extraer_numero)

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
# Utilidades
# -----------------------------------------------------------
def _sanitize_text(s):
    return str(s or "").strip().upper()


def _allowed_all(entes_usuario):
    """
    Devuelve:
    - 'ALL'         → ENTES + MUNICIPIOS
    - 'ENTES'       → Solo entes
    - 'MUNICIPIOS'  → Solo municipios
    - None          → Sin acceso especial
    Analiza textos como:
    - 'TODOS'
    - 'TODOS LOS ENTES'
    - 'TODOS LOS MUNICIPIOS'
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
    conn = db_manager._connect()
    cur = conn.cursor()

    # Unificación: entes + municipios en un solo catálogo interno
    cur.execute("""
        SELECT clave, siglas, nombre, 'ENTE' AS tipo FROM entes
        UNION ALL
        SELECT clave, siglas, nombre, 'MUNICIPIO' AS tipo FROM municipios
    """)

    data = {}
    for r in cur.fetchall():
        clave = (r["clave"] or "").strip().upper()
        data[clave] = {
            "siglas": (r["siglas"] or "").strip().upper(),
            "nombre": (r["nombre"] or "").strip().upper(),
            "tipo": r["tipo"]  # 'ENTE' o 'MUNICIPIO'
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
            # El usuario podría tener sigla, nombre o clave
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
            return d["siglas"] or d["nombre"] or s
    return s


def _ente_display(v):
    if not v:
        return "Sin Ente"
    s = _sanitize_text(v)
    for k, d in _entes_cache().items():
        if s in {k, d["siglas"], d["nombre"]}:
            return d["siglas"] or d["nombre"] or v
    return v

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
    return redirect(url_for("login"))

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
@app.route("/resultados")
def reporte_por_ente():
    # Obtener cruces reales y filtrar solo los que tienen duplicidad real (intersección de QNAs)
    resultados = db_manager.obtener_cruces_reales()
    resultados_filtrados = _filtrar_duplicados_reales(resultados)

    entes_usuario = session.get("entes", [])
    agrupado = {}

    modo_permiso = _allowed_all(entes_usuario)

    for r in resultados_filtrados:
        # Los entes con cruce real ya fueron calculados en _filtrar_duplicados_reales
        entes_cruce_real = set(r.get("entes_cruce_real", []))

        for e in entes_cruce_real:
            # Determinar tipo de ente (ENTE / MUNICIPIO)
            info_ente = _entes_cache().get(_sanitize_text(e), {})
            tipo_ente = info_ente.get("tipo", "")

            # Evaluar permisos
            if modo_permiso == "ALL":
                permitido = True
            elif modo_permiso == "ENTES":
                permitido = (tipo_ente == "ENTE")
            elif modo_permiso == "MUNICIPIO":
                permitido = (tipo_ente == "MUNICIPIO")
            else:
                permitido = any(_ente_match(eu, [e]) for eu in entes_usuario)

            if not permitido:
                continue

            ente_nombre = _ente_display(e)
            agrupado.setdefault(ente_nombre, {})

            rfc = r.get("rfc")
            puesto = (
                r.get("puesto")
                or ", ".join({reg.get("puesto", "").strip()
                              for reg in (r.get("registros") or [])
                              if reg.get("puesto")})
                or "Sin puesto"
            )

            if rfc not in agrupado[ente_nombre]:
                agrupado[ente_nombre][rfc] = {
                    "rfc": r["rfc"],
                    "nombre": r["nombre"],
                    "puesto": puesto,
                    "entes": set(),
                    "estado": r.get("estado", "Sin valoración"),
                    "estado_entes": {}
                }

            # Agregar todos los entes EXCEPTO el ente actual
            for en in r.get("entes", []):
                if _sanitize_text(en) != _sanitize_text(e):
                    agrupado[ente_nombre][rfc]["entes"].add(_ente_sigla(en))

            mapa_solvs = db_manager.get_solventaciones_por_rfc(r["rfc"])
            estado_default = r.get("estado", "Sin valoración")
            for en in r.get("entes", []):
                if _sanitize_text(en) != _sanitize_text(e):
                    clave = db_manager.normalizar_ente_clave(en)
                    est = mapa_solvs.get(clave, {}).get("estado") if mapa_solvs else None
                    agrupado[ente_nombre][rfc]["estado_entes"][_ente_sigla(en)] = est or estado_default

    # Agregar TODOS los entes del catálogo (incluso con 0 trabajadores)
    todos_entes = db_manager.listar_entes()
    todos_municipios = db_manager.listar_municipios()
    todos_entidades = todos_entes + todos_municipios
    entes_info = {}       # {nombre_ente: {siglas, total_trabajadores}}
    entes_con_datos = {}  # Entes con trabajadores cargados (incluso sin duplicidades)

    # Contar trabajadores por ente desde la tabla de registros
    trabajadores_por_ente_clave = db_manager.contar_trabajadores_por_ente()

    # Convertir claves a nombres display
    trabajadores_por_ente = {}
    for clave, total in trabajadores_por_ente_clave.items():
        ente_display = _ente_display(clave)
        trabajadores_por_ente[ente_display] = total

    for ente in todos_entidades:
        ente_nombre = ente['siglas'] or ente['nombre']

        # Determinar tipo de ente desde el catálogo unificado
        info_ente = _entes_cache().get(_sanitize_text(ente['clave']), {})
        tipo_ente = info_ente.get("tipo", "ENTE")  # por defecto ENTES

        # Verificar permisos según modo
        if modo_permiso == "ALL":
            permitido = True
        elif modo_permiso == "ENTES":
            permitido = (tipo_ente == "ENTE")
        elif modo_permiso == "MUNICIPIOS":
            permitido = (tipo_ente == "MUNICIPIO")
        else:
            permitido = any(_ente_match(eu, [ente['clave']]) for eu in entes_usuario)

        if not permitido:
            continue

        # Si el ente no tiene trabajadores en agrupado, agregarlo con lista vacía
        if ente_nombre not in agrupado:
            agrupado[ente_nombre] = {}

        total_trabajadores = trabajadores_por_ente.get(ente_nombre, 0)
        total_duplicados = len(agrupado.get(ente_nombre, {}))

        entes_info[ente_nombre] = {
            'num': ente['num'],
            'siglas': ente['siglas'],
            'nombre_completo': ente['nombre'],
            'total': total_trabajadores,
            'duplicados': total_duplicados,
            'tipo': tipo_ente  # ENTE o MUNICIPIO
        }

        # Si tiene trabajadores pero no duplicidades, agregarlo a entes_con_datos
        if total_trabajadores > 0 and total_duplicados == 0:
            entes_con_datos[ente_nombre] = {
                'siglas': ente['siglas'],
                'nombre_completo': ente['nombre'],
                'total': total_trabajadores
            }

    # Función de ordenamiento por NUM jerárquico
    def orden_por_num(item):
        """Ordena por NUM respetando jerarquía (1.2.3 antes de 1.10)"""
        ente_nombre, info = item
        num_str = str(info.get('num', '999')).strip().rstrip('.')
        partes = []
        for parte in num_str.split('.'):
            try:
                partes.append(int(parte))
            except ValueError:
                partes.append(999)
        # Rellenar con ceros para comparación consistente
        while len(partes) < 5:
            partes.append(0)
        return tuple(partes)

    agrupado_final = {k: list(v.values()) for k, v in agrupado.items()}

    # Ordenar entes_info por NUM
    entes_info_ordenados = sorted(entes_info.items(), key=orden_por_num)

    return render_template(
        "resultados.html",
        resultados=agrupado_final,
        entes_info=entes_info_ordenados,
        entes_con_datos=dict(sorted(entes_con_datos.items()))
    )

# -----------------------------------------------------------
# DETALLE POR RFC
# -----------------------------------------------------------
@app.route("/resultados/<rfc>")
def resultados_por_rfc(rfc):
    info = db_manager.obtener_resultados_por_rfc(rfc)
    if not info:
        return render_template("empty.html", mensaje="No hay registros del trabajador.")

    mapa_solvs = db_manager.get_solventaciones_por_rfc(rfc)
    if mapa_solvs and info.get("registros"):
        for reg in info["registros"]:
            ente_clave = db_manager.normalizar_ente_clave(reg.get("ente"))
            if ente_clave in mapa_solvs:
                reg["estado_ente"] = mapa_solvs[ente_clave]["estado"]
                reg["comentario_ente"] = mapa_solvs[ente_clave]["comentario"]

        estados_regs = {reg.get("estado_ente") or info.get("estado") for reg in info["registros"]}
        estados_regs = {e for e in estados_regs if e}
        if len(estados_regs) == 1:
            info["estado"] = estados_regs.pop()
        elif len(estados_regs) > 1:
            info["estado"] = "Mixto"

    return render_template("detalle_rfc.html", rfc=rfc, info=info)


@app.route("/solventacion/<rfc>", methods=["GET", "POST"])
def solventacion_detalle(rfc):
    if not session.get("autenticado"):
        return redirect(url_for("login"))

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

# -----------------------------------------------------------
# UTIL: filtrar solo duplicados reales (con intersección de QNAs)
# -----------------------------------------------------------
def _filtrar_duplicados_reales(resultados):
    """
    Filtra resultados para incluir SOLO registros con duplicidad real:
    - Mismos RFC en múltiples entes
    - Con intersección de QNAs (mismo periodo activo en ambos entes)

    Retorna: lista de resultados filtrados con entes_cruce_real agregado
    """
    resultados_filtrados = []

    for r in resultados:
        # Detectar si existe incompatibilidad real (misma QNA en más de un ente)
        registros_rfc = r.get("registros", [])
        qnas_por_ente = {}

        for reg in registros_rfc:
            ente = reg.get("ente")
            qnas = set(reg.get("qnas", {}).keys())
            qnas_por_ente[ente] = qnas

        # Buscar intersección real
        duplicidad_real = False
        entes_cruce_real = set()

        entes_lista = list(qnas_por_ente.keys())
        for i in range(len(entes_lista)):
            for j in range(i + 1, len(entes_lista)):
                e1, e2 = entes_lista[i], entes_lista[j]
                if qnas_por_ente[e1].intersection(qnas_por_ente[e2]):
                    duplicidad_real = True
                    entes_cruce_real.update([e1, e2])

        # Si NO hay coincidencia real de QNAs, NO incluir
        if not duplicidad_real:
            continue

        # Agregar información de entes con cruce real
        r_filtrado = r.copy()
        r_filtrado["entes_cruce_real"] = list(entes_cruce_real)
        resultados_filtrados.append(r_filtrado)

    return resultados_filtrados

# -----------------------------------------------------------
# UTIL: construir filas exportables
# -----------------------------------------------------------
def _construir_filas_export(resultados):
    agregados = {}
    for r in resultados:
        entes_cruce = r.get("entes_cruce_real") or []
        registros = r.get("registros") or []

        # Pre-calcular QNAs por ente para este RFC
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

            # CALCULAR SOLO LAS QNAS CON INTERSECCIÓN REAL
            qnas_ente_actual = qnas_por_ente.get(ente_origen, set())

            # Comparar con todos los otros entes y agregar solo las QNAs que se intersectan
            for otro_ente, qnas_otro in qnas_por_ente.items():
                if _sanitize_text(otro_ente) != _sanitize_text(ente_origen):
                    # Calcular intersección
                    interseccion = qnas_ente_actual.intersection(qnas_otro)
                    if interseccion:
                        # Agregar ente a la lista de incompatibilidades
                        agregados[key]["_entes_incomp_set"].add(otro_ente)
                        # Agregar solo las QNAs con intersección
                        for qna in interseccion:
                            qnum = qna.replace("QNA", "").strip()
                            if qnum.isdigit():
                                agregados[key]["_qnas_set"].add(int(qnum))

    # === Cargar comentarios reales desde la tabla solventaciones ===
    conn = db_manager._connect()
    cur = conn.cursor()
    cur.execute("SELECT rfc, ente, comentario FROM solventaciones")
    comentarios = cur.fetchall()
    conn.close()

    mapa_coment = {
        (c["rfc"], c["ente"]): c["comentario"]
        for c in comentarios
    }

    # === Construir filas ===
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

        ente_clave = db_manager.normalizar_ente_clave(item["_ente_origen_raw"])
        est_ente = db_manager.get_estado_rfc_ente(item["RFC"], ente_clave)
        est_final = est_ente or item["_estado_base"]

        # Comentario real (si existe en solventaciones)
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

# -----------------------------------------------------------
# EXPORTAR POR ENTE (JSON + Excel)
# -----------------------------------------------------------
@app.route("/exportar_por_ente")
def exportar_por_ente():
    ente_sel = request.args.get("ente", "").strip()
    formato = request.args.get("formato", "").lower()
    if not ente_sel:
        return jsonify({"error": "No se seleccionó un ente"}), 400

    # Obtener cruces y filtrar solo los que tienen duplicidad real (intersección de QNAs)
    resultados = db_manager.obtener_cruces_reales()
    resultados_filtrados = _filtrar_duplicados_reales(resultados)
    filas = _construir_filas_export(resultados_filtrados)

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
    # Obtener cruces y filtrar solo los que tienen duplicidad real (intersección de QNAs)
    resultados = db_manager.obtener_cruces_reales()
    resultados_filtrados = _filtrar_duplicados_reales(resultados)
    filas = _construir_filas_export(resultados_filtrados)

    # Filtrar registros con N/A en Quincenas (sin intersección temporal)
    filas = [f for f in filas if f.get("Quincenas") != "N/A"]

    if not filas:
        return jsonify({"error": "Sin datos para exportar."}), 404

    if formato == "json" or request.is_json:
        return jsonify({"total_registros": len(filas), "datos": filas})

    df = pd.DataFrame(filas)[[
        "RFC", "Nombre", "Puesto", "Fecha Alta", "Fecha Baja", "Total Percepciones",
        "Ente Origen", "Entes Incompatibilidad", "Quincenas", "Estatus", "Solventación"
    ]]
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
    port = int(os.environ.get("PORT", 5006))
    log.info("Levantando Flask en 0.0.0.0:%s (debug=%s)", port, True)
    app.run(host="0.0.0.0", port=port, debug=True)

