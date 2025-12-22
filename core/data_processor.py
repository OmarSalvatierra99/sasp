# ===========================================================
# core/data_processor.py — SCIL / SASP 2025
# Procesamiento de archivos laborales y cruces quincenales
# ===========================================================

import pandas as pd
import re
from datetime import datetime, date
from collections import defaultdict
from core.database import DatabaseManager


class DataProcessor:
    """
    Procesador de archivos Excel laborales.

    Funcionalidad principal:
    - Lee archivos Excel con datos de empleados por ente público
    - Detecta empleados activos en múltiples entes en la misma quincena
    - Genera registros de cruces (duplicaciones) y empleados únicos
    """

    def __init__(self):
        self.db = DatabaseManager("scil.db")
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
        val = str(etiqueta).strip().upper()
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


