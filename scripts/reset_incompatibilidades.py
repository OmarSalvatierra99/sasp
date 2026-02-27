#!/usr/bin/env python3
"""
Reset seguro de datos de incompatibilidades para SCIL/SASP.

No toca catalogos (entes/municipios) ni usuarios.
Limpia solo:
- registros_laborales
- prevalidaciones
- prevalidaciones_historial
- solventaciones

Tambien regresa workflow_estado.validacion_resultados a "borrador".
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


TABLAS_INCOMPATIBILIDAD = (
    "registros_laborales",
    "prevalidaciones",
    "prevalidaciones_historial",
    "solventaciones",
)


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _count_rows(cur: sqlite3.Cursor, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _backup_db(db_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.backup.{timestamp}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def run_reset(db_path: Path, dry_run: bool = False, skip_backup: bool = False) -> int:
    if not db_path.exists():
        print(f"ERROR: No existe la base de datos: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        faltantes = [t for t in TABLAS_INCOMPATIBILIDAD if not _table_exists(cur, t)]
        if faltantes:
            print("ERROR: Faltan tablas requeridas:")
            for t in faltantes:
                print(f"  - {t}")
            return 1

        conteo_antes = {t: _count_rows(cur, t) for t in TABLAS_INCOMPATIBILIDAD}
        print("Conteo actual:")
        for t in TABLAS_INCOMPATIBILIDAD:
            print(f"  {t}: {conteo_antes[t]}")

        if dry_run:
            print("DRY RUN: no se realizaron cambios.")
            return 0

        if not skip_backup:
            backup_path = _backup_db(db_path)
            print(f"Respaldo creado: {backup_path}")

        conn.execute("BEGIN")
        for table in TABLAS_INCOMPATIBILIDAD:
            cur.execute(f"DELETE FROM {table}")

        if _table_exists(cur, "workflow_estado"):
            cur.execute(
                """
                UPDATE workflow_estado
                SET valor='borrador',
                    actualizado=CURRENT_TIMESTAMP,
                    actualizado_por='SISTEMA'
                WHERE clave='validacion_resultados'
                """
            )
        conn.commit()

        conteo_despues = {t: _count_rows(cur, t) for t in TABLAS_INCOMPATIBILIDAD}
        print("Limpieza completada:")
        for t in TABLAS_INCOMPATIBILIDAD:
            eliminados = conteo_antes[t] - conteo_despues[t]
            print(f"  {t}: {conteo_antes[t]} -> {conteo_despues[t]} (eliminados: {eliminados})")
        print("workflow_estado.validacion_resultados -> borrador")
        return 0

    except Exception as exc:  # pragma: no cover
        conn.rollback()
        print(f"ERROR al reiniciar incompatibilidades: {exc}")
        return 1
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Limpia solo los datos de incompatibilidades sin tocar catalogos ni usuarios."
    )
    parser.add_argument(
        "--db",
        default="scil.db",
        help="Ruta a la base SQLite (default: scil.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra conteos y valida tablas, sin modificar datos.",
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="No crear respaldo automatico antes de limpiar.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(
        run_reset(
            db_path=Path(args.db).resolve(),
            dry_run=args.dry_run,
            skip_backup=args.skip_backup,
        )
    )
