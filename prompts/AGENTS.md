# AGENTS.md — SASP

## Identidad del proyecto
**Slug:** sasp | **Puerto:** 5006 | **Stack:** Flask 3 + SQLite (RBAC)  
**Servicio:** `portfolio-sasp` | **EnvironmentFile:** `/etc/default/portfolio-sasp`

## Descripción
Sistema de Auditoría de Servicios Personales (SASP/SCIL) del OFS Tlaxcala. Carga archivos Excel de nómina, valida datos de quincenas por RFC/ente y genera reportes de solventación.

## Auth — RBAC
- Login vía DB: `db_manager.get_usuario(usuario, clave)` en `scripts/utils.py`
- Superusuarios (acceso total): `odilia`, `luis`, `felipe`
- `session["entes"]` = lista de entes autorizados o `["TODOS"]`
- Middleware `verificar_autenticacion()` protege todas las rutas excepto `login`, `static`, `health_check`

## Rutas clave
- `GET/POST /` — login
- `GET /logout` — cierre de sesión (redirige a SIFEET-2025 interno)
- `GET /dashboard` — panel principal
- `POST /upload_laboral` — carga de nómina Excel
- `GET /api/health` — healthcheck (sin auth)

## Consideraciones críticas
- `SECRET_KEY` sin default en código — va en `/etc/default/portfolio-sasp`
- `SCIL_DB` — ruta absoluta a scil.db; los backups (`.backup.*`) no van a git
- `logs/` (corregido de `log/`)
- `deploy/` (corregido de `desploy/`)

## Healthcheck
`GET /api/health` → `{"status": "ok", "service": "sasp"}`
