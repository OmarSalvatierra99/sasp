# Contexto del proyecto — SASP

## Descripción
SASP (Sistema de Auditoría de Servicios Personales) / SCIL — herramienta crítica del OFS Tlaxcala para validar nóminas quincenales por RFC y ente. Detecta incompatibilidades, genera solventaciones y exporta reportes.

## Usuarios del sistema
- **odilia**, **luis**, **felipe** — superusuarios (acceso a todos los entes)
- Otros usuarios — acceso restringido por ente(s) asignados en DB

## Base de datos
`scil.db` — SQLite local. Contiene: usuarios, quincenas, registros, entes, solventaciones.  
Backups manuales en `.backup.YYYYMMDD_HHMMSS`.

## Estado de migración
- Migrado en wave 5 (2026-04-13)
- `desploy/` → `deploy/` corregido
- `log/` → `logs/` corregido
- `/api/health` añadido (exento de auth)
- `SECRET_KEY` sin default hardcodeado
