# CLAUDE.md — SASP

## Contexto de trabajo
Sistema crítico de auditoría de nómina. RBAC con DB SQLite. No modificar lógica de auth sin instrucción.

## Reglas
- `SECRET_KEY` sin default — va en `/etc/default/portfolio-sasp`
- Logs en `logs/` — no `log/`
- No modificar `db_manager.get_usuario()` sin entender el esquema completo
- Los archivos `.backup.*` y `scil.db` no van a git

## Testing
```bash
venv/bin/pytest tests/ -v
```

## Deploy
```bash
sudo systemctl restart portfolio-sasp
curl http://localhost:5006/api/health
```
