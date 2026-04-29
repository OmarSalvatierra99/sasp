# Repository Guidelines

## Project Structure & Module Organization
`app.py` is the main Flask entrypoint and currently contains most routes and export logic. Shared processing and database helpers live in `scripts/utils.py`, with one-off maintenance scripts in `scripts/`. HTML templates are in `templates/`, frontend assets in `static/`, deployment files in `deploy/`, and pytest coverage in `tests/`. The default SQLite database is `scil.db`; treat backup files and `SASP.zip` as data artifacts, not code.

## Build, Test, and Development Commands
Create or activate the local virtualenv before working. Typical commands:

```bash
source venv/bin/activate
pip install -r requirements.txt
python app.py
pytest
```

`python app.py` starts the Flask dev server using `config.PORT` and environment variables such as `SECRET_KEY` and `SCIL_DB`. `pytest` runs the route smoke tests in `tests/test_app.py`. For production reference, `deploy/systemd/portfolio-sasp.service` runs `gunicorn app:app`.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, `snake_case` for functions and variables, and short route helpers prefixed with `_` when they are private to `app.py`. Keep Flask route names descriptive, and match the current Spanish domain vocabulary used in templates and JSON responses. Use concise comments only where the control flow is not obvious. No formatter or linter is configured in this repo, so keep changes consistent with surrounding code.

## Testing Guidelines
Tests use `pytest` with Flask’s `test_client()` and in-memory configuration via `SCIL_DB=:memory:`. Add new tests under `tests/` and name them `test_*.py`; individual cases should be named `test_<behavior>()`. Cover both HTML routes and JSON endpoints when modifying request handling, auth guards, or exports. Run `pytest` before opening a PR.

## Commit & Pull Request Guidelines
Recent history mixes informal commits with conventional-style messages, but `fix: ...` is the clearest pattern and should be preferred. Keep commits focused and imperative, for example `fix: valida archivos vacios en carga masiva`. PRs should include a short summary, impacted routes or templates, manual test notes, and screenshots when UI templates or CSS change.

## Security & Configuration Tips
Do not commit real secrets or production database paths. Start from `deploy/env/sasp.env.example` or `.env.example`, set a strong `SECRET_KEY`, and keep `SCIL_DB` pointed at a local copy during development.
