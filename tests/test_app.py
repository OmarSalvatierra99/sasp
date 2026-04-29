"""Tests para SASP (05-sasp)."""
import os
import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing-only")
os.environ.setdefault("SCIL_DB", ":memory:")
os.environ.setdefault("FLASK_ENV", "testing")


@pytest.fixture
def client():
    from app import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_health_standard_route(client):
    """GET /api/health debe retornar 200 sin autenticación."""
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.get_json()
    assert data["status"] == "ok"
    assert data["service"] == "sasp"


def test_health_compat_route(client):
    """GET /health también debe retornar 200."""
    r = client.get("/health")
    assert r.status_code == 200


def test_login_page_loads(client):
    """GET / debe mostrar el formulario de login."""
    r = client.get("/")
    assert r.status_code == 200
    assert "Usuarios autorizados" in r.get_data(as_text=True)


def test_get_login_users_removes_duplicate_display_names(monkeypatch):
    """El login debe ocultar usuarios repetidos si comparten el mismo nombre visible."""
    from app import _get_login_users

    monkeypatch.setattr(
        "app.ordered_users",
        lambda *_args, **_kwargs: [
            {"username": "luis", "display_name": "C.P Luis Felipe Camilo Fuentes"},
            {"username": "felipe", "display_name": "C.P Luis Felipe Camilo Fuentes"},
            {"username": "odilia", "display_name": "C.P. Odilia Cuamatzi Bautista"},
        ],
    )

    usuarios = _get_login_users()

    assert [usuario["username"] for usuario in usuarios] == ["luis", "odilia"]


def test_dashboard_redirects_when_not_logged_in(client):
    """GET /dashboard sin sesión debe redirigir al login."""
    r = client.get("/dashboard")
    assert r.status_code == 302
    assert "/" in r.headers["Location"]


def test_logout_clears_session(client):
    """GET /logout debe limpiar la sesión."""
    with client.session_transaction() as sess:
        sess["autenticado"] = True
        sess["usuario"] = "testuser"
    r = client.get("/logout")
    assert r.status_code in (200, 302)


def test_schedule_overlap_helpers():
    from datetime import date, time
    from app import hay_traslape_de_horas, hay_traslape_de_fechas, calcular_minutos_traslapados

    assert hay_traslape_de_horas(time(8, 0), time(12, 0), time(11, 0), time(13, 0)) is True
    assert hay_traslape_de_horas(time(8, 0), time(10, 0), time(10, 0), time(12, 0)) is False
    assert hay_traslape_de_fechas(date(2026, 1, 1), None, date(2026, 1, 15), date(2026, 1, 31)) is True
    assert calcular_minutos_traslapados(time(8, 0), time(12, 0), time(10, 0), time(11, 30)) == 90


def test_detectar_cruces_de_horarios_dedupes_and_classifies():
    from app import detectar_cruces_de_horarios

    horarios = [
        {
            "id": 1,
            "rfc": "ABC123",
            "nombre": "PERSONA DEMO",
            "ente": "SEPE",
            "dia_semana": 0,
            "hora_inicio": "08:00",
            "hora_fin": "12:30",
            "fecha_inicio_vigencia": "2026-01-01",
            "fecha_fin_vigencia": "",
            "estatus": "activo",
        },
        {
            "id": 2,
            "rfc": "ABC123",
            "nombre": "PERSONA DEMO",
            "ente": "USET",
            "dia_semana": 0,
            "hora_inicio": "10:00",
            "hora_fin": "14:00",
            "fecha_inicio_vigencia": "2026-01-01",
            "fecha_fin_vigencia": "",
            "estatus": "activo",
        },
        {
            "id": 3,
            "rfc": "ABC123",
            "nombre": "PERSONA DEMO",
            "ente": "USET",
            "dia_semana": 2,
            "hora_inicio": "10:00",
            "hora_fin": "14:00",
            "fecha_inicio_vigencia": "2026-01-01",
            "fecha_fin_vigencia": "",
            "estatus": "inactivo",
        },
    ]

    resultado = detectar_cruces_de_horarios(horarios)

    assert len(resultado["conflictos"]) == 1
    assert resultado["conflictos"][0]["minutos_traslape"] == 150
    assert resultado["conflictos"][0]["severidad"] == "alta"
    assert len(resultado["personas"]) == 1


def test_new_modules_redirect_when_not_logged_in(client):
    for path in ["/horarios", "/cruce-entes", "/observaciones", "/pdp", "/reportes-operativos"]:
        r = client.get(path)
        assert r.status_code == 302
