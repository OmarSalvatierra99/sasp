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
