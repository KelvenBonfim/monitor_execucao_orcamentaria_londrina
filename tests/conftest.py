
import os
import pytest

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    # Block real network by default
    class _NoNetSession:
        def __init__(self, *a, **k): pass
        def get(self, *a, **k): raise RuntimeError("Network disabled in tests")
        def post(self, *a, **k): raise RuntimeError("Network disabled in tests")

    try:
        import requests
        monkeypatch.setattr(requests, "Session", _NoNetSession, raising=True)
    except Exception:
        pass

@pytest.fixture(autouse=True)
def dummy_env(monkeypatch):
    # Provide a fake DATABASE_URL so scripts that check for it don't exit.
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/dbtest")

@pytest.fixture
def fake_engine(monkeypatch):
    # Patch sqlalchemy.create_engine to return a lightweight fake with .begin()/.connect().
    class _FakeConn:
        def __init__(self):
            self.executed = []
        def execute(self, *a, **k):
            self.executed.append((a, k))
            class _Res:
                def fetchall(self): return []
                def fetchone(self): return None
            return _Res()
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
    class _FakeEngine:
        def begin(self): return _FakeConn()
        def connect(self): return _FakeConn()

    try:
        import sqlalchemy
        monkeypatch.setattr(sqlalchemy, "create_engine", lambda *a, **k: _FakeEngine(), raising=True)
    except Exception:
        pass
    return _FakeEngine()

@pytest.fixture
def fake_psycopg2(monkeypatch):
    # Patch psycopg2.connect to avoid real DB connections.
    class _FakeCursor:
        def __init__(self): self.statements = []
        def execute(self, *args, **kwargs): self.statements.append((args, kwargs))
        def fetchall(self): return []
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
    class _FakeConn:
        def __init__(self): self.cursors = []
        def cursor(self):
            c = _FakeCursor(); self.cursors.append(c); return c
        def commit(self): pass
    try:
        import psycopg2
        monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: _FakeConn(), raising=True)
    except Exception:
        pass
    return _FakeConn()
