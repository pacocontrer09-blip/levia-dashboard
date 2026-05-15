"""
data_dir.py — Fuente única de verdad para el directorio de datos persistentes.

Lógica:
  1. Si DATA_DIR env var está seteada (ej. DATA_DIR=/data en Railway), lo intenta.
  2. Hace un write-test real: si falla (permisos, montaje tardío, read-only), loguea y sigue.
  3. Si el env var no está o el write-test falló, usa BASE_DIR/cache (siempre escribible).

NUNCA hace sys.exit() — siempre hay fallback.
El log muestra qué path se usa → visible en Railway logs.

Uso en Railway:
  Agregar en Railway Variables: DATA_DIR=/data
  El volumen web-volume ya está montado en /data.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent


def resolve_data_dir() -> Path:
    candidate = os.getenv("DATA_DIR")
    if candidate:
        p = Path(candidate)
        try:
            p.mkdir(parents=True, exist_ok=True)
            # Write-test real: detecta permisos, montaje tardío, read-only
            test = p / ".write_test"
            test.write_text("ok")
            test.unlink()
            print(f"[data_dir] Usando volumen persistente: {p}")
            return p
        except Exception as e:
            print(f"[data_dir] WARNING: DATA_DIR={candidate} no escribible ({e}). Fallback a cache/")

    fallback = BASE_DIR / "cache"
    fallback.mkdir(parents=True, exist_ok=True)
    print(f"[data_dir] Usando cache local (efímero): {fallback}")
    return fallback


DATA_DIR = resolve_data_dir()
