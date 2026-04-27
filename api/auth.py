"""
auth.py
───────
Validación de API keys para la OpenSky API.

Gestión de keys:
  - Las keys se guardan en keys.json (ver ejemplo abajo)
  - Para añadir un compañero: agregar entrada en keys.json y reiniciar la API
  - Las keys se recargan en cada request (sin reinicio si usas reload_keys())

Formato de keys.json:
  {
    "juan":   "sk-juan-abc123xyz",
    "maria":  "sk-maria-def456uvw",
    "carlos": "sk-carlos-ghi789rst"
  }

Generar una key segura desde terminal:
  python -c "import secrets; print('sk-' + secrets.token_urlsafe(24))"
"""

import json
import logging
from pathlib import Path

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

logger = logging.getLogger(__name__)

KEYS_FILE = Path(__file__).parent / "keys.json"
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _load_keys() -> dict[str, str]:
    """Carga el mapa nombre→key desde keys.json."""
    if not KEYS_FILE.exists():
        logger.warning(f"keys.json no encontrado en {KEYS_FILE}. Creando vacío.")
        KEYS_FILE.write_text("{}")
        return {}
    try:
        data = json.loads(KEYS_FILE.read_text())
        if not isinstance(data, dict):
            raise ValueError("keys.json debe ser un objeto JSON {nombre: key}")
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Error leyendo keys.json: {e}")
        return {}


# Mapa invertido key→nombre para lookup O(1)
def _build_reverse(keys: dict[str, str]) -> dict[str, str]:
    return {v: k for k, v in keys.items()}


def verify_key(api_key: str = Security(API_KEY_HEADER)) -> str:
    """
    Dependencia FastAPI. Valida la API key del header X-API-Key.
    Retorna el nombre del usuario si es válida.
    Lanza HTTP 401 si falta el header, HTTP 403 si la key es inválida.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Header X-API-Key requerido.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    keys = _load_keys()
    reverse = _build_reverse(keys)

    if api_key not in reverse:
        logger.warning(f"Intento de acceso con key inválida: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API key inválida o expirada.",
        )

    user = reverse[api_key]
    logger.info(f"Acceso autorizado: {user}")
    return user


def list_users() -> list[str]:
    """Retorna la lista de usuarios registrados (sin exponer sus keys)."""
    return list(_load_keys().keys())
