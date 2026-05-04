from fastapi import Header, HTTPException, status
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9041"))
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "opensky")


def get_session():
    auth_provider = PlainTextAuthProvider(
        username=CASSANDRA_USER,
        password=CASSANDRA_PASSWORD
    )

    cluster = Cluster(
        contact_points=[CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=auth_provider
    )

    return cluster.connect(CASSANDRA_KEYSPACE)


def verify_key(x_api_key: str = Header(None, alias="X-API-Key")):
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header"
        )

    session = get_session()

    row = session.execute(
        """
        SELECT api_key, username, role, active
        FROM api_users
        WHERE api_key = %s
        """,
        (x_api_key,)
    ).one()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )

    if not row.active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive API key"
        )

    return {
        "api_key": row.api_key,
        "username": row.username,
        "role": row.role,
        "active": row.active
    }


def require_roles(allowed_roles: list[str]):
    def role_checker(x_api_key: str = Header(None, alias="X-API-Key")):
        verified_user = verify_key(x_api_key)

        if verified_user["role"] not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{verified_user['role']}' is not allowed"
            )

        return verified_user

    return role_checker


def list_users():
    session = get_session()

    rows = session.execute(
        """
        SELECT username, role, active
        FROM api_users
        """
    )

    return [
        {
            "username": row.username,
            "role": row.role,
            "active": row.active
        }
        for row in rows
    ]
