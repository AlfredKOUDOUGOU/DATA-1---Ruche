"""
config.py
=========
Chargement et validation de la configuration du connecteur OCDE.

Toutes les valeurs sensibles (identifiants base de donnees, URLs, etc.)
sont chargees depuis un fichier ".env" via python-dotenv. Aucune valeur
sensible n'est codee en dur dans le code source.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Racine du projet (dossier parent de config/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Charge le fichier .env s'il existe (ne fait rien sinon, permet
# egalement l'injection de variables d'environnement classiques,
# utile par exemple sous Airflow/Docker).
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")


def _get_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _get_optional_int(name: str) -> Optional[int]:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return None
    try:
        return int(val)
    except ValueError:
        return None


@dataclass(frozen=True)
class Config:
    """Configuration centrale du connecteur, immuable une fois chargee."""

    # --- PostgreSQL ---
    db_host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    db_port: int = field(default_factory=lambda: _get_int("DB_PORT", 5432))
    db_name: str = field(default_factory=lambda: os.getenv("DB_NAME", "bf_pulse"))
    db_user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    db_password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))
    db_schema: str = field(default_factory=lambda: os.getenv("DB_SCHEMA", "public"))

    # --- API OCDE ---
    oecd_api_base_url: str = field(
        default_factory=lambda: os.getenv(
            "OECD_API_BASE_URL", "https://sdmx.oecd.org/public/rest"
        )
    )
    oecd_agency_id: str = field(default_factory=lambda: os.getenv("OECD_AGENCY_ID", "OECD"))
    oecd_api_timeout: int = field(default_factory=lambda: _get_int("OECD_API_TIMEOUT", 60))
    target_country_code: str = field(
        default_factory=lambda: os.getenv("TARGET_COUNTRY_CODE", "BFA")
    )

    # --- Limitation des requetes / retry ---
    oecd_max_requests_per_hour: int = field(
        default_factory=lambda: _get_int("OECD_MAX_REQUESTS_PER_HOUR", 60)
    )
    oecd_max_retries: int = field(default_factory=lambda: _get_int("OECD_MAX_RETRIES", 3))
    oecd_retry_backoff: int = field(default_factory=lambda: _get_int("OECD_RETRY_BACKOFF", 5))

    # --- Chemins locaux ---
    data_dir: Path = field(
        default_factory=lambda: (
            PROJECT_ROOT / os.getenv("DATA_DIR", "./data")
        ).resolve()
    )
    logs_dir: Path = field(
        default_factory=lambda: (
            PROJECT_ROOT / os.getenv("LOGS_DIR", "./logs")
        ).resolve()
    )

    # --- Journalisation ---
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # --- Portee de la decouverte ---
    max_dataflows_to_scan: Optional[int] = field(
        default_factory=lambda: _get_optional_int("MAX_DATAFLOWS_TO_SCAN")
    )

    def __post_init__(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    @property
    def database_url(self) -> str:
        """URL de connexion SQLAlchemy pour PostgreSQL (psycopg2)."""
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def validate(self) -> None:
        """Verifie que les parametres essentiels sont bien renseignes.

        Leve une ValueError explicite si une valeur critique manque,
        plutot que de laisser echouer plus tard avec une erreur obscure.
        """
        missing = []
        if not self.db_name:
            missing.append("DB_NAME")
        if not self.db_user:
            missing.append("DB_USER")
        if not self.oecd_api_base_url:
            missing.append("OECD_API_BASE_URL")
        if not self.target_country_code:
            missing.append("TARGET_COUNTRY_CODE")

        if missing:
            raise ValueError(
                "Configuration incomplete. Variables manquantes ou vides : "
                + ", ".join(missing)
                + ". Verifiez votre fichier .env (voir .env.example)."
            )


def load_config() -> Config:
    """Point d'entree unique pour charger et valider la configuration."""
    config = Config()
    config.validate()
    return config
