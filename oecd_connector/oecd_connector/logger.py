"""
logger.py
=========
Configuration centralisee de la journalisation (logging natif Python).

Fournit un logger unique, partage par tous les modules du connecteur,
qui ecrit simultanement :
  - sur la console (stdout), format court ;
  - dans un fichier journalier sous logs/, format detaille.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path


_LOGGER_NAME = "oecd_connector"
_configured = False


def setup_logger(logs_dir: Path, level: str = "INFO") -> logging.Logger:
    """Configure et retourne le logger principal du connecteur.

    Idempotent : les appels suivants retournent le meme logger sans
    dupliquer les handlers.
    """
    global _configured

    logger = logging.getLogger(_LOGGER_NAME)

    if _configured:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    logs_dir = Path(logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"oecd_connector_{datetime.now():%Y%m%d}.log"

    file_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(module)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    _configured = True
    logger.debug("Logger initialise. Fichier de log : %s", log_file)
    return logger


def get_logger() -> logging.Logger:
    """Recupere le logger deja configure (a utiliser dans les autres modules)."""
    return logging.getLogger(_LOGGER_NAME)
