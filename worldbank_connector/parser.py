"""
parser.py
=========
Transformation des fichiers JSON bruts telecharges (observations
Banque mondiale) en DataFrames pandas normalises, prets a etre charges
en base PostgreSQL.

Une observation brute de l'API Banque mondiale ressemble a :
    {
        "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP (current US$)"},
        "country": {"id": "BF", "value": "Burkina Faso"},
        "countryiso3code": "BFA",
        "date": "2022",
        "value": 19736930000.6485,
        "unit": "",
        "obs_status": "",
        "decimal": 1
    }

Schema de sortie normalise (un enregistrement par observation) :
    dataset_code, indicator_code, indicator_name, country_code,
    year, value, unit, frequency
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from discover import DatasetMeta
from logger import get_logger

OUTPUT_COLUMNS = [
    "dataset_code",
    "indicator_code",
    "indicator_name",
    "country_code",
    "year",
    "value",
    "unit",
    "frequency",
]


class DataParsingError(Exception):
    """Erreur levee lorsqu'un fichier ne peut pas etre normalise."""


class DataParser:
    """Transforme les fichiers JSON bruts Banque mondiale en DataFrames normalises."""

    def __init__(self) -> None:
        self.logger = get_logger()

    # ------------------------------------------------------------------
    def _extract_year(self, value: object) -> Optional[int]:
        """Extrait une annee (int) a partir du champ `date` de l'API
        (generalement "1998" pour des donnees annuelles, mais peut aussi
        etre "1998Q1" ou "1998M01" pour certains indicateurs infra-annuels)."""
        if value is None:
            return None
        text = str(value).strip()
        if len(text) < 4:
            return None
        digits = text[:4]
        return int(digits) if digits.isdigit() else None

    # ------------------------------------------------------------------
    def parse_file(
        self,
        file_path: Path,
        dataset_meta: DatasetMeta,
        country_code: str,
    ) -> pd.DataFrame:
        """Charge et normalise un fichier JSON d'observations Banque mondiale.

        Retourne un DataFrame vide (avec les bonnes colonnes) si le fichier
        est vide, illisible, ou ne contient aucune observation valide,
        plutot que de lever une exception : cela permet au pipeline de
        continuer avec les autres fichiers.
        """
        empty_schema = pd.DataFrame(columns=OUTPUT_COLUMNS)

        if not file_path.exists() or file_path.stat().st_size == 0:
            self.logger.debug("Fichier vide ou introuvable, ignore : %s", file_path)
            return empty_schema

        try:
            raw_records = json.loads(file_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            self.logger.warning("Impossible de lire %s : %s", file_path, exc)
            return empty_schema

        if not raw_records:
            return empty_schema

        rows = []
        for rec in raw_records:
            indicator = rec.get("indicator") or {}
            indicator_code = indicator.get("id") or dataset_meta.indicator_id
            indicator_name = indicator.get("value") or dataset_meta.name

            geo_code = rec.get("countryiso3code") or (rec.get("country") or {}).get("id") or country_code
            year = self._extract_year(rec.get("date"))
            raw_value = rec.get("value")

            rows.append(
                {
                    "dataset_code": dataset_meta.indicator_id,
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_name,
                    "country_code": geo_code,
                    "year": year,
                    "value": raw_value,
                    "unit": (rec.get("unit") or "").strip() or dataset_meta.unit,
                    "frequency": "A",
                }
            )

        normalized = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        normalized["value"] = pd.to_numeric(normalized["value"], errors="coerce")

        # Filtrer strictement sur le pays cible.
        normalized = normalized[
            normalized["country_code"].astype(str).str.upper() == country_code.upper()
        ]

        before = len(normalized)
        normalized = normalized.dropna(subset=["year", "value"])
        normalized = normalized.drop_duplicates(
            subset=["dataset_code", "indicator_code", "country_code", "year"]
        )
        after = len(normalized)

        if before != after:
            self.logger.debug(
                "%s : %d lignes ecartees (valeurs/annees manquantes ou doublons).",
                file_path.name, before - after,
            )

        normalized["year"] = normalized["year"].astype(int)
        return normalized.reset_index(drop=True)

    # ------------------------------------------------------------------
    def parse_many(
        self,
        file_paths: Iterable[Path],
        dataset_meta: DatasetMeta,
        country_code: str,
    ) -> pd.DataFrame:
        """Parse plusieurs fichiers et les concatene en un seul DataFrame normalise."""
        frames = [self.parse_file(fp, dataset_meta, country_code) for fp in file_paths]
        frames = [f for f in frames if not f.empty]

        if not frames:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["dataset_code", "indicator_code", "country_code", "year"]
        )
        return combined
