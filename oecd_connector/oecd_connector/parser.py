"""
parser.py
=========
Transformation des fichiers bruts telecharges (CSV SDMX) en DataFrames
pandas normalises, prets a etre charges en base PostgreSQL.

Le format CSV renvoye par l'API OCDE varie legerement selon les
dataflows (noms de colonnes differents pour la zone geographique,
l'indicateur, etc.). Ce module applique une detection heuristique des
colonnes pertinentes plutot que de supposer un schema fixe.

Schema de sortie normalise (un enregistrement par observation) :
    dataset_code, indicator_code, indicator_name, country_code,
    year, value, unit, frequency
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from discover import DatasetMeta
from logger import get_logger

# Colonnes candidates (ordre de priorite) pour chaque champ normalise.
TIME_COLUMNS = ["TIME_PERIOD", "TIME", "Time period", "TIME_PERIOD_LABEL"]
VALUE_COLUMNS = ["OBS_VALUE", "VALUE", "Observation value", "OBS_VALUE_LABEL"]
GEO_COLUMNS = ["REF_AREA", "LOCATION", "COUNTRY", "COU", "GEO", "Reference area"]
INDICATOR_CODE_COLUMNS = ["INDICATOR", "MEASURE", "SUBJECT", "SERIES", "INDICATOR_CODE"]
INDICATOR_NAME_COLUMNS = [
    "Indicator",
    "Measure",
    "Subject",
    "Series",
    "Indicator name",
]
UNIT_COLUMNS = ["UNIT_MEASURE", "UNIT", "Unit of measure", "UNIT_MEASURE_LABEL"]
FREQ_COLUMNS = ["FREQ", "FREQUENCY", "Frequency"]


class DataParsingError(Exception):
    """Erreur levee lorsqu'un fichier ne peut pas etre normalise."""


class DataParser:
    """Transforme les fichiers CSV bruts OCDE en DataFrames normalises."""

    def __init__(self) -> None:
        self.logger = get_logger()

    # ------------------------------------------------------------------
    def _find_column(self, columns: List[str], candidates: List[str]) -> Optional[str]:
        upper_map = {c.upper(): c for c in columns}
        for candidate in candidates:
            if candidate.upper() in upper_map:
                return upper_map[candidate.upper()]
        return None

    def _extract_year(self, value: object) -> Optional[int]:
        """Extrait une annee (int) a partir d'une periode SDMX (ex: '1998',
        '1998-Q1', '1998-01', '1998-01-01')."""
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
        """Charge et normalise un fichier CSV SDMX.

        Retourne un DataFrame vide (avec les bonnes colonnes) si le fichier
        est vide ou ne contient aucune observation valide, plutot que de
        lever une exception : cela permet au pipeline de continuer avec les
        autres fichiers.
        """
        empty_schema = pd.DataFrame(
            columns=[
                "dataset_code",
                "indicator_code",
                "indicator_name",
                "country_code",
                "year",
                "value",
                "unit",
                "frequency",
            ]
        )

        if not file_path.exists() or file_path.stat().st_size == 0:
            self.logger.debug("Fichier vide ou introuvable, ignore : %s", file_path)
            return empty_schema

        try:
            raw_df = pd.read_csv(file_path, dtype=str, low_memory=False)
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
            self.logger.warning("Impossible de lire %s : %s", file_path, exc)
            return empty_schema

        if raw_df.empty:
            return empty_schema

        columns = list(raw_df.columns)

        time_col = self._find_column(columns, TIME_COLUMNS)
        value_col = self._find_column(columns, VALUE_COLUMNS)
        geo_col = self._find_column(columns, GEO_COLUMNS)
        indicator_code_col = self._find_column(columns, INDICATOR_CODE_COLUMNS)
        indicator_name_col = self._find_column(columns, INDICATOR_NAME_COLUMNS)
        unit_col = self._find_column(columns, UNIT_COLUMNS)
        freq_col = self._find_column(columns, FREQ_COLUMNS)

        if not time_col or not value_col:
            self.logger.warning(
                "Colonnes essentielles (periode/valeur) introuvables dans %s. Colonnes : %s",
                file_path.name,
                columns,
            )
            return empty_schema

        normalized = pd.DataFrame()
        normalized["dataset_code"] = pd.Series([dataset_meta.dataflow_id] * len(raw_df))
        normalized["indicator_code"] = (
            raw_df[indicator_code_col] if indicator_code_col else dataset_meta.dataflow_id
        )
        normalized["indicator_name"] = (
            raw_df[indicator_name_col]
            if indicator_name_col
            else (raw_df[indicator_code_col] if indicator_code_col else dataset_meta.name)
        )
        normalized["country_code"] = raw_df[geo_col] if geo_col else country_code
        normalized["year"] = raw_df[time_col].apply(self._extract_year)
        normalized["value"] = pd.to_numeric(raw_df[value_col], errors="coerce")
        normalized["unit"] = raw_df[unit_col] if unit_col else None
        normalized["frequency"] = raw_df[freq_col] if freq_col else None

        # Filtrer strictement sur le pays cible (au cas ou la cle SDMX
        # utilisee lors du telechargement etait "all").
        if geo_col:
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
                "%s : %d lignes ecartees (valeurs/periodes manquantes ou doublons).",
                file_path.name,
                before - after,
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
        """Parse plusieurs fichiers (ex: tranches temporelles d'un meme
        dataflow) et les concatene en un seul DataFrame normalise."""
        frames = [self.parse_file(fp, dataset_meta, country_code) for fp in file_paths]
        frames = [f for f in frames if not f.empty]

        if not frames:
            return pd.DataFrame(
                columns=[
                    "dataset_code",
                    "indicator_code",
                    "indicator_name",
                    "country_code",
                    "year",
                    "value",
                    "unit",
                    "frequency",
                ]
            )

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["dataset_code", "indicator_code", "country_code", "year"]
        )
        return combined
