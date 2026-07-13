"""
downloader.py
=============
Telechargement des observations SDMX pour un dataset OCDE donne,
filtrees sur le pays cible (Burkina Faso par defaut).

Fonctionnalites :
  - construction de la cle SDMX positionnelle a partir de l'ordre des
    dimensions decouvert dans la DSD (discover.py) ;
  - decoupage en tranches temporelles ("pagination" par periode), utile
    pour les dataflows a tres large historique et pour limiter la taille
    des reponses ;
  - reprise apres interruption : un fichier deja telecharge et non vide
    n'est pas retelecharge ;
  - barre de progression via tqdm.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from tqdm import tqdm

from client import OECDAPIError, OECDClient
from config.config import Config
from discover import DatasetMeta
from logger import get_logger


@dataclass
class DownloadResult:
    dataset_meta: DatasetMeta
    file_path: Path
    start_period: str
    end_period: str
    success: bool
    from_cache: bool = False
    error: Optional[str] = None


class DataDownloader:
    """Telecharge les observations d'un dataflow OCDE au format CSV."""

    def __init__(self, client: OECDClient, config: Config) -> None:
        self.client = client
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.logger = get_logger()

    # ------------------------------------------------------------------
    def build_key(self, dataset_meta: DatasetMeta, country_code: str) -> str:
        """Construit la cle SDMX positionnelle, ex: ".BFA." pour un dataflow
        a 3 dimensions dont REF_AREA est en position 2.

        Si l'ordre des dimensions n'a pas pu etre determine, retourne "all"
        (recupere alors l'ensemble des donnees, filtre applique plus tard
        lors du parsing).
        """
        if not dataset_meta.dimension_order or not dataset_meta.geo_dimension:
            self.logger.warning(
                "Ordre des dimensions inconnu pour %s : utilisation de la cle 'all'.",
                dataset_meta.dataflow_id,
            )
            return "all"

        parts = [
            country_code if dim == dataset_meta.geo_dimension else ""
            for dim in dataset_meta.dimension_order
        ]
        return ".".join(parts)

    def _output_path(
        self, dataset_meta: DatasetMeta, country_code: str, start_period: str, end_period: str
    ) -> Path:
        filename = (
            f"{dataset_meta.agency_id}_{dataset_meta.dataflow_id}_"
            f"{country_code}_{start_period}_{end_period}.csv"
        )
        return self.data_dir / filename

    def _year_chunks(self, start_year: int, end_year: int, chunk_size_years: int) -> List[tuple]:
        chunks = []
        current = start_year
        while current <= end_year:
            chunk_end = min(current + chunk_size_years - 1, end_year)
            chunks.append((current, chunk_end))
            current = chunk_end + 1
        return chunks

    # ------------------------------------------------------------------
    def download(
        self,
        dataset_meta: DatasetMeta,
        country_code: Optional[str] = None,
        start_year: int = 1960,
        end_year: Optional[int] = None,
        chunk_size_years: int = 20,
    ) -> List[DownloadResult]:
        """Telecharge les observations d'un dataflow pour le pays cible.

        Le telechargement est decoupe en tranches temporelles afin de
        limiter la taille des reponses et de permettre une reprise fine
        apres interruption (chaque tranche = un fichier independant).
        """
        country_code = country_code or self.config.target_country_code
        end_year = end_year or datetime.now().year

        key = self.build_key(dataset_meta, country_code)
        chunks = self._year_chunks(start_year, end_year, chunk_size_years)

        results: List[DownloadResult] = []

        for chunk_start, chunk_end in tqdm(
            chunks,
            desc=f"Telechargement {dataset_meta.dataflow_id}",
            unit="tranche",
            leave=False,
        ):
            start_period = str(chunk_start)
            end_period = str(chunk_end)
            output_path = self._output_path(dataset_meta, country_code, start_period, end_period)

            if output_path.exists() and output_path.stat().st_size > 0:
                self.logger.info(
                    "Fichier deja present, reprise (skip) : %s", output_path.name
                )
                results.append(
                    DownloadResult(
                        dataset_meta=dataset_meta,
                        file_path=output_path,
                        start_period=start_period,
                        end_period=end_period,
                        success=True,
                        from_cache=True,
                    )
                )
                continue

            path = f"/data/{dataset_meta.agency_id},{dataset_meta.dataflow_id},{dataset_meta.version}/{key}"
            params = {
                "startPeriod": start_period,
                "endPeriod": end_period,
                "format": "csvfilewithlabels",
            }
            headers = {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"}

            try:
                csv_text = self.client.get(path, params=params, headers=headers)

                if not csv_text or not csv_text.strip():
                    self.logger.info(
                        "Aucune observation pour %s [%s-%s].",
                        dataset_meta.dataflow_id,
                        start_period,
                        end_period,
                    )
                    results.append(
                        DownloadResult(
                            dataset_meta=dataset_meta,
                            file_path=output_path,
                            start_period=start_period,
                            end_period=end_period,
                            success=True,
                            error="no_data",
                        )
                    )
                    continue

                # Ecriture atomique : fichier temporaire puis renommage,
                # pour eviter des fichiers partiels en cas d'interruption.
                tmp_path = output_path.with_suffix(".tmp")
                tmp_path.write_text(csv_text, encoding="utf-8")
                tmp_path.rename(output_path)

                self.logger.info("Telecharge : %s", output_path.name)
                results.append(
                    DownloadResult(
                        dataset_meta=dataset_meta,
                        file_path=output_path,
                        start_period=start_period,
                        end_period=end_period,
                        success=True,
                    )
                )

            except OECDAPIError as exc:
                self.logger.error(
                    "Echec du telechargement pour %s [%s-%s] : %s",
                    dataset_meta.dataflow_id,
                    start_period,
                    end_period,
                    exc,
                )
                results.append(
                    DownloadResult(
                        dataset_meta=dataset_meta,
                        file_path=output_path,
                        start_period=start_period,
                        end_period=end_period,
                        success=False,
                        error=str(exc),
                    )
                )

        return results
