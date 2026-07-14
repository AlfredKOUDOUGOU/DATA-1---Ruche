"""
downloader.py
=============
Telechargement des observations Banque mondiale pour un indicateur
donne, filtrees sur le pays cible (Burkina Faso par defaut).

Contrairement au connecteur OCDE, il n'est generalement pas necessaire
de decouper manuellement en tranches temporelles : l'API Banque mondiale
accepte un parametre `date=1960:2025` couvrant plusieurs decennies en un
seul appel, et gere elle-meme la pagination (`page`/`pages`) si le
nombre d'observations depasse `per_page`.

Fonctionnalites :
  - pagination automatique (suit metadata.pages) ;
  - reprise apres interruption : un fichier deja telecharge et non vide
    n'est pas retelecharge ;
  - sauvegarde au format JSON brut (liste d'observations), lu ensuite
    par parser.py.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from client import WorldBankAPIError, WorldBankClient
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
    """Telecharge les observations d'un indicateur Banque mondiale au format JSON."""

    def __init__(self, client: WorldBankClient, config: Config) -> None:
        self.client = client
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.logger = get_logger()

    # ------------------------------------------------------------------
    def _output_path(self, dataset_meta: DatasetMeta, country_code: str, start_period: str, end_period: str) -> Path:
        safe_id = dataset_meta.indicator_id.replace(".", "_")
        filename = f"WB_{safe_id}_{country_code}_{start_period}_{end_period}.json"
        return self.data_dir / filename

    # ------------------------------------------------------------------
    def download(
        self,
        dataset_meta: DatasetMeta,
        country_code: Optional[str] = None,
        start_year: int = 1960,
        end_year: Optional[int] = None,
    ) -> List[DownloadResult]:
        """Telecharge toutes les observations d'un indicateur pour le pays
        cible sur la periode demandee, en suivant la pagination de l'API.

        Retourne une liste a un seul element (par coherence d'interface
        avec le connecteur OCDE, qui decoupe en plusieurs tranches/fichiers).
        """
        country_code = country_code or self.config.target_country_code
        end_year = end_year or datetime.now().year

        start_period = str(start_year)
        end_period = str(end_year)
        output_path = self._output_path(dataset_meta, country_code, start_period, end_period)

        if output_path.exists() and output_path.stat().st_size > 0:
            self.logger.info("Fichier deja present, reprise (skip) : %s", output_path.name)
            return [
                DownloadResult(
                    dataset_meta=dataset_meta,
                    file_path=output_path,
                    start_period=start_period,
                    end_period=end_period,
                    success=True,
                    from_cache=True,
                )
            ]

        path = f"/country/{country_code}/indicator/{dataset_meta.indicator_id}"
        all_records: List[dict] = []
        page = 1
        total_pages = 1

        try:
            while page <= total_pages:
                metadata, records = self.client.get_json(
                    path,
                    params={
                        "per_page": 1000,
                        "page": page,
                        "date": f"{start_period}:{end_period}",
                    },
                )
                if metadata:
                    total_pages = int(metadata.get("pages") or 1)
                all_records.extend(records)
                page += 1

            if not all_records:
                self.logger.info(
                    "Aucune observation pour %s [%s-%s].",
                    dataset_meta.indicator_id, start_period, end_period,
                )
                return [
                    DownloadResult(
                        dataset_meta=dataset_meta,
                        file_path=output_path,
                        start_period=start_period,
                        end_period=end_period,
                        success=True,
                        error="no_data",
                    )
                ]

            # Ecriture atomique : fichier temporaire puis renommage.
            tmp_path = output_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(all_records, ensure_ascii=False), encoding="utf-8")
            tmp_path.rename(output_path)

            self.logger.info("Telecharge : %s (%d observations)", output_path.name, len(all_records))
            return [
                DownloadResult(
                    dataset_meta=dataset_meta,
                    file_path=output_path,
                    start_period=start_period,
                    end_period=end_period,
                    success=True,
                )
            ]

        except WorldBankAPIError as exc:
            self.logger.error(
                "Echec du telechargement pour %s [%s-%s] : %s",
                dataset_meta.indicator_id, start_period, end_period, exc,
            )
            return [
                DownloadResult(
                    dataset_meta=dataset_meta,
                    file_path=output_path,
                    start_period=start_period,
                    end_period=end_period,
                    success=False,
                    error=str(exc),
                )
            ]
