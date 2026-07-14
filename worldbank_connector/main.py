#!/usr/bin/env python3
"""
main.py
=======
Point d'entree du connecteur Banque mondiale - BF Pulse.

Orchestre l'ensemble du pipeline :
    1. Chargement de la configuration (.env).
    2. Connexion a l'API Banque mondiale et a PostgreSQL.
    3. Decouverte des indicateurs couvrant le Burkina Faso.
    4. Telechargement des observations.
    5. Transformation (nettoyage + normalisation).
    6. Chargement en base PostgreSQL.
    7. Generation d'un rapport final.

Usage :
    python main.py
    python main.py --start-year 2000 --end-year 2024
    python main.py --max-datasets 5          # utile pour un test rapide
    python main.py --skip-db                 # decouverte + telechargement uniquement
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from typing import List

from client import WorldBankAPIError, WorldBankClient
from config.config import load_config
from database import DatabaseManager
from discover import DatasetDiscovery, DatasetMeta
from downloader import DataDownloader
from logger import setup_logger
from parser import DataParser


@dataclass
class RunReport:
    """Compteurs accumules pendant l'execution, affiches dans le rapport final."""

    datasets_found: int = 0
    datasets_processed: int = 0
    datasets_failed: int = 0
    indicators_count: int = 0
    observations_inserted: int = 0
    errors: List[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Connecteur Banque mondiale - BF Pulse : collecte des indicateurs Burkina Faso."
    )
    parser.add_argument(
        "--start-year", type=int, default=1960, help="Annee de debut des observations (defaut : 1960)"
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="Annee de fin des observations (defaut : annee courante)",
    )
    parser.add_argument(
        "--max-datasets", type=int, default=None,
        help="Limite le nombre d'indicateurs traites (utile pour tester rapidement)",
    )
    parser.add_argument(
        "--skip-db", action="store_true",
        help="N'effectue que la decouverte et le telechargement, sans ecrire en base",
    )
    return parser.parse_args()


def print_banner() -> None:
    print("=" * 70)
    print(" BF Pulse - Connecteur Banque mondiale ".center(70, "="))
    print("=" * 70)


def print_datasets_summary(datasets: List[DatasetMeta]) -> None:
    print("\nIndicateur(s) trouve(s) contenant des donnees pour le Burkina Faso :")
    if not datasets:
        print("  (aucun indicateur trouve)")
        return
    for ds in datasets[:30]:
        print(f"  - {ds.name} [{ds.full_id}]")
    if len(datasets) > 30:
        print(f"  ... et {len(datasets) - 30} de plus.")
    print()


def print_final_report(report: RunReport, elapsed_seconds: float) -> None:
    print("\n" + "=" * 70)
    print(" Rapport final ".center(70, "="))
    print("=" * 70)
    print(f"Nombre d'indicateurs recuperes      : {report.datasets_processed}/{report.datasets_found}")
    print(f"Nombre d'indicateurs en echec        : {report.datasets_failed}")
    print(f"Nombre d'observations inserees      : {report.observations_inserted}")
    print(f"Temps d'execution                  : {elapsed_seconds:.1f} secondes")
    if report.errors:
        print(f"\nErreurs rencontrees ({len(report.errors)}) :")
        for err in report.errors[:20]:
            print(f"  - {err}")
    print("=" * 70)


def run() -> int:
    args = parse_args()
    start_time = time.monotonic()

    config = load_config()
    logger = setup_logger(config.logs_dir, config.log_level)

    print_banner()
    logger.info("Demarrage du connecteur Banque mondiale - BF Pulse.")
    logger.info(
        "Configuration chargee : pays cible=%s, source WB=%s, quota API=%d req/min, base=%s",
        config.target_country_code,
        config.wb_source_id or "toutes",
        config.wb_max_requests_per_minute,
        config.wb_api_base_url,
    )

    report = RunReport()

    db_manager = None
    if not args.skip_db:
        db_manager = DatabaseManager(config)
        if not db_manager.check_connection():
            logger.error(
                "Impossible de se connecter a PostgreSQL. Verifiez les parametres DB_* du fichier .env."
            )
            return 1
        db_manager.create_tables()

    with WorldBankClient(config) as client:
        discovery = DatasetDiscovery(client, config)
        downloader = DataDownloader(client, config)
        data_parser = DataParser()

        try:
            datasets = discovery.find_datasets_with_country(config.target_country_code)
        except WorldBankAPIError as exc:
            logger.error("Echec de la decouverte des indicateurs : %s", exc)
            print_final_report(report, time.monotonic() - start_time)
            return 1

        if args.max_datasets:
            datasets = datasets[: args.max_datasets]

        report.datasets_found = len(datasets)
        print_datasets_summary(datasets)

        for dataset_meta in datasets:
            logger.info("Traitement de l'indicateur : %s", dataset_meta.full_id)
            try:
                download_results = downloader.download(
                    dataset_meta,
                    country_code=config.target_country_code,
                    start_year=args.start_year,
                    end_year=args.end_year,
                )
                file_paths = [r.file_path for r in download_results if r.success and r.file_path.exists()]

                df = data_parser.parse_many(file_paths, dataset_meta, config.target_country_code)

                report.indicators_count += df["indicator_code"].nunique() if not df.empty else 0

                if not args.skip_db and db_manager is not None:
                    inserted = db_manager.load_dataframe(dataset_meta, df)
                    report.observations_inserted += inserted
                else:
                    logger.info(
                        "Mode --skip-db actif : %d lignes normalisees mais non inserees.",
                        len(df),
                    )

                report.datasets_processed += 1

            except Exception as exc:  # noqa: BLE001
                logger.exception("Echec du traitement de l'indicateur %s", dataset_meta.full_id)
                report.datasets_failed += 1
                report.errors.append(f"{dataset_meta.full_id}: {exc}")

    elapsed = time.monotonic() - start_time
    print_final_report(report, elapsed)
    logger.info("Execution terminee en %.1f secondes.", elapsed)

    return 0 if report.datasets_failed == 0 else 2


def main() -> None:
    try:
        exit_code = run()
    except KeyboardInterrupt:
        print("\nInterrompu par l'utilisateur.")
        exit_code = 130
    except Exception as exc:  # noqa: BLE001
        print(f"Erreur fatale : {exc}", file=sys.stderr)
        raise
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
