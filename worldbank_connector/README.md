# Connecteur Banque mondiale — BF Pulse

Connecteur Python permettant de collecter automatiquement, via l'API REST/JSON de la **Banque mondiale** (`api.worldbank.org/v2`, portail public `data.worldbank.org`), les indicateurs disponibles pour le **Burkina Faso (BFA)**, puis de les centraliser dans une base **PostgreSQL** pour alimenter les tableaux de bord de la plateforme BF Pulse.

C'est le pendant du connecteur OCDE existant, avec la meme architecture generale (client / discover / downloader / parser / database / main), mais adapte a une API bien plus simple : JSON natif, pas de DSD a analyser, verification de couverture par pays en un seul appel leger.

## Sommaire

- [Architecture](#architecture)
- [Pourquoi c'est plus simple que le connecteur OCDE](#pourquoi-cest-plus-simple-que-le-connecteur-ocde)
- [Prerequis](#prerequis)
- [Installation](#installation)
- [Configuration](#configuration)
- [Base de donnees](#base-de-donnees)
- [Lancement](#lancement)
- [Exemple d'utilisation](#exemple-dutilisation)
- [Integration Airflow](#integration-airflow)
- [Limitations connues](#limitations-connues)

## Architecture

```
worldbank_connector/
├── README.md
├── requirements.txt
├── .env.example          # Modele de configuration (copier vers .env)
├── main.py                # Point d'entree / orchestration
├── client.py               # Client HTTP Banque mondiale (retry, rate limiting, erreurs)
├── discover.py              # Decouverte des indicateurs + verification de couverture pays
├── downloader.py             # Telechargement des observations (pagination, reprise)
├── parser.py                  # Transformation JSON -> DataFrame normalise
├── database.py                 # Connexion et persistance PostgreSQL
├── logger.py                    # Configuration des logs
├── config/
│   └── config.py                 # Chargement de la configuration (.env)
├── data/                            # Fichiers JSON bruts telecharges
├── logs/                             # Fichiers de logs
└── dags/
    └── worldbank_connector_dag.py      # DAG Airflow (execution hebdomadaire)
```

## Pourquoi c'est plus simple que le connecteur OCDE

| | OCDE (SDMX) | Banque mondiale (REST/JSON) |
|---|---|---|
| Format | XML SDMX-ML 2.1 | JSON natif |
| Metadonnees d'un indicateur | Necessite de parser une DSD + des codelists | Deja fournies par `/indicator` (nom, unite, source, themes) |
| Verifier qu'un pays est couvert | Chercher le code pays dans la codelist de la dimension geographique de la DSD | Un seul appel `GET /country/{pays}/indicator/{id}?per_page=1` -> champ `total` |
| Telechargement multi-periodes | Decoupage manuel en tranches d'annees | `date=1960:2025` en un seul appel ; pagination geree par l'API (`page`/`pages`) |
| Quota officiel | Oui (ex: 60 req/h) | Non publie, mais limitation cote client conservee par prudence |

Le module `discover.py` reste volontairement proche du connecteur OCDE dans sa forme (memes noms de methodes, meme dataclass `DatasetMeta`) pour que le reste du pipeline (`downloader.py`, `parser.py`, `database.py`, `main.py`) soit quasiment interchangeable entre les deux sources.

## Prerequis

- Python **3.10** ou superieur
- Une instance **PostgreSQL** accessible (locale ou distante)
- Acces reseau sortant vers `api.worldbank.org`

## Installation

```bash
cd worldbank_connector
python3 -m venv venv
source venv/bin/activate      # Windows : venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

1. Copier le fichier d'exemple :

   ```bash
   cp .env.example .env
   ```

2. Renseigner vos parametres dans `.env` :

   | Variable | Description | Exemple |
   |---|---|---|
   | `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | Parametres PostgreSQL | `localhost` / `5432` / `bf_pulse` / ... |
   | `WB_API_BASE_URL` | Base de l'API Banque mondiale | `https://api.worldbank.org/v2` |
   | `TARGET_COUNTRY_CODE` | Code ISO3 du pays cible | `BFA` |
   | `WB_SOURCE_ID` | ID de source WB a scanner (recommande : `2` = World Development Indicators, ~1500 indicateurs). Vide = tout le catalogue (~17000+, tres long) | `2` |
   | `WB_MAX_REQUESTS_PER_MINUTE` | Limitation cote client (pas de quota officiel WB) | `120` |
   | `WB_MAX_RETRIES` / `WB_RETRY_BACKOFF` | Politique de retry | `3` / `5` |
   | `DATA_DIR` / `LOGS_DIR` | Chemins locaux | `./data` / `./logs` |
   | `LOG_LEVEL` | Niveau de log | `INFO` |
   | `MAX_INDICATORS_TO_SCAN` | Limite optionnelle pour les tests | *(vide = tout scanner)* |
   | `SKIP_COVERAGE_CHECK` | Si `true`, saute la verification de couverture par pays et tente directement le telechargement de chaque indicateur (plus rapide, mais telecharge aussi des indicateurs vides) | `false` |

## Base de donnees

Meme schema que le connecteur OCDE (`sources`, `datasets`, `indicators`, `observations`), avec upsert idempotent (`ON CONFLICT ... DO UPDATE`). Les deux connecteurs peuvent cohabiter dans la meme base : la table `sources` distinguera simplement "OCDE" de "Banque mondiale".

## Lancement

```bash
python main.py
```

### Options en ligne de commande

```bash
python main.py --start-year 2000 --end-year 2024   # restreindre la periode
python main.py --max-datasets 5                       # limiter le nombre d'indicateurs (tests)
python main.py --skip-db                                # decouverte + telechargement uniquement, sans ecrire en base
```

Pour un premier test rapide (evite de scanner 1500 indicateurs) :

```bash
MAX_INDICATORS_TO_SCAN=20 python main.py --max-datasets 5 --skip-db
```

## Exemple d'utilisation

```python
from config.config import load_config
from client import WorldBankClient
from discover import DatasetDiscovery
from downloader import DataDownloader
from parser import DataParser

config = load_config()

with WorldBankClient(config) as client:
    discovery = DatasetDiscovery(client, config)
    datasets = discovery.find_datasets_with_country("BFA")

    downloader = DataDownloader(client, config)
    data_parser = DataParser()

    for dataset in datasets[:1]:
        results = downloader.download(dataset, country_code="BFA", start_year=2010)
        files = [r.file_path for r in results if r.success]
        df = data_parser.parse_many(files, dataset, "BFA")
        print(df.head())
```

## Integration Airflow

Un DAG pret a l'emploi est fourni dans `dags/worldbank_connector_dag.py` (planification hebdomadaire, `retries=2`, execution via `BashOperator`). Memes etapes d'integration que pour le connecteur OCDE (voir son propre README).

## Limitations connues

- La Banque mondiale ne publie pas de quota strict par heure/minute : la limitation cote client (`WB_MAX_REQUESTS_PER_MINUTE`) est une precaution, pas une contrainte imposee par l'API.
- Scanner l'integralite du catalogue (`WB_SOURCE_ID` vide) represente ~17000+ indicateurs et donc potentiellement autant d'appels de verification de couverture ; il est fortement recommande de restreindre a une source (ex: `2` pour les World Development Indicators) ou d'activer `SKIP_COVERAGE_CHECK=true` pour eviter le double appel par indicateur.
- Certains indicateurs Banque mondiale sont infra-annuels (trimestriels/mensuels) ; `parser.py` extrait uniquement l'annee (les 4 premiers caracteres du champ `date`), ce qui agregera implicitement plusieurs observations d'une meme annee sous une seule cle si l'indicateur n'est pas annuel (a affiner selon les besoins de BF Pulse).
- Le code pays retourne par l'API (`countryiso3code`) est utilise pour le filtrage strict ; certains indicateurs regionaux/aggreges (ex: "Sub-Saharan Africa") ne seront jamais retenus puisqu'ils ne correspondent pas au code ISO3 du Burkina Faso.
