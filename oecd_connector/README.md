# Connecteur OCDE — BF Pulse

Connecteur Python professionnel permettant de collecter automatiquement, via l'API SDMX de l'OCDE, les indicateurs disponibles pour le **Burkina Faso (BFA)**, puis de les centraliser dans une base **PostgreSQL** pour alimenter les tableaux de bord de la plateforme BF Pulse.

## Sommaire

- [Architecture](#architecture)
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
oecd_connector/
├── README.md
├── requirements.txt
├── .env.example          # Modele de configuration (copier vers .env)
├── .gitignore
├── main.py                # Point d'entree / orchestration
├── client.py               # Client HTTP OCDE (retry, rate limiting, erreurs)
├── discover.py              # Decouverte des dataflows + metadonnees
├── downloader.py             # Telechargement SDMX (pagination, reprise)
├── parser.py                  # Transformation CSV -> DataFrame normalise
├── database.py                 # Connexion et persistance PostgreSQL
├── logger.py                    # Configuration des logs
├── config/
│   └── config.py                 # Chargement de la configuration (.env)
├── data/                            # Fichiers CSV bruts telecharges
├── logs/                             # Fichiers de logs
└── dags/
    └── oecd_connector_dag.py           # DAG Airflow (execution hebdomadaire)
```

Chaque module a une responsabilite unique : `client.py` ne connait rien de SDMX metier, `discover.py` ne telecharge pas d'observations, `downloader.py` ne parse pas les fichiers, etc. Cela permet de faire evoluer independamment chaque etape (par exemple ajouter une autre source comme la Banque mondiale en dupliquant ce meme patron d'architecture).

## Prerequis

- Python **3.10** ou superieur
- Une instance **PostgreSQL** accessible (locale ou distante)
- Acces reseau sortant vers `sdmx.oecd.org`

## Installation

```bash
# 1. Cloner / copier le dossier oecd_connector
cd oecd_connector

# 2. Creer un environnement virtuel (recommande)
python3 -m venv venv
source venv/bin/activate      # Windows : venv\Scripts\activate

# 3. Installer les dependances
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
   | `DB_HOST` | Hote PostgreSQL | `localhost` |
   | `DB_PORT` | Port PostgreSQL | `5432` |
   | `DB_NAME` | Nom de la base | `bf_pulse` |
   | `DB_USER` | Utilisateur PostgreSQL | `bf_pulse_user` |
   | `DB_PASSWORD` | Mot de passe PostgreSQL | *(secret)* |
   | `OECD_API_BASE_URL` | Base de l'API SDMX OCDE | `https://sdmx.oecd.org/public/rest` |
   | `TARGET_COUNTRY_CODE` | Code ISO3 du pays cible | `BFA` |
   | `OECD_MAX_REQUESTS_PER_HOUR` | Quota de requetes/heure | `60` |
   | `OECD_MAX_RETRIES` / `OECD_RETRY_BACKOFF` | Politique de retry | `3` / `5` |
   | `DATA_DIR` / `LOGS_DIR` | Chemins locaux | `./data` / `./logs` |
   | `LOG_LEVEL` | Niveau de log | `INFO` |
   | `MAX_DATAFLOWS_TO_SCAN` | Limite optionnelle pour les tests | *(vide = tout scanner)* |

   **Aucune valeur sensible n'est codee en dur dans le code** : tout passe par `.env`, charge par `config/config.py` via `python-dotenv`.

## Base de donnees

Au premier lancement, `main.py` cree automatiquement (si absentes) les tables suivantes dans PostgreSQL :

| Table | Colonnes | Role |
|---|---|---|
| `sources` | `id, name, url, description` | Organismes fournisseurs (OCDE, futurs : Banque mondiale, OMS...) |
| `datasets` | `id, source_id, dataset_code, dataset_name, description, last_update` | Dataflows OCDE couvrant le Burkina Faso |
| `indicators` | `id, dataset_id, indicator_code, indicator_name, unit, frequency` | Indicateurs disponibles par dataset |
| `observations` | `id, indicator_id, country_code, year, value` | Valeurs annuelles observees |

Les insertions sont **idempotentes** (upsert via `ON CONFLICT`) : relancer le connecteur ne cree pas de doublons, il met a jour les valeurs existantes.

## Lancement

```bash
python main.py
```

Deroulement attendu :

```
======================================================================
================ BF Pulse - Connecteur OCDE =================
======================================================================

Dataset(s) trouve(s) contenant des donnees pour le Burkina Faso :
  - OECD Health Statistics [OECD:HEALTH_STAT(1.0)] (42 indicateur(s))
  - OECD Education Statistics [OECD:EDU_STAT(1.0)] (18 indicateur(s))
  - OECD Environment Statistics [OECD:ENV_STAT(1.0)] (12 indicateur(s))

======================================================================
==================== Rapport final ====================
======================================================================
Nombre de datasets recuperes      : 3/3
Nombre de datasets en echec        : 0
Nombre d'indicateurs               : 72
Nombre d'observations inserees      : 4830
Temps d'execution                  : 187.4 secondes
======================================================================
```

### Options en ligne de commande

```bash
python main.py --start-year 2000 --end-year 2024   # restreindre la periode
python main.py --max-datasets 5                       # limiter le nombre de dataflows (tests)
python main.py --skip-db                                # decouverte + telechargement uniquement, sans ecrire en base
```

## Exemple d'utilisation

Utilisation programmatique des modules (par exemple depuis un notebook ou un autre script) :

```python
from config.config import load_config
from client import OECDClient
from discover import DatasetDiscovery
from downloader import DataDownloader
from parser import DataParser

config = load_config()

with OECDClient(config) as client:
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

Un DAG pret a l'emploi est fourni dans `dags/oecd_connector_dag.py` :

- planification hebdomadaire (`0 3 * * 1` : chaque lundi a 03h00 UTC) ;
- `retries=2` avec un delai de 15 minutes ;
- execution via `BashOperator` (`python main.py`).

Etapes d'integration :

1. Copier `dags/oecd_connector_dag.py` dans le dossier `dags/` de votre instance Airflow.
2. Deployer le dossier `oecd_connector/` complet sur le worker (ex. `/opt/bf_pulse/oecd_connector`).
3. Adapter la constante `CONNECTOR_DIR` dans le DAG si le chemin differe.
4. S'assurer que le fichier `.env` (ou des variables d'environnement equivalentes) est present sur le worker.

## Limitations connues

- L'API SDMX de l'OCDE peut presenter des schemas legerement differents selon les dataflows (nom de la dimension geographique, presence ou non d'une dimension "indicateur" dediee). Le module `discover.py` applique une detection heuristique tolerante ; certains dataflows atypiques peuvent necessiter un ajustement manuel des listes `GEO_DIMENSION_CANDIDATES` / `INDICATOR_DIMENSION_CANDIDATES`.
- Le quota de **60 requetes/heure** est applique cote client (`RateLimiter` dans `client.py`) en plus du retry automatique sur les reponses HTTP 429/5xx.
- La couverture reelle du Burkina Faso varie fortement d'un dataflow OCDE a l'autre (certains jeux de donnees OCDE ne couvrent que les pays membres). Le connecteur ne remonte que les dataflows ou `BFA` figure explicitement dans la codelist de la dimension geographique.
