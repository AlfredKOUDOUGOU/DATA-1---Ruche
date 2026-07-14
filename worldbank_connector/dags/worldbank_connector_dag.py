"""
worldbank_connector_dag.py
===========================
DAG Apache Airflow permettant d'executer automatiquement le connecteur
Banque mondiale (BF Pulse) chaque semaine.

Installation :
    1. Copier ce fichier dans le dossier `dags/` de votre instance Airflow.
    2. Copier (ou monter) le dossier `worldbank_connector/` complet sur la
       machine/le worker Airflow, par exemple sous `/opt/bf_pulse/worldbank_connector`.
    3. Definir les variables d'environnement (.env) ou des Airflow
       Variables/Connections equivalentes sur le worker.
    4. Adapter la constante CONNECTOR_DIR ci-dessous si necessaire.

Le DAG s'execute tous les lundis a 03h30 (UTC) et :
    - lance `python main.py` ;
    - envoie le code retour et les logs a Airflow (visibles dans l'UI).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Chemin vers le dossier du connecteur sur le worker Airflow
CONNECTOR_DIR = "/opt/bf_pulse/worldbank_connector"

# Interpreteur Python a utiliser (idealement un venv dedie avec requirements.txt installe)
PYTHON_BIN = "python3"

default_args = {
    "owner": "bf_pulse",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["alerts@bfpulse.example"],
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="bf_pulse_worldbank_connector",
    description="Collecte hebdomadaire des indicateurs Banque mondiale pour le Burkina Faso (BF Pulse)",
    default_args=default_args,
    schedule_interval="30 3 * * 1",  # Chaque lundi a 03h30 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bf_pulse", "worldbank", "data_engineering"],
) as dag:

    run_worldbank_connector = BashOperator(
        task_id="run_worldbank_connector",
        bash_command=(
            f"cd {CONNECTOR_DIR} && "
            f"{PYTHON_BIN} main.py --start-year 1990"
        ),
        env={
            # Les variables sensibles sont idealement injectees via
            # Airflow Connections/Variables plutot que codees en dur ici.
            # Elles peuvent aussi provenir d'un fichier .env present sur
            # le worker, charge automatiquement par config/config.py.
        },
    )

    run_worldbank_connector
