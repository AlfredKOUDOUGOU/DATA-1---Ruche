"""
database.py
============
Gestion de la persistance PostgreSQL via SQLAlchemy.

Responsabilites :
  - creation du moteur de connexion SQLAlchemy ;
  - definition et creation des tables (sources, datasets, indicators,
    observations) ;
  - insertion et mise a jour (upsert) idempotentes des donnees, afin
    que des executions repetees du connecteur ne creent pas de doublons.

Le schema est identique a celui du connecteur OCDE (meme structure de
tables), ce qui permet de faire cohabiter plusieurs sources (OCDE,
Banque mondiale, etc.) dans la meme base BF Pulse.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

from config.config import Config
from discover import DatasetMeta
from logger import get_logger


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    url = Column(String(500))
    description = Column(Text)

    datasets = relationship("Dataset", back_populates="source", cascade="all, delete-orphan")


class Dataset(Base):
    __tablename__ = "datasets"
    __table_args__ = (UniqueConstraint("dataset_code", "source_id", name="uq_dataset_code_source"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False)
    dataset_code = Column(String(255), nullable=False)
    dataset_name = Column(String(500))
    description = Column(Text)
    last_update = Column(DateTime, default=datetime.utcnow)

    source = relationship("Source", back_populates="datasets")
    indicators = relationship("Indicator", back_populates="dataset", cascade="all, delete-orphan")


class Indicator(Base):
    __tablename__ = "indicators"
    __table_args__ = (
        UniqueConstraint("indicator_code", "dataset_id", name="uq_indicator_code_dataset"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    indicator_code = Column(String(255), nullable=False)
    indicator_name = Column(String(500))
    unit = Column(String(100))
    frequency = Column(String(50))

    dataset = relationship("Dataset", back_populates="indicators")
    observations = relationship(
        "Observation", back_populates="indicator", cascade="all, delete-orphan"
    )


class Observation(Base):
    __tablename__ = "observations"
    __table_args__ = (
        UniqueConstraint(
            "indicator_id", "country_code", "year", name="uq_observation_indicator_country_year"
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_id = Column(Integer, ForeignKey("indicators.id", ondelete="CASCADE"), nullable=False)
    country_code = Column(String(10), nullable=False)
    year = Column(Integer, nullable=False)
    value = Column(Float)

    indicator = relationship("Indicator", back_populates="observations")


class DatabaseManager:
    """Point d'entree unique pour toutes les operations PostgreSQL."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger()
        self.engine = create_engine(config.database_url, pool_pre_ping=True, future=True)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)

    # ------------------------------------------------------------------
    def create_tables(self) -> None:
        """Cree les tables si elles n'existent pas deja (idempotent)."""
        self.logger.info("Verification/creation des tables PostgreSQL...")
        Base.metadata.create_all(self.engine)
        self.logger.info("Tables pretes : sources, datasets, indicators, observations.")

    def check_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Connexion PostgreSQL impossible : %s", exc)
            return False

    # ------------------------------------------------------------------
    def get_or_create_source(
        self, session: Session, name: str, url: str = "", description: str = ""
    ) -> Source:
        source = session.query(Source).filter_by(name=name).one_or_none()
        if source is None:
            source = Source(name=name, url=url, description=description)
            session.add(source)
            session.flush()
            self.logger.info("Source creee : %s", name)
        return source

    def upsert_dataset(self, session: Session, source: Source, dataset_meta: DatasetMeta) -> Dataset:
        dataset = (
            session.query(Dataset)
            .filter_by(dataset_code=dataset_meta.indicator_id, source_id=source.id)
            .one_or_none()
        )
        if dataset is None:
            dataset = Dataset(
                source_id=source.id,
                dataset_code=dataset_meta.indicator_id,
                dataset_name=dataset_meta.name,
                description=dataset_meta.description,
                last_update=datetime.utcnow(),
            )
            session.add(dataset)
            session.flush()
            self.logger.info("Dataset cree : %s", dataset_meta.indicator_id)
        else:
            dataset.dataset_name = dataset_meta.name
            dataset.description = dataset_meta.description
            dataset.last_update = datetime.utcnow()
            self.logger.debug("Dataset mis a jour : %s", dataset_meta.indicator_id)
        return dataset

    def upsert_indicator(
        self,
        session: Session,
        dataset: Dataset,
        indicator_code: str,
        indicator_name: str,
        unit: Optional[str],
        frequency: Optional[str],
    ) -> Indicator:
        indicator = (
            session.query(Indicator)
            .filter_by(indicator_code=indicator_code, dataset_id=dataset.id)
            .one_or_none()
        )
        if indicator is None:
            indicator = Indicator(
                dataset_id=dataset.id,
                indicator_code=indicator_code,
                indicator_name=indicator_name,
                unit=unit,
                frequency=frequency,
            )
            session.add(indicator)
            session.flush()
        else:
            indicator.indicator_name = indicator_name or indicator.indicator_name
            indicator.unit = unit or indicator.unit
            indicator.frequency = frequency or indicator.frequency
        return indicator

    def bulk_upsert_observations(self, session: Session, indicator: Indicator, df: pd.DataFrame) -> int:
        """Insere/actualise en masse les observations d'un indicateur.

        Utilise la clause PostgreSQL `ON CONFLICT ... DO UPDATE` pour une
        operation idempotente et performante (une seule requete par lot).
        """
        if df.empty:
            return 0

        records = [
            {
                "indicator_id": indicator.id,
                "country_code": row["country_code"],
                "year": int(row["year"]),
                "value": float(row["value"]) if pd.notna(row["value"]) else None,
            }
            for _, row in df.iterrows()
        ]

        if not records:
            return 0

        stmt = pg_insert(Observation.__table__).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_observation_indicator_country_year",
            set_={"value": stmt.excluded.value},
        )
        session.execute(stmt)
        return len(records)

    # ------------------------------------------------------------------
    def load_dataframe(
        self,
        dataset_meta: DatasetMeta,
        df: pd.DataFrame,
        source_name: str = "Banque mondiale",
        source_url: str = "https://data.worldbank.org",
        source_description: str = "World Bank Open Data (api.worldbank.org)",
    ) -> int:
        """Charge un DataFrame normalise (sortie de parser.py) en base.

        Cree/actualise la source, le dataset, chaque indicateur rencontre,
        puis insere les observations en masse. Retourne le nombre
        d'observations inserees/mises a jour.
        """
        if df.empty:
            self.logger.info("Aucune observation a charger pour %s.", dataset_meta.indicator_id)
            return 0

        total_observations = 0

        with self.SessionLocal() as session:
            try:
                source = self.get_or_create_source(
                    session, source_name, source_url, source_description
                )
                dataset = self.upsert_dataset(session, source, dataset_meta)

                for indicator_code, group in df.groupby("indicator_code"):
                    indicator_name = group["indicator_name"].iloc[0]
                    unit = group["unit"].iloc[0] if "unit" in group else None
                    frequency = group["frequency"].iloc[0] if "frequency" in group else None

                    indicator = self.upsert_indicator(
                        session, dataset, indicator_code, indicator_name, unit, frequency
                    )
                    inserted = self.bulk_upsert_observations(session, indicator, group)
                    total_observations += inserted

                session.commit()
                self.logger.info(
                    "Chargement OK pour %s : %d observations.",
                    dataset_meta.indicator_id, total_observations,
                )
            except Exception:
                session.rollback()
                self.logger.exception(
                    "Echec du chargement en base pour %s, transaction annulee.",
                    dataset_meta.indicator_id,
                )
                raise

        return total_observations

    # ------------------------------------------------------------------
    def get_summary_counts(self) -> dict:
        """Retourne un resume (nombre de datasets, indicateurs, observations)."""
        with self.SessionLocal() as session:
            return {
                "datasets": session.query(Dataset).count(),
                "indicators": session.query(Indicator).count(),
                "observations": session.query(Observation).count(),
            }
