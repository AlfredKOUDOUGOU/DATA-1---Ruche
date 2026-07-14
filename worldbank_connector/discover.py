"""
discover.py
===========
Decouverte du catalogue d'indicateurs de la Banque mondiale et
identification de ceux disposant de donnees pour le pays cible.

Contrairement au connecteur OCDE (ou il faut analyser une DSD complete
pour savoir si un pays est couvert), l'API Banque mondiale permet de
verifier directement la couverture d'un indicateur pour un pays via un
appel leger : GET /country/{pays}/indicator/{indicateur}?per_page=1.
La reponse contient un champ `total` (nombre d'observations disponibles) :
si `total == 0`, l'indicateur ne couvre pas le pays cible.

Strategie :
  1. Lister les indicateurs (globalement, ou restreints a une source
     comme les "World Development Indicators", id=2, ce qui est
     fortement recommande : ~1500 indicateurs contre ~17000+ au total).
  2. Pour chaque indicateur, effectuer un appel leger (per_page=1) afin
     de verifier s'il existe au moins une observation pour le pays cible.
  3. Conserver les indicateurs couverts, avec leurs metadonnees (nom,
     unite, source, themes) deja fournies par l'endpoint /indicator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from client import WorldBankAPIError, WorldBankClient
from config.config import Config
from logger import get_logger


@dataclass
class DatasetMeta:
    """Equivalent d'un "dataflow" OCDE : ici, un indicateur Banque mondiale."""

    indicator_id: str
    name: str
    source_id: Optional[str] = None
    source_value: Optional[str] = None
    source_note: Optional[str] = None
    source_organization: Optional[str] = None
    topics: List[str] = field(default_factory=list)
    unit: Optional[str] = None
    total_observations: Optional[int] = None

    @property
    def full_id(self) -> str:
        return f"WB:{self.indicator_id}"

    # Champs conserves pour compatibilite avec le schema generique
    # (parser.py / database.py attendent parfois ces noms).
    @property
    def dataflow_id(self) -> str:
        return self.indicator_id

    @property
    def agency_id(self) -> str:
        return "WB"

    @property
    def version(self) -> str:
        return "1.0"

    @property
    def indicators(self) -> List["DatasetMeta"]:
        # Un indicateur Banque mondiale est "atomique" : il ne se
        # decompose pas en sous-indicateurs comme un dataflow OCDE.
        return [self]

    @property
    def description(self) -> str:
        return self.source_note or f"Indicateur Banque mondiale '{self.name}'."


class DatasetDiscovery:
    """Explore le catalogue Banque mondiale et identifie les indicateurs pertinents."""

    def __init__(self, client: WorldBankClient, config: Config) -> None:
        self.client = client
        self.config = config
        self.logger = get_logger()

    # ------------------------------------------------------------------
    # Etape 1 : liste des indicateurs
    # ------------------------------------------------------------------
    def list_indicators(self) -> List[DatasetMeta]:
        """Recupere la liste des indicateurs (paginee), eventuellement
        restreinte a une source (WB_SOURCE_ID, ex: 2 = World Development
        Indicators).
        """
        if self.config.wb_source_id:
            path = f"/source/{self.config.wb_source_id}/indicator"
            self.logger.info(
                "Recuperation des indicateurs de la source Banque mondiale id=%s...",
                self.config.wb_source_id,
            )
        else:
            path = "/indicator"
            self.logger.info(
                "Recuperation de l'integralite du catalogue d'indicateurs Banque mondiale "
                "(aucune source specifiee, cela peut etre long)..."
            )

        indicators: List[DatasetMeta] = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            metadata, records = self.client.get_json(path, params={"per_page": 1000, "page": page})
            if metadata:
                total_pages = int(metadata.get("pages") or 1)

            for rec in records:
                indicators.append(self._parse_indicator_record(rec))

            page += 1

        self.logger.info("%d indicateurs trouves.", len(indicators))

        if self.config.max_indicators_to_scan:
            indicators = indicators[: self.config.max_indicators_to_scan]
            self.logger.info(
                "Analyse limitee aux %d premiers indicateurs (MAX_INDICATORS_TO_SCAN).",
                len(indicators),
            )

        return indicators

    @staticmethod
    def _parse_indicator_record(rec: Dict) -> DatasetMeta:
        source = rec.get("source") or {}
        topics = [t.get("value", "") for t in (rec.get("topics") or []) if t.get("value")]
        return DatasetMeta(
            indicator_id=rec.get("id", ""),
            name=(rec.get("name") or rec.get("id") or "").strip(),
            source_id=str(source.get("id")) if source.get("id") is not None else None,
            source_value=source.get("value"),
            source_note=(rec.get("sourceNote") or "").strip() or None,
            source_organization=(rec.get("sourceOrganization") or "").strip() or None,
            topics=topics,
            unit=(rec.get("unit") or "").strip() or None,
        )

    # ------------------------------------------------------------------
    # Etape 2 : verification de la couverture par pays
    # ------------------------------------------------------------------
    def _check_country_coverage(self, indicator_id: str, country_code: str) -> int:
        """Retourne le nombre d'observations disponibles pour cet
        indicateur et ce pays (0 si aucune donnee ou en cas d'erreur)."""
        path = f"/country/{country_code}/indicator/{indicator_id}"
        try:
            metadata, _records = self.client.get_json(path, params={"per_page": 1})
        except WorldBankAPIError as exc:
            self.logger.warning(
                "Impossible de verifier la couverture de %s pour %s : %s",
                indicator_id, country_code, exc,
            )
            return 0

        if not metadata:
            return 0
        return int(metadata.get("total") or 0)

    # ------------------------------------------------------------------
    # Etape 3 : recherche des indicateurs couvrant le pays cible
    # ------------------------------------------------------------------
    def find_datasets_with_country(self, country_code: Optional[str] = None) -> List[DatasetMeta]:
        """Parcourt les indicateurs du catalogue (ou de la source
        configuree) et retourne ceux couvrant `country_code`.

        Si `config.skip_coverage_check` est actif, la verification de
        couverture est sautee : tous les indicateurs listes sont retournes
        tels quels (le telechargement determinera lui-meme s'il existe des
        donnees). Cela evite un appel API supplementaire par indicateur,
        au prix de telechargements "a vide" pour les indicateurs non
        couverts.
        """
        country_code = country_code or self.config.target_country_code
        indicators = self.list_indicators()

        if self.config.skip_coverage_check:
            self.logger.info(
                "Verification de couverture desactivee (SKIP_COVERAGE_CHECK) : "
                "%d indicateur(s) seront tentes directement.",
                len(indicators),
            )
            return indicators

        matches: List[DatasetMeta] = []
        for meta in indicators:
            self.logger.info("Verification de la couverture pour '%s' (%s)...", meta.name, meta.indicator_id)
            total = self._check_country_coverage(meta.indicator_id, country_code)
            if total > 0:
                meta.total_observations = total
                matches.append(meta)
                self.logger.info(
                    "-> %s contient %d observation(s) pour %s.", meta.indicator_id, total, country_code
                )
            else:
                self.logger.debug("%s n'est pas couvert par %s.", country_code, meta.indicator_id)

        self.logger.info(
            "%d indicateur(s) identifie(s) comme couvrant %s.", len(matches), country_code
        )
        return matches
