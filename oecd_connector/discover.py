"""
discover.py
===========
Decouverte du catalogue de donnees OCDE (SDMX 2.1) et identification des
dataflows (jeux de donnees) contenant des informations sur le Burkina Faso.

Strategie :
  1. Lister tous les "Dataflows" exposes par l'agence OCDE
     (GET /dataflow/{agency}/all/latest).
  2. Pour chaque dataflow, recuperer sa Data Structure Definition (DSD)
     ainsi que les codelists associees (GET /datastructure/.../{id}?references=children).
  3. Localiser la dimension geographique (REF_AREA / COUNTRY / LOCATION)
     et verifier si le code pays cible (ex: BFA) figure dans sa codelist.
  4. Si oui, extraire les metadonnees utiles : nom, description, dimension
     "indicateur", unites, frequence.

Cette approche evite de telecharger l'integralite des observations de
chaque dataflow uniquement pour verifier la presence du Burkina Faso,
ce qui est essentiel pour respecter le quota de 60 requetes/heure.

Remarque : la structure exacte des reponses SDMX peut varier legerement
d'un dataflow a l'autre (nom de dimension geographique, presence ou non
d'une dimension "INDICATOR" dediee, etc.). Le code ci-dessous applique
une detection heuristique tolerante aux variations et journalise les cas
non standards plutot que d'echouer silencieusement.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from client import OECDAPIError, OECDClient
from config.config import Config
from logger import get_logger

# Espaces de noms SDMX-ML 2.1
NAMESPACES = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "xml": "http://www.w3.org/XML/1998/namespace",
}

# Noms de dimensions/attributs candidats pour la zone geographique et l'indicateur
GEO_DIMENSION_CANDIDATES = ["REF_AREA", "LOCATION", "COUNTRY", "COU", "GEO"]
INDICATOR_DIMENSION_CANDIDATES = ["INDICATOR", "MEASURE", "SUBJECT", "SERIES"]
FREQUENCY_DIMENSION_CANDIDATES = ["FREQ", "FREQUENCY"]
UNIT_ATTRIBUTE_CANDIDATES = ["UNIT_MEASURE", "UNIT", "UNIT_MEASURE_TYPE"]


@dataclass
class IndicatorMeta:
    code: str
    name: str
    unit: Optional[str] = None
    frequency: Optional[str] = None


@dataclass
class DatasetMeta:
    dataflow_id: str
    agency_id: str
    version: str
    name: str
    description: str
    source: str
    dsd_id: Optional[str] = None
    geo_dimension: Optional[str] = None
    indicator_dimension: Optional[str] = None
    frequency_dimension: Optional[str] = None
    indicators: List[IndicatorMeta] = field(default_factory=list)
    dimension_order: List[str] = field(default_factory=list)
    period_start: Optional[str] = None
    period_end: Optional[str] = None

    @property
    def full_id(self) -> str:
        return f"{self.agency_id}:{self.dataflow_id}({self.version})"


def _local_name(tag: str) -> str:
    """Retourne le nom local d'un tag XML namespace (ignore le prefixe)."""
    return tag.split("}")[-1] if "}" in tag else tag


def _find_name(element: ET.Element, lang_pref: tuple = ("fr", "en")) -> str:
    """Recherche le meilleur libelle <com:Name> disponible pour un element."""
    names: Dict[str, str] = {}
    for name_el in element.findall("com:Name", NAMESPACES):
        lang = name_el.get("{http://www.w3.org/XML/1998/namespace}lang", "en")
        names[lang] = (name_el.text or "").strip()

    for lang in lang_pref:
        if lang in names and names[lang]:
            return names[lang]
    return next(iter(names.values()), "")


class DatasetDiscovery:
    """Explore le catalogue OCDE et identifie les jeux de donnees pertinents."""

    def __init__(self, client: OECDClient, config: Config) -> None:
        self.client = client
        self.config = config
        self.logger = get_logger()

    # ------------------------------------------------------------------
    # Etape 1 : liste des dataflows
    # ------------------------------------------------------------------
    def list_dataflows(self) -> List[Dict[str, str]]:
        """Recupere la liste complete des dataflows publies par l'agence OCDE.

        Returns:
            Liste de dicts : {id, agency_id, version, name, dsd_id, dsd_agency}
        """
        agency = self.config.oecd_agency_id
        path = f"/dataflow/{agency}/all/latest"
        headers = headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

        self.logger.info("Recuperation de la liste des dataflows OCDE (%s)...", agency)
        xml_text = self.client.get(path, headers=headers)
        root = ET.fromstring(xml_text)

        dataflows: List[Dict[str, str]] = []
        for df_el in root.iter():
            if _local_name(df_el.tag) != "Dataflow":
                continue

            df_id = df_el.get("id", "")
            df_agency = df_el.get("agencyID", agency)
            df_version = df_el.get("version", "latest")
            name = _find_name(df_el) or df_id

            dsd_id, dsd_agency, dsd_version = None, None, "latest"
            structure_el = df_el.find("str:Structure", NAMESPACES)
            if structure_el is not None:
                ref_el = structure_el.find("Ref")
                if ref_el is None:
                    # Certaines implementations utilisent le namespace 'str' pour Ref
                    ref_el = structure_el.find("str:Ref", NAMESPACES)
                if ref_el is not None:
                    dsd_id = ref_el.get("id")
                    dsd_agency = ref_el.get("agencyID", df_agency)
                    dsd_version = ref_el.get("version", "latest")

            dataflows.append(
                {
                    "id": df_id,
                    "agency_id": df_agency,
                    "version": df_version,
                    "name": name,
                    "dsd_id": dsd_id,
                    "dsd_agency": dsd_agency,
                    "dsd_version": dsd_version,
                }
            )

        self.logger.info("%d dataflows trouves.", len(dataflows))

        if self.config.max_dataflows_to_scan:
            dataflows = dataflows[: self.config.max_dataflows_to_scan]
            self.logger.info(
                "Analyse limitee aux %d premiers dataflows (MAX_DATAFLOWS_TO_SCAN).",
                len(dataflows),
            )

        return dataflows

    # ------------------------------------------------------------------
    # Etape 2 : DSD + codelists d'un dataflow
    # ------------------------------------------------------------------
    def _fetch_datastructure(
        self, dsd_id: str, dsd_agency: str, dsd_version: str = "latest"
    ) -> Optional[ET.Element]:
        path = f"/datastructure/{dsd_agency}/{dsd_id}/{dsd_version}"
        params = {"references": "children"}
        headers = headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

        try:
            xml_text = self.client.get(path, params=params, headers=headers)
        except OECDAPIError as exc:
            self.logger.warning("DSD introuvable pour %s : %s", dsd_id, exc)
            return None

        return ET.fromstring(xml_text)

    def _extract_dimension_ids(self, dsd_root: ET.Element) -> Dict[str, str]:
        """Associe chaque dimension a l'id de codelist qu'elle reference.

        Returns: {dimension_id: codelist_id}
        """
        mapping: Dict[str, str] = {}
        for dim_el in dsd_root.iter():
            if _local_name(dim_el.tag) not in ("Dimension", "TimeDimension"):
                continue
            dim_id = dim_el.get("id")
            if not dim_id:
                continue

            codelist_id = None
            enum_el = dim_el.find(".//str:Enumeration/Ref", NAMESPACES)
            if enum_el is None:
                enum_el = dim_el.find(".//Enumeration/Ref")
            if enum_el is not None:
                codelist_id = enum_el.get("id")

            if codelist_id:
                mapping[dim_id] = codelist_id
        return mapping

    def _extract_dimension_order(self, dsd_root: ET.Element) -> List[str]:
        """Retourne la liste des dimensions dans leur ordre positionnel.

        L'ordre est essentiel pour construire la cle SDMX (ex: "FREQ.BFA.INDIC")
        utilisee lors du telechargement des observations.
        """
        dims: List[tuple] = []
        for dim_el in dsd_root.iter():
            local = _local_name(dim_el.tag)
            if local not in ("Dimension", "TimeDimension"):
                continue
            dim_id = dim_el.get("id")
            position = dim_el.get("position")
            if dim_id is None:
                continue
            try:
                pos_val = int(position) if position is not None else len(dims)
            except ValueError:
                pos_val = len(dims)
            dims.append((pos_val, dim_id, local))

        dims.sort(key=lambda t: t[0])
        # La dimension temporelle (TimeDimension) n'entre pas dans la cle positionnelle
        # habituelle (elle est geree via startPeriod/endPeriod), on l'exclut de l'ordre.
        return [dim_id for _, dim_id, local in dims if local != "TimeDimension"]

    def _extract_codelists(self, dsd_root: ET.Element) -> Dict[str, Dict[str, str]]:
        """Extrait toutes les codelists presentes dans la reponse DSD (references=children).

        Returns: {codelist_id: {code: label}}
        """
        codelists: Dict[str, Dict[str, str]] = {}
        for cl_el in dsd_root.iter():
            if _local_name(cl_el.tag) != "Codelist":
                continue
            cl_id = cl_el.get("id")
            if not cl_id:
                continue
            codes: Dict[str, str] = {}
            for code_el in cl_el.findall("str:Code", NAMESPACES):
                code_id = code_el.get("id")
                if code_id:
                    codes[code_id] = _find_name(code_el) or code_id
            codelists[cl_id] = codes
        return codelists

    def _first_matching_dimension(
        self, dim_to_codelist: Dict[str, str], candidates: List[str]
    ) -> Optional[str]:
        for candidate in candidates:
            for dim_id in dim_to_codelist:
                if dim_id.upper() == candidate.upper():
                    return dim_id
        return None

    # ------------------------------------------------------------------
    # Etape 3 : recherche des dataflows contenant le pays cible
    # ------------------------------------------------------------------
    def find_datasets_with_country(self, country_code: Optional[str] = None) -> List[DatasetMeta]:
        """Parcourt tous les dataflows et retourne ceux couvrant `country_code`.

        Chaque dataflow retenu est enrichi avec ses metadonnees (indicateurs,
        unites, frequence) extraites de sa DSD.
        """
        country_code = country_code or self.config.target_country_code
        dataflows = self.list_dataflows()

        matches: List[DatasetMeta] = []

        for df in dataflows:
            if not df.get("dsd_id"):
                self.logger.debug("Dataflow %s sans DSD reference, ignore.", df["id"])
                continue

            self.logger.info(
                "Analyse du dataflow '%s' (%s)...", df["name"], df["id"]
            )

            dsd_root = self._fetch_datastructure(
                df["dsd_id"], df["dsd_agency"], df.get("dsd_version", "latest")
            )
            if dsd_root is None:
                continue

            dim_to_codelist = self._extract_dimension_ids(dsd_root)
            codelists = self._extract_codelists(dsd_root)
            dimension_order = self._extract_dimension_order(dsd_root)

            geo_dim = self._first_matching_dimension(dim_to_codelist, GEO_DIMENSION_CANDIDATES)
            if not geo_dim:
                self.logger.debug(
                    "Aucune dimension geographique reconnue pour %s, ignore.", df["id"]
                )
                continue

            geo_codelist_id = dim_to_codelist.get(geo_dim)
            geo_codes = codelists.get(geo_codelist_id, {})

            if country_code not in geo_codes:
                self.logger.debug(
                    "%s n'est pas couvert par le dataflow %s.", country_code, df["id"]
                )
                continue

            self.logger.info(
                "-> %s contient des donnees pour %s.", df["id"], country_code
            )

            indicator_dim = self._first_matching_dimension(
                dim_to_codelist, INDICATOR_DIMENSION_CANDIDATES
            )
            frequency_dim = self._first_matching_dimension(
                dim_to_codelist, FREQUENCY_DIMENSION_CANDIDATES
            )

            indicators: List[IndicatorMeta] = []
            if indicator_dim:
                indicator_codelist = codelists.get(dim_to_codelist.get(indicator_dim, ""), {})
                for code, label in indicator_codelist.items():
                    indicators.append(IndicatorMeta(code=code, name=label))
            else:
                # Pas de dimension "indicateur" dediee : le dataflow lui-meme
                # est considere comme portant un indicateur unique.
                indicators.append(IndicatorMeta(code=df["id"], name=df["name"]))

            dataset_meta = DatasetMeta(
                dataflow_id=df["id"],
                agency_id=df["agency_id"],
                version=df["version"],
                name=df["name"],
                description=f"Dataflow OCDE '{df['name']}' incluant des donnees pour le Burkina Faso.",
                source="OCDE (Organisation de Cooperation et de Developpement Economiques)",
                dsd_id=df["dsd_id"],
                geo_dimension=geo_dim,
                indicator_dimension=indicator_dim,
                frequency_dimension=frequency_dim,
                indicators=indicators,
                dimension_order=dimension_order,
            )
            matches.append(dataset_meta)

        self.logger.info(
            "%d dataflow(s) identifie(s) comme couvrant %s.", len(matches), country_code
        )
        return matches
