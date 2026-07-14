"""
client.py
=========
Client HTTP bas niveau pour l'API REST/JSON de la Banque mondiale
(https://api.worldbank.org/v2).

Contrairement a l'API SDMX de l'OCDE, l'API Banque mondiale :
  - repond en JSON par defaut (pas de negociation de format complexe) ;
  - retourne toujours une liste a deux elements en cas de succes :
        [ {metadonnees de pagination}, [ {observation}, ... ] ]
    et une liste a un seul element en cas d'erreur :
        [ {"message": [{"id": ..., "key": ..., "value": ...}]} ]
  - ne publie pas de quota officiel strict, mais on applique tout de
    meme une limitation cote client par prudence (bonne citoyennete API,
    evite les blocages IP en cas de scan intensif du catalogue).

Ce module ne connait rien de la logique metier (indicateurs, pays...) :
il expose uniquement des primitives generiques d'appel a l'API.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

from config.config import Config
from logger import get_logger


class WorldBankAPIError(Exception):
    """Erreur levee lorsque l'API Banque mondiale renvoie une erreur/reponse invalide."""


class RateLimiter:
    """Limiteur de requetes en fenetre glissante (sliding window).

    Garantit qu'au plus `max_requests` appels sont effectues au cours des
    60 dernieres secondes. Si la limite est atteinte, `wait_if_needed()`
    met le thread en pause jusqu'a ce qu'un slot se libere.
    """

    def __init__(self, max_requests_per_minute: int) -> None:
        self.max_requests = max_requests_per_minute
        self.window_seconds = 60
        self._timestamps: deque[float] = deque()
        self.logger = get_logger()

    def wait_if_needed(self) -> None:
        now = time.monotonic()

        while self._timestamps and now - self._timestamps[0] > self.window_seconds:
            self._timestamps.popleft()

        if len(self._timestamps) >= self.max_requests:
            oldest = self._timestamps[0]
            sleep_time = self.window_seconds - (now - oldest) + 1
            if sleep_time > 0:
                self.logger.warning(
                    "Limite de %s requetes/minute atteinte. Pause de %.1f secondes...",
                    self.max_requests,
                    sleep_time,
                )
                time.sleep(sleep_time)

            now = time.monotonic()
            while self._timestamps and now - self._timestamps[0] > self.window_seconds:
                self._timestamps.popleft()

        self._timestamps.append(time.monotonic())


class WorldBankClient:
    """Client HTTP pour l'API REST de la Banque mondiale.

    Exemple :
        client = WorldBankClient(config)
        meta, data = client.get_json("/country/BFA/indicator/NY.GDP.MKTP.CD",
                                      params={"per_page": 1000, "date": "1960:2025"})
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.base_url = config.wb_api_base_url.rstrip("/")
        self.timeout = config.wb_api_timeout
        self.max_retries = config.wb_max_retries
        self.retry_backoff = config.wb_retry_backoff
        self.rate_limiter = RateLimiter(config.wb_max_requests_per_minute)
        self.logger = get_logger()

        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "User-Agent": "BF-Pulse-WorldBank-Connector/1.0 (+https://bfpulse.example)",
                "Accept": "application/json",
            }
        )

    def get_json(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """Execute un GET JSON vers l'API Banque mondiale avec retry + rate limiting.

        Args:
            path: chemin relatif (ex: "/country/BFA/indicator/NY.GDP.MKTP.CD")
                  ou URL absolue.
            params: parametres de requete additionnels (per_page, page, date...).
                    "format=json" est ajoute automatiquement.

        Returns:
            Tuple (metadata, data) : metadata est le dict de pagination
            ({"page":1,"pages":1,"per_page":1000,"total":N,...}) ou None si
            l'API n'en renvoie pas (cas de /indicator par exemple, ou le
            premier element EST les metadonnees) ; data est la liste des
            enregistrements (peut etre vide).

        Raises:
            WorldBankAPIError: apres epuisement des tentatives de retry,
                ou si l'API renvoie un message d'erreur explicite.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"

        query_params = dict(params or {})
        query_params.setdefault("format", "json")

        last_exception: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            self.rate_limiter.wait_if_needed()

            try:
                self.logger.debug(
                    "GET %s (tentative %d/%d) params=%s", url, attempt, self.max_retries, query_params
                )
                response = self.session.get(
                    url, params=query_params, timeout=self.timeout
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", self.retry_backoff))
                    self.logger.warning(
                        "HTTP 429 recu de l'API Banque mondiale. Pause de %s secondes.", retry_after
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code >= 500:
                    raise requests.exceptions.HTTPError(
                        f"Erreur serveur Banque mondiale {response.status_code}", response=response
                    )

                response.raise_for_status()

                try:
                    payload = response.json()
                except ValueError as exc:
                    raise WorldBankAPIError(
                        f"Reponse non-JSON recue depuis {url} : {exc}"
                    ) from exc

                self.logger.info(
                    "Requete OK : %s [%d] (%d octets)", url, response.status_code, len(response.content)
                )

                return self._unpack_payload(payload, url)

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exception = exc
                self.logger.warning(
                    "Erreur reseau sur %s (tentative %d/%d) : %s", url, attempt, self.max_retries, exc
                )
            except requests.exceptions.HTTPError as exc:
                last_exception = exc
                status = exc.response.status_code if exc.response is not None else "N/A"
                self.logger.warning(
                    "Erreur HTTP %s sur %s (tentative %d/%d) : %s",
                    status, url, attempt, self.max_retries, exc,
                )
                if exc.response is not None and 400 <= exc.response.status_code < 500:
                    if exc.response.status_code == 404:
                        raise WorldBankAPIError(f"Ressource introuvable (404) : {url}") from exc

            if attempt < self.max_retries:
                backoff = self.retry_backoff * (2 ** (attempt - 1))
                self.logger.info("Nouvelle tentative dans %s secondes...", backoff)
                time.sleep(backoff)

        raise WorldBankAPIError(
            f"Echec de la requete vers {url} apres {self.max_retries} tentatives : {last_exception}"
        )

    @staticmethod
    def _unpack_payload(
        payload: Any, url: str
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """Decompose la reponse JSON brute de la Banque mondiale.

        Formats possibles :
          - succes avec donnees : [metadata_dict, [obs, obs, ...]]
          - succes sans donnees : [metadata_dict, None]
          - erreur              : [{"message": [{"id": ..., "value": ...}]}]
        """
        if not isinstance(payload, list) or not payload:
            raise WorldBankAPIError(f"Format de reponse inattendu depuis {url} : {payload!r}")

        first = payload[0]

        if isinstance(first, dict) and "message" in first and len(payload) == 1:
            messages = first.get("message", [])
            detail = "; ".join(
                f"{m.get('id', '?')}: {m.get('value', m.get('key', ''))}" for m in messages
            ) or str(first)
            raise WorldBankAPIError(f"Erreur API Banque mondiale sur {url} : {detail}")

        metadata = first if isinstance(first, dict) else None
        data = payload[1] if len(payload) > 1 else []
        return metadata, (data or [])

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "WorldBankClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
