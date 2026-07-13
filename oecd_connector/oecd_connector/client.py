"""
client.py
=========
Client HTTP bas niveau pour l'API SDMX de l'OCDE.

Responsabilites :
  - construire et executer les appels HTTP vers l'API OCDE (SDMX REST) ;
  - gerer les erreurs HTTP/reseau de maniere robuste ;
  - retry automatique avec backoff exponentiel ;
  - limitation cote client du nombre de requetes (max N / heure), afin
    de respecter le quota impose par l'OCDE.

Ce module ne connait rien de la logique metier (dataflows, datasets...):
il expose uniquement des primitives generiques d'appel a l'API.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter

from config.config import Config
from logger import get_logger


class OECDAPIError(Exception):
    """Erreur levee lorsque l'API OCDE renvoie une reponse invalide/erreur."""


class RateLimitExceededError(Exception):
    """Erreur interne : ne devrait jamais remonter a l'appelant (on attend
    plutot que d'echouer), conservee pour un usage explicite eventuel."""


class RateLimiter:
    """Limiteur de requetes en fenetre glissante (sliding window).

    Garantit qu'au plus `max_requests` appels sont effectues au cours des
    60 dernieres minutes. Si la limite est atteinte, `wait_if_needed()`
    met le thread en pause jusqu'a ce qu'un slot se libere.
    """

    def __init__(self, max_requests_per_hour: int) -> None:
        self.max_requests = max_requests_per_hour
        self.window_seconds = 3600
        self._timestamps: deque[float] = deque()
        self.logger = get_logger()

    def wait_if_needed(self) -> None:
        now = time.monotonic()

        # Purge les horodatages sortis de la fenetre d'une heure
        while self._timestamps and now - self._timestamps[0] > self.window_seconds:
            self._timestamps.popleft()

        if len(self._timestamps) >= self.max_requests:
            oldest = self._timestamps[0]
            sleep_time = self.window_seconds - (now - oldest) + 1
            if sleep_time > 0:
                self.logger.warning(
                    "Limite de %s requetes/heure atteinte. Pause de %.1f secondes...",
                    self.max_requests,
                    sleep_time,
                )
                time.sleep(sleep_time)

            # Re-purge apres la pause
            now = time.monotonic()
            while self._timestamps and now - self._timestamps[0] > self.window_seconds:
                self._timestamps.popleft()

        self._timestamps.append(time.monotonic())

    @property
    def remaining_requests(self) -> int:
        now = time.monotonic()
        while self._timestamps and now - self._timestamps[0] > self.window_seconds:
            self._timestamps.popleft()
        return max(0, self.max_requests - len(self._timestamps))


class OECDClient:
    """Client HTTP pour l'API SDMX de l'OCDE.

    Exemple :
        client = OECDClient(config)
        xml_text = client.get("/dataflow/OECD/all/latest", headers={"Accept": "application/xml"})
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.base_url = config.oecd_api_base_url.rstrip("/")
        self.timeout = config.oecd_api_timeout
        self.max_retries = config.oecd_max_retries
        self.retry_backoff = config.oecd_retry_backoff
        self.rate_limiter = RateLimiter(config.oecd_max_requests_per_hour)
        self.logger = get_logger()

        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "User-Agent": "BF-Pulse-OECD-Connector/1.0 (+https://bfpulse.example)",
            }
        )

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        as_text: bool = True,
    ) -> Any:
        """Execute un GET vers l'API OCDE avec retry + rate limiting.

        Args:
            path: chemin relatif (ex: "/dataflow/OECD/all/latest") ou URL absolue.
            params: parametres de requete additionnels.
            headers: en-tetes HTTP additionnels (ex: Accept).
            as_text: si True, retourne le corps en texte brut ; sinon, l'objet
                Response complet est retourne (utile pour du contenu binaire).

        Returns:
            str ou requests.Response selon `as_text`.

        Raises:
            OECDAPIError: apres epuisement des tentatives de retry.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"

        last_exception: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            self.rate_limiter.wait_if_needed()

            try:
                self.logger.debug(
                    "GET %s (tentative %d/%d) params=%s",
                    url,
                    attempt,
                    self.max_retries,
                    params,
                )
                response = self.session.get(
                    url, params=params, headers=headers, timeout=self.timeout
                )

                if response.status_code == 429:
                    # Trop de requetes cote serveur malgre notre limiteur local
                    retry_after = int(response.headers.get("Retry-After", self.retry_backoff))
                    self.logger.warning(
                        "HTTP 429 recu de l'API OCDE. Pause de %s secondes.", retry_after
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code >= 500:
                    raise requests.exceptions.HTTPError(
                        f"Erreur serveur OCDE {response.status_code}", response=response
                    )

                response.raise_for_status()

                self.logger.info(
                    "Requete OK : %s [%d] (%d octets)",
                    url,
                    response.status_code,
                    len(response.content),
                )
                return response.text if as_text else response

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exception = exc
                self.logger.warning(
                    "Erreur reseau sur %s (tentative %d/%d) : %s",
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                )
            except requests.exceptions.HTTPError as exc:
                last_exception = exc
                status = exc.response.status_code if exc.response is not None else "N/A"
                self.logger.warning(
                    "Erreur HTTP %s sur %s (tentative %d/%d) : %s",
                    status,
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                )
                # Les erreurs 4xx (hors 429) sont generalement definitives :
                # inutile de reessayer un dataflow inexistant (404) par exemple.
                if exc.response is not None and 400 <= exc.response.status_code < 500:
                    if exc.response.status_code == 404:
                        raise OECDAPIError(f"Ressource introuvable (404) : {url}") from exc

            if attempt < self.max_retries:
                backoff = self.retry_backoff * (2 ** (attempt - 1))
                self.logger.info("Nouvelle tentative dans %s secondes...", backoff)
                time.sleep(backoff)

        raise OECDAPIError(
            f"Echec de la requete vers {url} apres {self.max_retries} tentatives : {last_exception}"
        )

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "OECDClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
