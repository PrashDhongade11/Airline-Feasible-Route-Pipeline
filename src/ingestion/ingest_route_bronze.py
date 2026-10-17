"""
Phase 1.5: OpenSky Bronze Ingestion

Purpose:
    Fetch raw state-vector data from OpenSky and store it locally
    without modifying the source payload.

Bronze principles:
    - Preserve the exact API response bytes.
    - Do not transform source data.
    - Retry transient API failures.
    - Respect OpenSky rate limits.
    - Record ingestion metadata separately.
    - Use atomic local writes.
    - Generate a unique batch ID for every execution.

Location:
    src/etl/opensky_bronze_ingestion.py
"""

import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, TypedDict

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

load_dotenv()

OPENSKY_URL = "https://opensky-network.org/api/states/all"

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)

BASE_OUTPUT_DIR = "data/bronze/opensky"

REQUEST_TIMEOUT: Tuple[int, int] = (5, 30)

MAX_RETRIES = 3

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}

RETRY_BACKOFF_SECONDS = 2

TOKEN_REFRESH_MARGIN_SECONDS = 30


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("OpenSkyBronzeIngestion")


# ---------------------------------------------------------
# Exceptions & Types
# ---------------------------------------------------------

class OpenSkyIngestionError(Exception):
    """Raised when OpenSky Bronze ingestion fails."""
    pass


class OpenSkyFetchResult(TypedDict):
    raw_bytes: bytes
    parsed_json: Dict[str, Any]
    http_status: int
    response_headers: Dict[str, str]


# ---------------------------------------------------------
# OAuth2 token manager
# ---------------------------------------------------------

class OpenSkyTokenManager:
    """
    Handles OpenSky OAuth2 client-credentials authentication.

    The access token is cached and refreshed before expiry.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret

        self.access_token: Optional[str] = None
        self.expires_at: Optional[float] = None

    def get_access_token(self) -> str:
        """
        Return a valid access token.

        Refreshes the token if:
        - no token exists
        - token has expired
        - token is close to expiry
        """

        now = time.time()

        if (
            self.access_token is not None
            and self.expires_at is not None
            and now < self.expires_at
        ):
            return self.access_token

        logger.info("Requesting new OpenSky OAuth2 access token.")

        try:
            response = requests.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            token_data = response.json()

            access_token = token_data.get("access_token")

            if not isinstance(access_token, str) or not access_token:
                raise OpenSkyIngestionError(
                    "OpenSky authentication response did not contain "
                    "a valid 'access_token'."
                )

            expires_in = int(
                token_data.get("expires_in", 1800)
            )

            self.access_token = access_token

            self.expires_at = (
                now
                + max(
                    expires_in - TOKEN_REFRESH_MARGIN_SECONDS,
                    1,
                )
            )

            logger.info(
                "OpenSky OAuth2 token obtained successfully."
            )

            return self.access_token

        except requests.exceptions.Timeout as exc:
            raise OpenSkyIngestionError(
                "OpenSky authentication request timed out."
            ) from exc

        except requests.exceptions.RequestException as exc:
            raise OpenSkyIngestionError(
                f"OpenSky authentication request failed: {exc}"
            ) from exc

        except (ValueError, TypeError) as exc:
            raise OpenSkyIngestionError(
                "OpenSky authentication response was invalid JSON."
            ) from exc


# ---------------------------------------------------------
# OpenSky API request
# ---------------------------------------------------------

def fetch_raw_opensky_states(
    token_manager: Optional[OpenSkyTokenManager] = None,
) -> OpenSkyFetchResult:
    """
    Fetch OpenSky state vectors.

    Returns:
        OpenSkyFetchResult containing:
            - raw_bytes
            - parsed_json
            - http_status
            - response_headers
    """

    last_error: Exception = OpenSkyIngestionError("No request attempts performed.")

    for attempt in range(1, MAX_RETRIES + 1):

        logger.info(
            "OpenSky request attempt %d/%d.",
            attempt,
            MAX_RETRIES,
        )

        headers = {
            "Accept": "application/json",
        }

        if token_manager is not None:
            access_token = token_manager.get_access_token()

            headers["Authorization"] = (
                f"Bearer {access_token}"
            )

        try:
            response = requests.get(
                OPENSKY_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            http_status = response.status_code

            rate_limit_remaining = response.headers.get(
                "X-Rate-Limit-Remaining"
            )

            if rate_limit_remaining is not None:
                logger.info(
                    "OpenSky rate limit remaining: %s",
                    rate_limit_remaining,
                )

            # -------------------------------------------------
            # Successful response
            # -------------------------------------------------

            if response.status_code == 200:

                raw_bytes = response.content

                if not raw_bytes:
                    response.close()
                    raise OpenSkyIngestionError(
                        "OpenSky returned an empty HTTP response."
                    )

                try:
                    parsed_json = response.json()
                except ValueError as exc:
                    response.close()
                    raise OpenSkyIngestionError(
                        "OpenSky returned invalid JSON."
                    ) from exc

                validate_opensky_payload(parsed_json)

                state_count = len(
                    parsed_json.get("states") or []
                )

                logger.info(
                    "Successfully fetched %d state vectors.",
                    state_count,
                )

                return {
                    "raw_bytes": raw_bytes,
                    "parsed_json": parsed_json,
                    "http_status": http_status,
                    "response_headers": dict(response.headers),
                }

            # -------------------------------------------------
            # Token expired
            # -------------------------------------------------

            if response.status_code == 401 and token_manager:

                logger.warning(
                    "OpenSky OAuth2 token was rejected. "
                    "Refreshing token and retrying."
                )

                token_manager.access_token = None
                token_manager.expires_at = None

                last_error = OpenSkyIngestionError(
                    "OpenSky OAuth2 token expired or invalid."
                )
                response.close()
                continue

            # -------------------------------------------------
            # Rate limiting
            # -------------------------------------------------

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "X-Rate-Limit-Retry-After-Seconds"
                )

                if retry_after is not None:

                    try:
                        wait_seconds = int(retry_after)
                    except ValueError:
                        wait_seconds = (
                            RETRY_BACKOFF_SECONDS
                            * (2 ** (attempt - 1))
                        )
                else:
                    wait_seconds = (
                        RETRY_BACKOFF_SECONDS
                        * (2 ** (attempt - 1))
                    )

                logger.warning(
                    "OpenSky rate limit reached. "
                    "Waiting %d seconds before retry.",
                    wait_seconds,
                )

                last_error = OpenSkyIngestionError(
                    "OpenSky API rate limit reached."
                )

                response.close()
                if attempt < MAX_RETRIES:
                    time.sleep(wait_seconds)

                continue

            # -------------------------------------------------
            # Other retryable HTTP errors
            # -------------------------------------------------

            if response.status_code in RETRYABLE_STATUS_CODES:

                wait_seconds = (
                    RETRY_BACKOFF_SECONDS
                    * (2 ** (attempt - 1))
                )

                logger.warning(
                    "OpenSky returned HTTP %d. "
                    "Retrying after %d seconds.",
                    response.status_code,
                    wait_seconds,
                )

                last_error = OpenSkyIngestionError(
                    f"Retryable HTTP error: {response.status_code}"
                )

                response.close()
                if attempt < MAX_RETRIES:
                    time.sleep(wait_seconds)

                continue

            # -------------------------------------------------
            # Permanent HTTP error
            # -------------------------------------------------

            err_msg = response.text[:500]
            response.close()
            raise OpenSkyIngestionError(
                f"OpenSky returned non-retryable HTTP "
                f"status {http_status}: {err_msg}"
            )

        except requests.exceptions.Timeout as exc:

            last_error = OpenSkyIngestionError(
                f"OpenSky request timed out: {exc}"
            )

            logger.warning(
                "OpenSky request timed out on attempt %d/%d.",
                attempt,
                MAX_RETRIES,
            )

            if attempt < MAX_RETRIES:

                wait_seconds = (
                    RETRY_BACKOFF_SECONDS
                    * (2 ** (attempt - 1))
                )

                time.sleep(wait_seconds)

        except requests.exceptions.ConnectionError as exc:

            last_error = OpenSkyIngestionError(
                f"OpenSky connection failure: {exc}"
            )

            logger.warning(
                "OpenSky connection failure on attempt %d/%d.",
                attempt,
                MAX_RETRIES,
            )

            if attempt < MAX_RETRIES:

                wait_seconds = (
                    RETRY_BACKOFF_SECONDS
                    * (2 ** (attempt - 1))
                )

                time.sleep(wait_seconds)

        except requests.exceptions.RequestException as exc:

            raise OpenSkyIngestionError(
                f"Unexpected OpenSky request failure: {exc}"
            ) from exc

    raise OpenSkyIngestionError(
        f"OpenSky ingestion failed after "
        f"{MAX_RETRIES} attempts: {last_error}"
    )


# ---------------------------------------------------------
# Payload validation
# ---------------------------------------------------------

def validate_opensky_payload(
    payload: Any,
) -> None:
    """
    Perform minimal Bronze-level validation.

    This function validates the envelope only.
    It does NOT transform or modify individual state vectors.
    """

    if not isinstance(payload, dict):

        raise OpenSkyIngestionError(
            "OpenSky payload is not a JSON object."
        )

    if "time" not in payload:

        raise OpenSkyIngestionError(
            "OpenSky payload is missing 'time'."
        )

    if "states" not in payload:

        raise OpenSkyIngestionError(
            "OpenSky payload is missing 'states'."
        )

    states = payload["states"]

    if states is not None and not isinstance(states, list):

        raise OpenSkyIngestionError(
            "OpenSky 'states' field must be a list or null."
        )


# ---------------------------------------------------------
# Atomic raw payload writer
# ---------------------------------------------------------

def write_raw_bytes_atomic(
    raw_bytes: bytes,
    target_path: Path,
) -> None:
    """
    Atomically write exact source bytes to local storage.
    """

    target_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path: Optional[Path] = None

    try:

        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=target_path.parent,
            prefix=".tmp_",
            delete=False,
        ) as temp_file:

            temp_path = Path(temp_file.name)

            temp_file.write(raw_bytes)
            temp_file.flush()

            os.fsync(temp_file.fileno())

        # File handle closed here via context manager before replacing
        temp_path.replace(target_path)

    except Exception as exc:

        if temp_path and temp_path.exists():

            try:
                temp_path.unlink()
            except OSError:
                logger.warning(
                    "Could not remove temporary file: %s",
                    temp_path,
                )

        raise OpenSkyIngestionError(
            f"Failed to write Bronze payload: {exc}"
        ) from exc


# ---------------------------------------------------------
# Metadata writer
# ---------------------------------------------------------

def write_metadata_atomic(
    metadata: Dict[str, Any],
    target_path: Path,
) -> None:
    """
    Write ingestion metadata separately from the raw payload.
    """

    target_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path: Optional[Path] = None

    try:

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target_path.parent,
            prefix=".tmp_",
            delete=False,
        ) as temp_file:

            temp_path = Path(temp_file.name)

            json.dump(
                metadata,
                temp_file,
                ensure_ascii=False,
                indent=2,
            )

            temp_file.write("\n")

            temp_file.flush()
            os.fsync(temp_file.fileno())

        # File handle closed here via context manager before replacing
        temp_path.replace(target_path)

    except Exception as exc:

        if temp_path and temp_path.exists():

            try:
                temp_path.unlink()
            except OSError:
                logger.warning(
                    "Could not remove temporary metadata file: %s",
                    temp_path,
                )

        raise OpenSkyIngestionError(
            f"Failed to write Bronze metadata: {exc}"
        ) from exc


# ---------------------------------------------------------
# Bronze ingestion
# ---------------------------------------------------------

def run_opensky_ingestion() -> str:

    batch_id = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%S%fZ")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    ingestion_time = datetime.now(timezone.utc)

    logger.info(
        "Starting OpenSky Bronze ingestion. batch_id=%s",
        batch_id,
    )

    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    token_manager: Optional[OpenSkyTokenManager] = None

    if client_id and client_secret:

        token_manager = OpenSkyTokenManager(
            client_id=client_id,
            client_secret=client_secret,
        )

        logger.info(
            "OpenSky authentication configured."
        )

    else:

        logger.warning(
            "OpenSky OAuth2 credentials not configured. "
            "Using anonymous access."
        )

    # -----------------------------------------------------
    # Fetch source data
    # -----------------------------------------------------

    result = fetch_raw_opensky_states(
        token_manager=token_manager,
    )

    raw_bytes = result["raw_bytes"]
    parsed_json = result["parsed_json"]

    # -----------------------------------------------------
    # Calculate source integrity hash
    # -----------------------------------------------------

    sha256_hash = hashlib.sha256(
        raw_bytes
    ).hexdigest()

    state_count = len(
        parsed_json.get("states") or []
    )

    # -----------------------------------------------------
    # Create partition path
    # -----------------------------------------------------

    year = ingestion_time.strftime("%Y")
    month = ingestion_time.strftime("%m")
    day = ingestion_time.strftime("%d")

    partition_path = (
        Path(BASE_OUTPUT_DIR)
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
    )

    # -----------------------------------------------------
    # Unique filenames
    # -----------------------------------------------------

    filename = (
        f"opensky_states_{batch_id}.json"
    )

    metadata_filename = (
        f"opensky_states_{batch_id}.metadata.json"
    )

    raw_path = partition_path / filename

    metadata_path = (
        partition_path / metadata_filename
    )

    # -----------------------------------------------------
    # Write exact source payload
    # -----------------------------------------------------

    write_raw_bytes_atomic(
        raw_bytes=raw_bytes,
        target_path=raw_path,
    )

    # -----------------------------------------------------
    # Write ingestion metadata
    # -----------------------------------------------------

    metadata = {
        "batch_id": batch_id,
        "source": "opensky",
        "endpoint": OPENSKY_URL,
        "ingestion_status": "SUCCESS",
        "ingested_at_utc": ingestion_time.isoformat(),
        "http_status": result["http_status"],
        "response_size_bytes": len(raw_bytes),
        "sha256": sha256_hash,
        "state_count": state_count,
        "authenticated": token_manager is not None,
        "raw_payload_file": raw_path.name,
    }

    write_metadata_atomic(
        metadata=metadata,
        target_path=metadata_path,
    )

    logger.info(
        "OpenSky Bronze ingestion completed successfully."
    )

    logger.info(
        "Raw payload: %s",
        raw_path,
    )

    logger.info(
        "Metadata: %s",
        metadata_path,
    )

    return str(raw_path)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":

    try:

        run_opensky_ingestion()

    except OpenSkyIngestionError as exc:

        logger.error(
            "OpenSky Bronze ingestion failed: %s",
            exc,
        )

        raise