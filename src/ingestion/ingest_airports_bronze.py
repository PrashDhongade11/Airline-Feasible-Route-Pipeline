"""Bronze ingestion for raw airport reference data."""

import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("airport_bronze_ingestion")

# OpenFlights raw airport dataset
OPENFLIGHTS_AIRPORTS_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
)


def get_configured_session(
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (
        500,
        502,
        503,
        504,
    ),
) -> requests.Session:
    """Create HTTP session with retry and backoff handling."""
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def fetch_openflights_airports(
    session: requests.Session,
    url: str = OPENFLIGHTS_AIRPORTS_URL,
    timeout: tuple[float, float] = (5.0, 15.0),
) -> dict[str, Any]:
    """Fetch and preserve the raw OpenFlights payload without parsing it."""
    ingested_at = datetime.now(timezone.utc).isoformat()

    base_record: dict[str, Any] = {
        "source": "openflights",
        "source_url": url,
        "ingested_at_utc": ingested_at,
        "ingestion_status": "FAILED",
        "raw_content": None,
        "raw_content_bytes": None,
        "content_sha256": None,
        "error_message": None,
    }

    try:
        response = session.get(
            url,
            timeout=timeout,
        )

        if response.status_code != 200:
            base_record["error_message"] = (
                f"HTTP {response.status_code}: {response.text}"
            )

            logger.warning(
                "OpenFlights returned HTTP status %s",
                response.status_code,
            )

            return base_record

        # Treat an empty successful response as an ingestion failure.
        if not response.content:
            base_record["error_message"] = (
                "HTTP 200 received but response payload is empty."
            )

            logger.error("OpenFlights returned an empty payload.")

            return base_record

        # Preserve source payload without parsing or transforming records.
        raw_content = response.text

        base_record["ingestion_status"] = "SUCCESS"
        base_record["raw_content"] = raw_content
        base_record["raw_content_bytes"] = len(response.content)
        base_record["content_sha256"] = hashlib.sha256(
            response.content
        ).hexdigest()

        logger.info(
            "Successfully fetched OpenFlights dataset: %d bytes",
            len(response.content),
        )

        return base_record

    except requests.exceptions.RequestException as err:
        base_record["error_message"] = f"Network or Connection Error: {err}"

        logger.error(
            "Failed to reach OpenFlights dataset: %s",
            err,
        )

        return base_record


def write_jsonl_atomic(
    record: dict[str, Any],
    target_filepath: str,
) -> None:
    """Atomically write one raw Bronze record."""
    target_dir = os.path.dirname(target_filepath)

    if target_dir:
        os.makedirs(
            target_dir,
            exist_ok=True,
        )

    temp_fd, temp_path = tempfile.mkstemp(
        dir=target_dir,
        suffix=".tmp",
    )

    try:
        with os.fdopen(
            temp_fd,
            "w",
            encoding="utf-8",
        ) as file:

            serialized = json.dumps(
                record,
                ensure_ascii=False,
                default=str,
            )

            file.write(serialized + "\n")

            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temp_path,
            target_filepath,
        )

        logger.info(
            "Successfully wrote Bronze airport payload to %s",
            target_filepath,
        )

    except Exception:
        logger.exception(
            "Failed atomic write operation to %s",
            target_filepath,
        )

        if os.path.exists(temp_path):
            os.remove(temp_path)

        raise


def run_ingestion(
    output_filepath: Optional[str] = None,
) -> str:
    """Run raw airport Bronze ingestion."""
    session = get_configured_session()

    if output_filepath is None:
        execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")

        output_filepath = (
            "data/bronze/airports/" f"airports_raw_{execution_date}.jsonl"
        )

    logger.info("Starting OpenFlights Airport Bronze Ingestion.")

    record = fetch_openflights_airports(
        session=session,
    )

    write_jsonl_atomic(
        record=record,
        target_filepath=output_filepath,
    )

    if record["ingestion_status"] != "SUCCESS":
        raise RuntimeError(
            "OpenFlights airport ingestion failed. "
            f"Bronze audit record written to {output_filepath}. "
            f"Error: {record['error_message']}"
        )

    return output_filepath


if __name__ == "__main__":
    run_ingestion()