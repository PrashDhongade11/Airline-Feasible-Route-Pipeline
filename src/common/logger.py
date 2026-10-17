import logging


def get_logger(name: str) -> logging.Logger:
    """Configures and returns a structured logger for pipeline modules."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    return logging.getLogger(name)