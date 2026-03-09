from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_file: Path | None = None, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("autotag")
    logger.setLevel(level.upper())
    logger.handlers.clear()

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
