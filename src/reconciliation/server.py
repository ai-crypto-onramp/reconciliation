"""uvicorn entrypoint: ``python -m reconciliation.server``."""

from __future__ import annotations

import logging

from .app import create_app
from .config import get_settings


def main() -> None:
    """Run the reconciliation service with uvicorn."""
    import uvicorn

    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_level=settings.log_level)


if __name__ == "__main__":
    main()
