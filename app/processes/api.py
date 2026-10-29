import uvicorn

from app.core.config import get_settings
from app.processes.shutdown import watch_shutdown


def main() -> None:
    settings = get_settings()
    server = uvicorn.Server(
        uvicorn.Config(
            "app.main:app",
            host=settings.api_host,
            port=settings.api_port,
            log_level=settings.log_level.lower(),
        )
    )
    watch_shutdown(lambda: setattr(server, "should_exit", True))
    server.run()


if __name__ == "__main__":
    main()
