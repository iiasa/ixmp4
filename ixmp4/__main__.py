from ixmp4.cli import app
from ixmp4.conf import settings

if __name__ == "__main__":
    settings.configure_logging(settings.mode)

    app()
