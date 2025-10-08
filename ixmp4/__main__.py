from ixmp4.rewrite.cli import app
from ixmp4.rewrite.conf import settings

if __name__ == "__main__":
    settings.configure_logging(settings.mode)

    app()
