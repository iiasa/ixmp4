from ixmp4.cli import app
from ixmp4.conf.settingsmodel import Settings

if __name__ == "__main__":
    settings = Settings()
    settings.configure_logging(settings.mode)

    app()
