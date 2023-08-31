from dotenv import load_dotenv

from ixmp4.conf.settings import Settings

load_dotenv()
# strict typechecking fails due to a bug
# https://docs.pydantic.dev/visual_studio_code/#adding-a-default-with-field
settings = Settings()  # type: ignore
