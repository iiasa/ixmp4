import os

from dotenv import load_dotenv

from ixmp4.conf.settings import Settings

load_dotenv()
# strict typechecking fails due to a bug
# https://docs.pydantic.dev/visual_studio_code/#adding-a-default-with-field
settings = Settings()  # type: ignore
access_file = os.getenv("IXMP4_ACCESS_LOG")
debug_file = os.getenv("IXMP4_DEBUG_LOG")
error_file = os.getenv("IXMP4_ERROR_LOG")
