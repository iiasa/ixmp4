from uvicorn.workers import UvicornWorker as DefaultUvicornWorker


class UvicornWorker(DefaultUvicornWorker):
    CONFIG_KWARGS = {
        "loop": "auto",
        "http": "auto",
    }
