import os

### Log Store 
HOST = os.environ.get("PYDAG_LOG_STORE_HOST", "")
PASSWORD = os.environ.get("PYDAG_LOG_STORE_PASSWORD", "")

if HOST == "" or PASSWORD == "":
    raise Exception(
        f"""Please set `PYDAG_LOG_STORE_HOST` and `PYDAG_LOG_STORE_PASSWORD` envrionment varible,
                        `PYDAG_LOG_STORE_HOST` is a redis connection host and `PYDAG_LOG_STORE_PASSWORD`is a redis connection password"""
    )

TO_RUN_NEW = os.environ.get("PYDAG_RUN_NEW", "yes").lower().strip()