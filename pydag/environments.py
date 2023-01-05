import os
from .exceptions import PyDagException

# Meta
PREFIX = os.environ.get("PYDAG_LOG_PREFIX", "Pydag")
QueueBlockTime = os.environ.get("PYDAG_ROUNDS_TIME", "10")
QueueBlockTime = int(QueueBlockTime)

# Log Store
HOST = os.environ.get("PYDAG_LOG_STORE_HOST", "")
PASSWORD = os.environ.get("PYDAG_LOG_STORE_PASSWORD", "")


# Other Settings
TO_RUN_NEW = os.environ.get("PYDAG_RUN_NEW", "yes").lower().strip()
TASK_TIMEOUT = int(os.environ.get("PYDAG_TASK_TIMEOUT", 3600))

ADD_SUDO = str(os.environ.get("PYDAG_ADD_SUDO", "no"))
ADD_SUDO_BOOL = True

if ADD_SUDO.lower() != "yes":
    ADD_SUDO_BOOL = False
