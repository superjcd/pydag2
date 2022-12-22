import os
import time
import logging
import networkx as nx
import matplotlib.pyplot as plt
import platform
import subprocess
from enum import Enum
from rich.logging import RichHandler
from .exceptions import PyDagException


class TaskStatus(Enum):
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"
    PENDING = "pending"


def draw_graph(g):
    options = {
        "font_size": 10,
        "node_size": 2000,
        "node_color": "#ff8c00",
        "edgecolors": "black",
        "linewidths": 1,
        "width": 2,
        "with_labels": True,
        "alpha": 0.8,
    }
    plt.figure(1, figsize=(12, 12))
    pos = nx.nx_pydot.pydot_layout(g, prog="dot")
    nx.draw(g, pos, **options)
    # plt.show()


def prepare_rich_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    rh = RichHandler()
    rh.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("%(message)s")

    # add formatter to rh
    rh.setFormatter(formatter)

    # add rh to logger
    logger.addHandler(rh)

    return logger


def compose_task_name(job_name: str, task_name: str) -> str:
    name = f"{job_name}:{task_name}"
    if len(name) > 32:
        raise ValueError(
            "Task name too long! The length of `job_name` plus the length of `task_name` must within `31`"
        )
    return name


def compose_command(file: str) -> str:
    file_abpath = os.path.join(os.path.abspath(os.path.curdir), file)

    if not os.path.exists(file_abpath):
        raise PyDagException(f"file `{file}` not exists in curent directory")

    try:
        suffix = file.split(".")[-1]
    except:
        raise PyDagException(
            f"Wrong file name `{file}`, proper file name should have suffix, e.g, `.py`, `.ipynb`, etc"
        )

    command_for_file = get_command_by_suffix(suffix)

    return command_for_file + " " + file_abpath


def compose_command_for_job(file, to_run_new: bool) -> str:
    if to_run_new:
        return (
            get_enviroment_set_command_by_platform("PYDAG_RUN_NEW", "yes")
            + " && "
            + compose_command(file)
        )
    else:
        return (
            get_enviroment_set_command_by_platform("PYDAG_RUN_NEW", "no")
            + " && "
            + compose_command(file)
        )


def get_command_by_suffix(suffix: str):
    if suffix == "py":
        command = os.environ.get("PYDAG_PYTHON_COMMAND", "")
        if command == "":
            return get_default_executable("python", True)
        return command

    elif suffix == "ipynb":
        command = os.environ.get("PYDAG_JUPYTERNB_COMMAND", "")
        if command == "":
            return get_default_jupyternb_executable("jupyter", True)
        return command


def get_default_executable(command: str, add_sudo=False):
    platform_name = platform.system()
    if platform_name == "Windows":
        executable = subprocess.check_output(["where", command]).decode("utf-8").strip()
    elif platform_name == "Linux":
        executable = subprocess.check_output(["which", command]).decode("utf-8").strip()
    else:
        raise PyDagException(f"Platform `{platform_name}` is not supported yet")
    if add_sudo:
        return "sudo" + " " + executable
    else:
        return executable


def get_default_jupyternb_executable(command: str, add_sudo=False):
    platform_name = platform.system()
    if platform_name == "Windows":
        executable = subprocess.check_output(["where", command]).decode("utf-8").strip()
    elif platform_name == "Linux":
        executable = subprocess.check_output(["which", command]).decode("utf-8").strip()
    else:
        raise PyDagException(f"Platform `{platform_name}` is not supported yet")
    if add_sudo:
        return "sudo" + " " + executable + " " + "run"
    else:
        return executable + " " + "run"


def get_enviroment_set_command_by_platform(varible: str, value):
    platform_name = platform.system()
    if platform_name == "Windows":
        return f"set {varible}={value}"
    elif platform_name == "Linux":
        return f"export {varible}={value}"
    else:
        raise PyDagException(f"Platform `{platform_name}` is not supported yet")


def timestamp_to_datetime(job_run_at:str)->str:
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(job_run_at)))