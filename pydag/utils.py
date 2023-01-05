import os
import time
import logging
import networkx as nx
import matplotlib.pyplot as plt
import platform
import subprocess
import redis 
import pickle
import re
from plan import Plan
from enum import Enum
from rich import print as rprint
from rich.logging import RichHandler
from .exceptions import PyDagException
from .environments import ADD_SUDO_BOOL, HOST, PASSWORD, PREFIX

class TaskStatus(Enum):
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"
    PENDING = "pending"


def draw_graph(g, name):
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
    plt.figure(1, figsize=(10, 10))
    pos = nx.nx_pydot.pydot_layout(g, prog="dot")
    nx.draw(g, pos, **options)
    plt.savefig(f"{name}.png")


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


def compose_command(file: str, default_command_map={}, add_sudo=True) -> str:
    file_abpath = os.path.join(os.path.abspath(os.path.curdir), file)

    if not os.path.exists(file_abpath):
        raise PyDagException(f"file `{file}` not exists in curent directory")

    try:
        suffix = file.split(".")[-1]
    except:
        raise PyDagException(
            f"Wrong file name `{file}`, proper file name should have suffix, e.g, `.py`, `.ipynb`, etc"
        )
    
    if default_command_map != {}:
        try:
            command_for_file = default_command_map[suffix]
        except KeyError:
            raise PyDagException(f"No command for file type:`{suffix}` if found")
    else:
        command_for_file = get_command_by_suffix(suffix, add_sudo)

    if callable(command_for_file):
        return command_for_file(file_abpath)
    else:
        raise PyDagException(f"Command for file type:`{suffix}` is not callable, command must be callable with one argument `file`")

def compose_command_for_job(file, to_run_new: bool, add_sudo:bool) -> str:
    if to_run_new:
        return (
            get_enviroment_set_command_by_platform("PYDAG_RUN_NEW", "yes")
            + " && "
            + compose_command(file, add_sudo=add_sudo)
        )
    else:
        return (
            get_enviroment_set_command_by_platform("PYDAG_RUN_NEW", "no")
            + " && "
            + compose_command(file, add_sudo=add_sudo)
        )


def get_command_by_suffix(suffix: str, add_sudo):  # return a function, take file as its parameter
    if suffix == "py":
        command = os.environ.get("PYDAG_PYTHON_COMMAND", "")
        if command == "":
            command =  get_default_executable("python", add_sudo=add_sudo)  
        return lambda file: command + " " + file

    # Jupyter notebook is little bit different
    # command neet to be a function, take file as its parameter
    elif suffix == "ipynb":
        command = os.environ.get("PYDAG_JUPYTERNB_COMMAND", "")
        if command == "":
            command =  get_default_jupyternb_executable("jupyter", add_sudo=ADD_SUDO_BOOL)
        return lambda file: command + " " + file + " --stdout"

    else:
        raise PyDagException(f"File with subfix of `{suffix}` is not supported yet")


def get_default_executable(command: str, add_sudo=False):
    platform_name = platform.system()
    if platform_name == "Windows":
        executable = subprocess.check_output(["where", command]).decode("utf-8").strip()
        print(executable)
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

    executable_jn = executable + " nbconvert --execute --to notebook"
    if add_sudo:
        return "sudo" + " " + executable_jn
    else:
        return executable_jn


def get_enviroment_set_command_by_platform(varible: str, value):
    platform_name = platform.system()
    if platform_name == "Windows":
        return f"set {varible}={value}"
    elif platform_name == "Linux":
        return f"export {varible}={value}"
    else:
        raise PyDagException(f"Platform `{platform_name}` is not supported yet")


def timestamp_to_datetime(job_run_at: str) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(job_run_at)))


def list_pipelines(job_name=None):
    _store = redis.Redis(host=HOST, password=PASSWORD, db=3)
    if job_name == None:
        pattern = PREFIX + "*META"  # skip meta
        job_keys = _store.keys(pattern=pattern)

        jobs = []

        for job_key in job_keys:
            name = job_key.decode().split(":")[1]
            jobs.append(name)

        planned_jobs = get_all_planned_cron_jobs()

        if jobs == []:
            rprint("[red b]No jobs found")
        else:
            for job in jobs:
                if job not in planned_jobs:
                    rprint(f"[blue b]{job}")
                else:
                    rprint(f"[green b]*{job}")
            return jobs 
    else:
        job_meta = _store.get(":".join([PREFIX, job_name, "META"]))

        if not job_meta:
            rprint(f"[red b]\[Pydag]There are no job named `{job_name}`")
            return

        job_graph = pickle.loads(job_meta)
        tasks = job_graph.nodes
        print("\n".join(tasks))
        return tasks



def get_all_planned_cron_jobs():
    cron = Plan()
    cron_raw = cron.read_crontab()
    return re.findall("# Begin Plan generated jobs for: (.*?)\n", cron_raw)
