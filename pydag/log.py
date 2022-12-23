import redis
import pickle
from rich.tree import Tree
from rich import print as rprint
import pandas as pd
from dataclasses import dataclass
from networkx import DiGraph
from pygocron.pygocron import PyGoCron
from .environments import HOST, PASSWORD
from .utils import timestamp_to_datetime

PREFIX = "Pydag"


@dataclass
class TaskRecord:
    job_name: str
    job_run_at: int
    task_name: str
    run_id: int
    task_id: int
    status: str
    record_at: int

    def compose_key(self):
        return ":".join([PREFIX, self.job_name, str(self.job_run_at), self.task_name])

    def compose_value(self):
        return ",".join(
            [str(self.run_id), str(self.task_id), self.status, str(self.record_at)]
        )


class BasicJobLogger:
    def __init__(self):
        self._store = redis.Redis(host=HOST, password=PASSWORD, db=3)

    def record_job_info(self, job_name, dg: DiGraph):
        _key = ":".join([PREFIX, job_name, "META"])
        dg_picked = pickle.dumps(dg)
        if self._store.get(_key) != None:
            return

        self._store.set(_key, dg_picked)

    def record_task_info(
        self,
        job_name: str,
        job_run_at: int,
        task_name: str,
        run_id: int,
        task_id: int,
        status: str,
        task_record_at: int,
    ):
        record = TaskRecord(
            job_name, job_run_at, task_name, run_id, task_id, status, task_record_at
        )
        self._store.set(record.compose_key(), record.compose_value())

    def log_job(self, job_name, latest_n=3):
        pattern = ":".join([PREFIX, job_name]) + ":[1-9]*"  # skip meta
        task_keys = self._store.keys(pattern=pattern)
        job_meta = self._store.get(":".join([PREFIX, job_name, "META"]))
        if not job_meta:
            rprint(f"[red b]\[Pydag]There are no job named `{job_name}`")
            return

        job_graph = pickle.loads(job_meta)

        records = []

        for task in task_keys:
            keys = task.decode().split(":")
            values = self._store.get(task).decode().split(",")
            records.append(keys + values)

        record_df = pd.DataFrame(records)
        if record_df.empty:
            rprint(f"[red b]\[Pydag]No logs for job `{job_name}`")
            return
        record_df = pd.DataFrame(records).loc[:, 1:]
        record_df.columns = [
            "job_name",
            "job_run_at",
            "task_name",
            "run_id",
            "task_id",
            "status",
            "task_record_at",
        ]
        record_df = record_df.sort_values(
            ["job_run_at", "task_record_at"], ascending=[False, True]
        )
        job_run_ats = record_df["job_run_at"].unique()[:latest_n]
        record_df = record_df[record_df["job_run_at"].isin(job_run_ats)]
        log_groups = record_df.groupby("job_run_at")
        log_groups = sorted(log_groups, reverse=True)

        print_a_log_title(f"Log For Job `{job_name}`")
        for n, (job_run_at, df) in enumerate(log_groups):
            task_records = df.to_dict(orient="records")
            display_job_run(job_name, job_run_at, task_records, job_graph)
            if n != len(log_groups) - 1:
                print_a_log_sep()
        print_a_log_end()

    def log_task(self, job_name, task_name, latest_n=1):
        pyg = PyGoCron()
        try:
            task_id = pyg.get_task_id_by_name(":".join([job_name, task_name]))
        except:
            rprint(f"[red b]Error: Can not find the task `{task_name}`")
            return
        task_log = pyg.get_task_logs(task_id=task_id, page_size=latest_n)
        total = task_log["total"]
        print_a_log_title(f"Log For Task `{task_name}`")
        if total >= 1:
            logs = task_log["data"]
            for n, log in enumerate(logs):
                print(
                    f"`{log['name']}` at {log['end_time'].split('+')[0].replace('T', ' ')}"
                )
                print(log["result"].strip())
                if n != len(logs) - 1:
                    print_a_log_sep()
        print_a_log_end()

    def clear_log(self, job_name):
        """
        Clear all log and job meta info related to the job
        """

        def delete_keys(items):
            pipeline = self._store.pipeline()
            for item in items:
                pipeline.delete(item)
            pipeline.execute()

        pattern = ":".join([PREFIX, job_name]) + "*"
        task_keys = self._store.keys(pattern=pattern)
        delete_keys(task_keys)


def display_job_run(job_name, job_run_at, tasks, job_graph, method="dependant"):
    job_run_at = timestamp_to_datetime(job_run_at)
    tree = Tree(f"`{job_name}` at {job_run_at}")

    root_task_name = [n for n, d in job_graph.in_degree() if d == 0][0]

    build_log_tree(tree, job_graph, root_task_name, tasks, method)

    rprint(tree)


def build_log_tree(tree, job_graph, root_task_name, tasks, method):
    if method == "dependant":
        build_dependant_log_tree(tree, job_graph, root_task_name, tasks)
    else:
        build_flatten_log_tree(tree, tasks)


def build_dependant_log_tree(tree, graph, root, tasks):
    try:
        task_info = [task for task in tasks if task["task_name"] == root][
            0
        ]  # get self task details
    except IndexError:
        return
    status = task_info["status"]
    task_name = task_info["task_name"]
    task_reocrd_at = timestamp_to_datetime(task_info["task_record_at"])

    _info = f"[{pick_color_for_status(status)} b] `{task_name}` at {task_reocrd_at}"
    tree = tree.add(_info)  # root

    for successor in graph.successors(root):
        build_dependant_log_tree(tree, graph, successor, tasks)


def build_flatten_log_tree(tree, tasks):
    for task in tasks:
        status = task["status"]
        task_name = task["task_name"]
        task_reocrd_at = timestamp_to_datetime(task["task_record_at"])
        _task = f"[{pick_color_for_status(status)} b] `{task_name}` at {task_reocrd_at}"
        tree.add(_task)


def pick_color_for_status(status: str):
    if status == "success":
        return "green"
    elif status == "failed":
        return "red"
    elif status == "running":
        return "blue"
    elif status == "pending":
        return "yellow"
    return "black"


def print_a_log_title(title):
    rprint("[blue b]" + ">> " * 5 + title + "<< " * 5)


def print_a_log_sep():
    rprint("[blue b]" + "== " * 20)


def print_a_log_end():
    rprint("[blue b]" + ">> " * 20)
