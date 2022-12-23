from typing import List
from abc import ABC, abstractclassmethod
import networkx as nx
from pygocron.pygocron import PyGoCron
from .task import Task
from .utils import draw_graph, prepare_rich_logger
from .executor import RunQueue, CheckQueue, RunTaskExecutor, CheckTaskExecutor
from .environments import TO_RUN_NEW
from .exceptions import PyDagException
from .log import BasicJobLogger

logger = prepare_rich_logger("Job")


class TaskManger(ABC):
    @abstractclassmethod
    def create_task():
        ...

    @abstractclassmethod
    def run_task():
        ...

    @abstractclassmethod
    def check_run_status():
        ...


class Job:
    def __init__(self, name: str, task_manager: TaskManger) -> None:
        self.id = self.name = name
        self.root_task = None
        self._tasks: List[Task] = []
        self._graph = nx.DiGraph()
        self._task_manager = task_manager
        self._job_logger = BasicJobLogger()

    def add_task(self, *tasks):
        self._tasks.extend(tasks)

        self._graph.add_nodes_from([task._get_node_presentation() for task in tasks])

        edges = []
        existing_task_names = [task.id for task in self._tasks]

        for task in tasks:
            task_edges = task._get_edges()
            for pair in task_edges:
                for node in pair:
                    if node not in existing_task_names:
                        raise ValueError(
                            f"`Task<{node}>` seems forgoted to be added to the job"
                        )
            edges.extend(task_edges)

        self._graph.add_edges_from(edges)

        if not nx.is_directed_acyclic_graph(self._graph):
            raise ValueError(
                f"""Can not build a directed acyclic graph for job:`{self.name}`, 
                                   please ensure there's no loop among your tasks"""
            )
        self.root_task = self.get_root_task()

    def get_task_by_id(self, id):
        for task in self._tasks:
            if task.id == id:
                return task
        raise ValueError(f"Cannnot find a task given the id: {id}")

    def get_root_task(self,):
        root_id = [n for n, d in self._graph.in_degree() if d == 0][0]
        return self.get_task_by_id(root_id)

    def get_successors(self, id):
        successors = list(self._graph.successors(id))
        tasks = []

        for task in self._tasks:
            if task.id in successors:
                tasks.append(task)
        return tasks

    def get_predecessors(self, id):
        predecessors = list(self._graph.predecessors(id))
        tasks = []

        for task in self._tasks:
            if task.id in predecessors:
                tasks.append(task)
        return tasks


class GoCronJob(Job):
    """
    Build a job based on cron
    """

    def __init__(self, name: str, task_manager: PyGoCron) -> None:
        super().__init__(name, task_manager)
        self._check_job_not_exists_before()

    def run(self):
        self._submit_tasks()

        logger.info(f"Job `{self.id}` trggered")
        root_task = self.get_root_task()

        RunQueue.put(root_task)
        CheckQueue.put(root_task)

        run_executor = RunTaskExecutor(job=self)
        check_executor = CheckTaskExecutor(job=self, root_task_id=root_task.id)

        run_executor.start()
        check_executor.start()

        run_executor.join()
        check_executor.join()

    def _check_job_not_exists_before(self):
        if TO_RUN_NEW == "yes":
            logger.info("Prepare to run a brand new job")
            tasks = self._task_manager.get_tasks(
                tag=self.name
            )  # {'data': [], 'total': 0}
            if tasks["total"] > 0:
                raise PyDagException(
                    f"Job `{self.name}` already exists, please try another name, or you can delete the existing job and run again"
                )

        elif TO_RUN_NEW == "no":
            logger.info("Prepare to run a existed job")
            return
        else:
            raise PyDagException(
                f"Wrong `PYDAG_RUN_NEW` value: {TO_RUN_NEW}, must be one of [`yes`, `no`] (case insensitive)"
            )

    def _submit_tasks(self, log_job_meta=True):
        for task in self._tasks:
            task.submit(
                add_job_name=True,
                job_name=self.name,
                tag=self.name,
                task_manager=self._task_manager,
            )
        if log_job_meta:
            self._job_logger.record_job_info(self.name, self._graph)

    def __repr__(self):
        draw_graph(self._graph)
        return f"<{self.name} with {len(self._tasks)} tasks>"

    @staticmethod
    def delete_job_tasks(task_manager: PyGoCron, job_name: str):
        task_manager.delete_task_by_tag(job_name)
        logger.info(
            f"All tasks belongs to job `{job_name}` have been deleted successfully"
        )
