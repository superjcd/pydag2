import networkx as nx
from typing import List
from .task import Task
from .utils import draw_graph, prepare_rich_logger
from .executor import RunQueue, CheckQueue, RunTaskExecutor, CheckTaskExecutor

logger = prepare_rich_logger("Job")


class Job:
    def __init__(self, name: str) -> None:
        self.id = self.name = name
        self.root_task = None
        self._tasks: List[Task] = []
        self._graph = nx.DiGraph()

    def add_task(self, *tasks):
        self._tasks.extend(tasks)

        self._graph.add_nodes_from([task._get_node_presentation() for task in tasks])

        edges = []
        existing_task_names = [task.id for task in self._tasks]  #

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

    def __init__(self, name: str) -> None:
        super().__init__(name)

    def submit(self):
        for task in self._tasks:
            task.submit(add_job_name=True, job_name=self.name)  # add other configs

    def run(self):
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

    def __repr__(self):
        draw_graph(self._graph)
        return f"<{self.name} with {len(self._tasks)} tasks>"
