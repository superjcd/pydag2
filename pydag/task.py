from pygocron.pygocron import RunStatus
from .exceptions import PyGoCronException
from abc import ABC, abstractclassmethod


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


class Task:
    def __init__(self, name: str, command: str, task_manager: TaskManger):
        self.id = self.name = name
        self.command = command
        self.upstreams = []
        self.downstreams = []
        self._job = None
        self._task_manager = task_manager

    def set_downstream(self, task: "Task"):
        if task not in self.downstreams:
            self.downstreams.append(task)

    def set_upstream(self, task: "Task"):
        if task not in self.upstreams:
            self.upstreams.append(task)

    def _get_node_presentation(self):
        return (self.id, self._get_infos())

    def _get_infos(self):
        return {"task_name": self.name, "command": self.command}

    def _get_edges(self):
        edges = []
        for successor in self.downstreams:
            edges.append((self.id, successor.id))
        for predecessor in self.upstreams:
            edges.append((predecessor.id, self.id))
        return edges

    def __repr__(self) -> str:
        return f"Task<{self.id}>"


class GoCronTask(Task):
    def __init__(self, name: str, command: str, task_manager):
        super().__init__(name, command, task_manager)

    def submit(self, add_job_name=False, job_name=""):  # add dag configs
        """
        submit ad task
        """
        if add_job_name:
            if job_name:
                task_name = str(job_name) + "-" + self.name
            else:
                raise ValueError(
                    "if `add_job_name` is `True`, then you must set a `job_name`"
                )
        else:
            task_name = self.name

        task_id = self._task_manager.create_task(
            name=task_name, spec=None, tag=None, command=self.command, level=2
        )  #
        if task_id:
            self.task_id = task_id
        else:
            raise PyGoCronException("Task submittion failed")

    def run(self,):
        """
        run a task 
        """

        run_id = self._task_manager.run_task(self.task_id)
        if run_id:
            self.run_id = run_id
        else:
            raise PyGoCronException("Task run failed")

    def check_run_status(self,) -> str:
        """
        get a runnning task status, either one of ["sucess", "failed", "pendding", "running"]
        """
        if not hasattr(self, "run_id"):
            # the run_id is set by run method, if task doesn't has a run id, meanning task didn't run yet
            return "pendding"

        status = self._task_manager.check_run_status(
            self.task_id, self.run_id
        )  # must wait

        if status == RunStatus.SUCCESS:
            return "success"
        elif status == RunStatus.FAILED:
            return "failed"
        elif status == RunStatus.RUNNING:
            return "running"
        elif status == RunStatus.PENDING:
            return "pendding"
        raise PyGoCronException(f"Wrong status: {status}")
