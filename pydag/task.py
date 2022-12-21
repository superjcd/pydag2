import os
from pygocron.pygocron import RunStatus
from .exceptions import PyGoCronException
from .utils import compose_task_name



class Task:
    def __init__(self, name: str, command: str):
        self.id = self.name = name
        self.command = command
        self.upstreams = []
        self.downstreams = []

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
    def __init__(self, name: str, command: str):
        super().__init__(name, command)

    def submit(self, add_job_name=False, job_name="", tag="", task_manager=None): 
        """
        submit ad task
        """
        if task_manager == None:
            raise ValueError("It seems you didn't provide a `task_manger` when definnig a `job`")
        if add_job_name:
            if job_name:
                task_name = compose_task_name(job_name, self.name)
            else:
                raise ValueError(
                    "if `add_job_name` is `True`, then you must set a `job_name`"
                )
        else:
            task_name = self.name
        
        # check run a brand new job or existing job
        to_run_new = os.environ.get("PYDAG_RUN_NEW", "yes").lower().strip()
        if to_run_new == "yes":
            task_id = task_manager.create_task(
                name=task_name, spec=None, tag=tag, command=self.command, level=2
            )  
        elif to_run_new == "no":
            task_id = task_manager.get_task_id_by_name(task_name) 
        else:
            raise Exception(f"Wrong `PYDAG_RUN_NEW` value, must be one of [`yes`, `no`] (case insensitive)")      

        if task_id:
            self.task_id = task_id
        else:
            raise PyGoCronException("Task submittion failed")

    def run(self, task_manager):
        """
        run a task 
        """

        run_id = task_manager.run_task(self.task_id)
        if run_id:
            self.run_id = run_id
        else:
            raise PyGoCronException("Task run failed")

    def check_run_status(self, task_manager) -> str:
        """
        get a runnning task status, either one of ["sucess", "failed", "pendding", "running"]
        """
        if not hasattr(self, "run_id"):
            # the run_id is set by run method, if task doesn't has a run id, meanning task didn't run yet
            return "pendding"

        status = task_manager.check_run_status(
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
    

