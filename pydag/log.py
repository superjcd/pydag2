import redis
from dataclasses import dataclass
from .utils import TaskStatus
from .environments import HOST, PASSWORD

@dataclass
class RecordDetail:
    run_id: int
    task_id: int
    status: str

    def compose_detail(self):
        return ",".join([str(self.run_id), str(self.task_id), self.status])


@dataclass
class Record:
    job_name: str
    job_run_at: int 
    task_name: str
    info: RecordDetail

    def compose_key(self):
        return ":".join([self.job_name,str(self.job_run_at),self.task_name])

    def compose_value(self):
        return self.info.compose_detail()




class BasicJobLogger:
    def __init__(self):
        self._store = redis.Redis(host=HOST, password=PASSWORD, db=1)

    def record(
        self,
        job_name: str,
        job_run_at: int,
        task_name: str,
        run_id: int,
        task_id: int,
        status: str,
    ):
        record = Record(job_name, job_run_at, task_name, RecordDetail(run_id, task_id, status))
        self._store.set(record.compose_key(), record.compose_value())


