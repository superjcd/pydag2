from threading import Thread
from queue import Queue
from .utils import prepare_rich_logger, TaskStatus
import time

logger = prepare_rich_logger("Executor")


RunQueue = Queue()
CheckQueue = Queue()

QueueBlockTime = 10


class RunTaskExecutor(Thread):
    def __init__(self, job):
        Thread.__init__(self)
        self._job = job

    def run(self) -> None:
        while True:
            logger.info("New run round begains")

            try:
                task = RunQueue.get(
                    block=True, timeout=QueueBlockTime
                )  # block untile timeout
            except:
                logger.warn("Empty RunQueue ")
                continue

            if task is None:
                break

            predecessors = self._job.get_predecessors(task.id)
            # logger.debug(f"task:{task.id} successors: {successors}")
            task_predecessors_success_num = 0

            for predecessor in predecessors:
                # log here, and base the job
                status = predecessor.check_run_status(self._job._task_manager)
                if status == TaskStatus.SUCCESS:
                    task_predecessors_success_num += 1

                # if the status is not right

            if len(predecessors) == task_predecessors_success_num:
                try:
                    task.run(self._job._task_manager)  # mark failed
                except Exception as e:  # task.log()
                    raise RuntimeError(
                        f"Task `{task.name}` not been triggered successfully, details: {e.args}"
                    )
            else:
                RunQueue.put(task)


class CheckTaskExecutor(Thread):
    def __init__(self, job, root_task_id):
        Thread.__init__(self)
        self._job = job
        self._seen_tasks = set()
        self._seen_tasks.add(root_task_id)
        self._job_start_at = int(time.time())

    def run(self) -> None:
        success_checked = 0
        non_success_checked = 0

        while True:
            logger.info("New check round begains")
            try:
                task = CheckQueue.get(block=True, timeout=QueueBlockTime)
            except:
                logger.warn("Empty CheckQueue")
                continue

            status = task.check_run_status(self._job._task_manager)

            try:
                task.record(
                    self._job.name, self._job_start_at, status, int(time.time())
                )
            except AttributeError:
                # run id and task id is not ready at very beginning
                pass

            if status in [TaskStatus.RUNNING, TaskStatus.PENDING]:
                CheckQueue.put(task)
                time.sleep(10)

            elif status == TaskStatus.FAILED:
                non_success_checked += 1
                RunQueue.put(None)
                raise RuntimeError(f"Task `{task.id}` failed;")

            elif status == TaskStatus.SUCCESS:
                success_checked += 1  # the last one

                # all task checked
                if (success_checked + non_success_checked) == len(self._job._tasks):
                    logger.info(f"All tasks of {self._job.name} finished")
                    RunQueue.put(None)  # also notify RunQueue to terminate
                    break

                successors = self._job.get_successors(task.id)

                for successor in successors:
                    if successor.id in self._seen_tasks:  # machine learning 1
                        continue
                    self.put_task_and_record(successor)
                    CheckQueue.put(successor)

                # logger.info(f"Task `{task.id}` finished successfully")

    def put_task_and_record(self, task):
        RunQueue.put(task)
        self._seen_tasks.add(task.id)
