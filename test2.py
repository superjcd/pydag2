from pydag.task import GoCronTask
from pydag.job import GoCronJob
from pygocron.pygocron import PyGoCron


tm = PyGoCron()

# class DummyTaskManger(TaskManger):
#     def __init__(self) -> None:
#         pass

#     def create_task(self, name, spec, tag,):
#         print("task created")
#         return randint(1,100)

#     def run_task(self, task_id):
#         print("task triggerd")
#         return randint(1,100)

#     def  check_run_status(self, task_id, run_id):
#         random_number = random()
#         if random_number >= 0 and random_number <= 0.2:
#             return RunStatus.RUNNING
#         elif random_number > 0.2 and random_number <= 0.9:
#             return RunStatus.SUCCESS
#         else:
#             return RunStatus.FAILED

# tm = DummyTaskManger()

job = GoCronJob(name="TestJob")

task1 = GoCronTask(name="Sync Data", command="sleep 1", task_manager=tm)  # root

task2 = GoCronTask(name="Feature Engineering1", command="sleep 1", task_manager=tm)

task3 = GoCronTask(name="Feature Engineering2", command="sleep 1", task_manager=tm)

task4 = GoCronTask(name="Machine Learning", command="sleep 1", task_manager=tm)

task5 = GoCronTask(name="Send Email", command="sleep 1", task_manager=tm)

task1.set_downstream(task2)

task1.set_downstream(task3)

task2.set_downstream(task4)

task3.set_downstream(task4)

task4.set_downstream(task5)


job.add_task(task1, task2, task3, task4, task5)

# print(job)

job.submit()


job.run()
