from pydag.task import GoCronTask
from pydag.job import GoCronJob
from pygocron.pygocron import PyGoCron

tm = PyGoCron()

job = GoCronJob(name="TestJob", task_manager=tm)

task1 = GoCronTask(name="Sync Data", command="sleep 1")

task2 = GoCronTask(name="Feature Engineering1", command="sleep 1")

task3 = GoCronTask(name="Feature Engineering2", command="sleep 1")

task4 = GoCronTask(name="Machine Learning", command="sleep 1")

task5 = GoCronTask(name="Send Email", command="sleep 1")

task1.set_downstream(task2)

task1.set_downstream(task3)

task2.set_downstream(task4)

task3.set_downstream(task4)

task4.set_downstream(task5)

job.add_task(task1, task2, task3, task4, task5)


# print(job.get_descendant_counts("Feature Engineering2"))

# print(job)

job.run()
