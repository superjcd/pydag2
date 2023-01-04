from pydag.task import GoCronTask
from pydag.job import GoCronJob
from pygocron.pygocron import PyGoCron

tm = PyGoCron()

job = GoCronJob(name="TestJob18", task_manager=tm)

task1 = GoCronTask(name="Sync Data", command="echo syncdata")

task2 = GoCronTask(name="Feature Engineering1", command="echo featureengineering1")

task3 = GoCronTask(name="Feature Engineering2", command="echo featureengineering1")

task4 = GoCronTask(name="Machine Learning", command="echo machinelearning")

task5 = GoCronTask(name="Send Email", command="echo sendemail")

task1.set_downstream(task2)

task1.set_downstream(task3)

task2.set_downstream(task4)

task3.set_downstream(task4)

task4.set_downstream(task5)

job.add_task(task1, task2, task3, task4, task5)


# print(job)

job.run(mode="execute")
