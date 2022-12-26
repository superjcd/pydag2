from pydag.task import GoCronTask
from pydag.job import GoCronJob
from pygocron.pygocron import PyGoCron
from pydag.helper import compose_command_for_file

tm = PyGoCron()

fn = compose_command_for_file

job = GoCronJob(name="TestJob15", task_manager=tm)

task1 = GoCronTask(name="Sync Data", command=fn("examples/1/syncdata.py"))

task2 = GoCronTask(name="Feature Engineering1", command=fn("examples/1/fe1.py"))

task3 = GoCronTask(name="Feature Engineering2", command=fn("examples/1/fe2.py"))

task4 = GoCronTask(name="Machine Learning", command="examples/1/machinelearning.py")

task5 = GoCronTask(name="Send Email", command="examples/1/sendemail.py")

task1.set_downstream(task2)

task1.set_downstream(task3)

task2.set_downstream(task4)

task3.set_downstream(task4)

task4.set_downstream(task5)

job.add_task(task1, task2, task3, task4, task5)


# print(job.get_descendant_counts("Feature Engineering2"))

# print(job)

job.run()
