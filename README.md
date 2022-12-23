# Pydag

## What is pydag
Before explainning what is `pydag`, let't me explain you what does a  `dag` means;`dag` stands for a  `Directed Acyclic Graph`, does that ring a bell with you?Yes, the same `DAG` in `Airflow DAG`, but they are different. 
pydag is way more slim, I mean lightweight comparing to `ariflow`, yet it can also build and manage complicated workflow or pipeline.   
Pydag can not only handle sequential worflow(one task after another), but  can also handle a workflow(or a job, i will use both words interchangeably) like this:

<img src="resource/images/TestJob.png" alt="drawing" width="600"/>


> The task `Feature Engineering1` and `Feature Engineering2` have to run 
> in parallel, and Task `Machine Learning` has to wait until both of `Feature Engineering1` and `Feature Engineering2` finishing their jobs.


Besides **running a workflow**, pydag also supoorts:
- Schedule a workflow
- Manage a worfllow, including Create or Deltete a workflow
- Log a workflow, both at job level and task level(task belongs to the workflow)

## Install pydag
You can just run the following command to install  pydag:

```shell
 pip install pydag2
```

Although I said pydag is a lightwight tool(yes it is), it still rely on some externel stuff to make everythings function well.

Now, `pydag` rely on a job scheduling system called [gocron](https://github.com/ouqiang/gocron), the good news is this tool is written and build with go, so it's light(Again, airflow is just too heavy), bad news is it seems not well documented, and its only documentation is written in chineses. But most of time you donn't need to care too much about it, cuz 
pydag will connect it via its python sdk called `pygocron`(which will be install after installing the `pydag` automatically)。
If you konw Chinese, you can go to gocron's homepage, otherwise you can go to [here](https://github.com/superjcd/pygocron) and follow the **Install the gocron** section。

> Although pydag can run a workflow locally ignoring the system you are using, but when it comes to the scheduling part, it is better to put on a Linux machine(eventually it will relate to the cron table)

## How to use
### Build a workflow
The syntex is quite easy, here is a simple example:

```python
from pydag.task import GoCronTask
from pydag.job import GoCronJob
from pygocron.pygocron import PyGoCron

# step1 : define a job(workflow)
tm = PyGoCron()
job = GoCronJob(name="TestJob", task_manager=tm)


# step2: define tasks
task1 = GoCronTask(name="Sync Data", command="sleep 1") 
task2 = GoCronTask(name="Feature Engineering1", command="sleep 1")
task3 = GoCronTask(name="Feature Engineering2", command="sleep 1") 
task4 = GoCronTask(name="Machine Learning", command="sleep 1")
task5 = GoCronTask(name="Send Email", command="sleep 1")

# step 3: define relationships between tasks
task1.set_downstream(task2)
task1.set_downstream(task3)
task2.set_downstream(task4)
task3.set_downstream(task4)
task4.set_downstream(task5)

# step 4: 
job.add_task(task1, task2, task3, task4, task5)  
print(job)
job.run()

```
Let's break down the workflow step by step: 
- Step 1, define a job,  `GoCronJob` takes two arguments, one for job name(note job name cannot be duplicated, if you built a job with same name before, u have to use another), another for a `taskmanager`, we use `pygocron.PyGoCron` as our taskmanager
- Step2, define tasks related to the job, `GoCronTask` takes two arguments, one for task name, another for a command（shell command）
- Step3, very important part, you have to define the relationships betweeen tasks by using `setdownstream` or `setupstream` method. Note, you can never define a dependency like `task1 -> task2 -> task1`, cuz this will lead to a infinite running loop,  fortunately `pydag` will detect the bad cyclic relationships  for you 
- The last step, you just add all tasks you defined to the job, and kick off it by the run method。Note the `print` will output a digrah(png format) to your current workdirectory

### Plan a workflow

### Delete a workflow

### Get logs