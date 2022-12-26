# Pydag

## What is pydag
Before explainning what is `pydag`, let't me explain you what does a  `dag` means;`dag` stands for a  `Directed Acyclic Graph`, does that ring a bell with you?Yes, the same `DAG` in `Airflow DAG`, but they are different. 
pydag is way more slim, I mean lightweight comparing to `ariflow`, yet it can also build and manage complicated workflow or pipeline.   
Pydag can not only handle sequential worflow(one task after another), but  can also handle a workflow(or a job, I will use both words interchangeably) like this:

<img src="resource/images/TestJob.png" alt="drawing" width="600"/>


> The task `Feature Engineering1` and `Feature Engineering2`  have to run 
> in parallel when `Sync Data` is done,  and Task `Machine Learning` has to wait until both of `Feature Engineering1` and `Feature Engineering2` finishing their jobs.


Besides **running a workflow**, pydag also supoorts:
- Schedule a workflow
- Manage a worfllow, including Create or Deltete a workflow
- Log a workflow, both at job level and task level(task belongs to the workflow)

## Install pydag
You can just run the following command to install  pydag:

```shell
 pip install pydag2
```

Although I said pydag is a lightwight tool(yes it is), it still relys on some externel stuff to make everythings function well.
Now, `pydag` relys on a job scheduling system called [gocron](https://github.com/ouqiang/gocron).   
The good news is the tool is written and build with go, so it's light(Again, airflow is just too heavy), bad news is it seems not well documented, and its only documentation is written in chineses. But most of time it will not bother you, cuz pydag will connect it via its python sdk called `pygocron`(which will be install after installing the `pydag` automatically)。  
If you konw Chinese, you can go to gocron's homepage, otherwise you can go to [here](https://github.com/superjcd/pygocron) and follow the **Install the gocron** section to install gocron.

> Although pydag can run a workflow locally ignoring the system you are using, but when it comes to the scheduling part, it is better to put on a Linux machine(eventually it will relate to the cron table)

## How to use
### Build a workflow
Before getting started ,there're couple of enviroment varibles to be set, most of them are for connection to the `gocron`:

```shell
# connect to gocron
export GOCRON_ADDRESS=***
export GOCRON_ADMIN_USER=***
export GOCRON_ADMIN_PASSWORD=***

# connect to redis
export PYDAG_LOG_STORE_HOST=***
export PYDAG_LOG_STORE_PASSWORD=***

```

Just replace the `***` with real ones, and for `Windows` just substitude the `export` with set too.

The above envroment varibles maily did three things to get `pydag` ready：
- first three enviroments are for the connection to gocron
- the following two are for redis connection(we are using password here), this is mostly for logging purpose

(see other available enviroments in details below)

The syntex for building a pipelien is quite easy, here is a simple example(see `example.py` under the repo):

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
Let's break down the syntex step by step: 
- Step 1, define a job,  `GoCronJob` takes two arguments, one for job name(note job name cannot be duplicated, if you built a job with same name before, u have to use another), another for a `taskmanager`, we use `pygocron.PyGoCron` as our taskmanager
- Step2, define tasks related to the job, `GoCronTask` takes two arguments, one for task name, another for a command（shell command）
- Step3, very important part, you have to define the relationships betweeen tasks by using `setdownstream` or `setupstream` method. Note, you can never define a dependency like `task1 -> task2 -> task1`, cuz this will lead to a infinite running loop,  fortunately `pydag` will detect the bad cyclic relationships  for you 
- The last step, you just add all tasks you defined to the job, and submit all tasks to gocron by running `submit` method, then kick it off by the `run` method。Note the `print` will output a digrah(`png` format) to your current work directory

To run this workflow, say `example.py`, you can just run:

```shell
python example.py
```

> Behind the scene, pydag render all the tasks to the `gocron`(via `submit`), and keep tracking the running status for each tasks, when all task is done(no matter sucessed or not), the workflow will terminated too. 


## The pydag cli tool
`pydag` also offers a cli tool to manage our jobs

### Submit
To submit a pipeline, you can use the `submit` sub command:

```shell
pydag submit example.py --cron="0 0 * * *" --name="TestJob"
```

`cron` is just a normal cron expression, and `name` is the jobname you defined in the `example.py` 

Behind the scene, this command will run you job(here `example.py`) by rendering it to the cron table, which means you have to ran on a crontab-supported paltform 

### Delete
To delete a job, just by running the following command:

```shell
pydag delete TestJob
```
That is it 

### Log
pydag support both `job` level log and `task` level log.

#### Get job level logs
To get recent loggings for job `TestJob` by run:
```shell 
pydag log TestJob
```
Then the terminal shold give you:

<img src="resource/images/JobLogFlat.png" alt="drawing" width="500"/>

Note the color also shows the status of the task:
- `green` for success
- `red` for fail
- `blue` for runninng
- `yellow` for pendding

The above log doesn't show the dependency between tasks, if you want to, you can specify a `style` flag
```shell
pydag log TestJob --style=tree
```

Then the log will shown as a `tree` style:

<img src="resource/images/JobLogTree.png" alt="drawing" width="500"/>

Finally you can define how many recent logs do you want to show by add a `-n` flag(default is 3)

#### Get task level logs

## The envrionment varibles


## Case study
Good luck, have fun.