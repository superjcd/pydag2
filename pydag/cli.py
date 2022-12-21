from pygocron.pygocron import PyGoCron
from plan import Plan
from argparse import ArgumentParser
from .utils import compose_command_for_job
from .job import GoCronJob


def submit(args):
    """
    plan job run by cron via `plan`
    """
    file = args.file      
    job_name = args.name if args.name else f"Pydag:Run {file}" # why not use the job name defined in file ?  
    
    command = compose_command_for_job(file, to_run_new=False) # always run a existing job 
    cron = Plan(name=job_name)

    cron_expression = args.cron
    if not cron_expression:
        raise ValueError("No cron expresion, please define one via `pydag summbit --cron [cron expression]`")
    cron.command(command, every=cron_expression)
    cron.run("write")

def delete(args):
    pyg = PyGoCron()
    job_name = args.job_name 
    GoCronJob.delete_job_tasks(pyg, job_name)

class Submit():
    @staticmethod
    def register_subcommand(subparser):
        submit_parser = subparser.add_parser("submit", help="CLI tool to submit a `pydag` job;Before run you have to make sure you had submitted the job before")
        submit_parser.add_argument("file",  type=str, help="Executable file to run, be aware that the `file` must exist in the current directory")
        submit_parser.add_argument("--cron",  type=str, help="Cron expression for the job, then job will run according to that expression")
        submit_parser.add_argument("--name", type=str, help="Job name, this is optional, if not given, the `file` will treates as a job name; Not this job name can differ from the job name you defined in the file")
        submit_parser.set_defaults(func=submit)


class Log():
    @staticmethod
    def register_subcommand(subparser):
        log_parser = subparser.add_parser("log", help="CLI tool to get `pydag` job & task logs")
        ...

class Delete():
    @staticmethod
    def register_subcommand(subparser):
        delete_parser = subparser.add_parser("delete", help="CLI tool to get `pydag` job & task logs")
        delete_parser.add_argument("job_name",  type=str, help="Job name, all tasks associate with the job name then will be deleted")
        delete_parser.set_defaults(func=delete)


def main():
    parser = ArgumentParser()  
    
    subparser = parser.add_subparsers()

    Submit.register_subcommand(subparser)
    Log.register_subcommand(subparser)
    Delete.register_subcommand(subparser)
    
    args = parser.parse_args()
    if not hasattr(args, 'func'):
        parser.print_help()
        exit(1)

    args.func(args)