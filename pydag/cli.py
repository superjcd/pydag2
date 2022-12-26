from pygocron.pygocron import PyGoCron
from plan import Plan
from argparse import ArgumentParser
from .utils import compose_command_for_job, prepare_rich_logger
from .job import GoCronJob
from .log import BasicJobLogger

logger = prepare_rich_logger("Pydag")


def submit(args):
    """
    plan job run by cron via `plan`
    """
    file = args.file
    job_name = args.name

    if not job_name:
        raise ValueError(
            "No `job` name provided, please define one via `--name=[job name]` flag"
        )

    command = compose_command_for_job(
        file, to_run_new=False
    )  # always run a existing job
    cron = Plan(name=job_name)

    cron_expression = args.cron
    if not cron_expression:
        raise ValueError(
            "No cron expresion provides, please define one via `--cron=[cron expression]` flag"
        )
    cron.command(command, every=cron_expression)
    cron.run("update")


def delete(args):
    pyg = PyGoCron()
    job_name = args.job_name

    # delete all task instance
    GoCronJob.delete_job_tasks(pyg, job_name)

    # delete associated cron-job
    cron = Plan(name=job_name)
    cron.run("clear")

    # delete associate logs
    bl = BasicJobLogger()
    bl.clear_log(job_name)


def log(args):
    bl = BasicJobLogger()
    

    style = args.style

    if args.task_name:
        bl.log_task(args.job_name, args.task_name, args.n)
    else:
        bl.log_job(args.job_name, args.n, style)

    


class Submit:
    @staticmethod
    def register_subcommand(subparser):
        submit_parser = subparser.add_parser(
            "submit",
            help="CLI tool to submit a `pydag` job;Before run you have to make sure you had submitted the job before",
        )
        submit_parser.add_argument(
            "file",
            type=str,
            help="Executable file to run, be aware that the `file` must exist in the current directory",
        )
        submit_parser.add_argument(
            "--cron",
            type=str,
            help="Cron expression for the job, then job will run according to that expression",
        )
        submit_parser.add_argument(
            "--name",
            type=str,
            help="Job name, this is optional, if not given, the `file` will treates as a job name; Not this job name can differ from the job name you defined in the file",
        )
        submit_parser.set_defaults(func=submit)


class Log:
    @staticmethod
    def register_subcommand(subparser):
        log_parser = subparser.add_parser(
            "log", help="CLI tool to get `pydag` job & task logs"
        )
        log_parser.add_argument("job_name", type=str, help="Job name")
        log_parser.add_argument(
            "--task_name",
            type=str,
            help="Task name, optional; If it's given, then the log for specific task will be shown",
        )
        log_parser.add_argument(
            "-n",
            type=int,
            default=3,
            help="The number of recent job logs, default is `3`",
        )
        log_parser.add_argument(
            "--style",
            type=str,
            default="flat",
            choices=["flat", "tree"],
            help="Print the log in `tree` format or `flat` format"
        )
        log_parser.set_defaults(func=log)


class Delete:
    @staticmethod
    def register_subcommand(subparser):
        delete_parser = subparser.add_parser(
            "delete", help="CLI tool to delete `pydag` job"
        )
        delete_parser.add_argument(
            "job_name",
            type=str,
            help="Job name, all tasks associate with the job name then will be deleted",
        )
        delete_parser.set_defaults(func=delete)


def main():
    parser = ArgumentParser()

    subparser = parser.add_subparsers()

    Submit.register_subcommand(subparser)
    Log.register_subcommand(subparser)
    Delete.register_subcommand(subparser)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        exit(1)

    args.func(args)
