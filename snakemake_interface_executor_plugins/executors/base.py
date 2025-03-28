__author__ = "Johannes Köster"
__copyright__ = "Copyright 2022, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface


@dataclass
class SubmittedJobInfo:
    job: JobExecutorInterface
    external_jobid: Optional[str] = None
    aux: Optional[Dict[Any, Any]] = None


class AbstractExecutor(ABC):
    def __init__(
        self,
        workflow: WorkflowExecutorInterface,
        logger: LoggerExecutorInterface,
    ):
        self.workflow = workflow
        self.dag = workflow.dag
        self.logger = logger

    def get_resource_declarations_dict(self, job: JobExecutorInterface):

        excluded_resources = self.workflow.resource_scopes.excluded.union(
            {"_nodes", "_cores"}
        )
        return {
            resource: value
            for resource, value in job.resources.items()

            # need to check explicitly because bool is a subclass of int
            if type(value) is int and (resource not in excluded_resources)
        }

    def run_jobs(
        self,
        jobs: List[JobExecutorInterface],
    ):
        """Run a list of jobs that is ready at a given point in time.

        By default, this method just runs each job individually.
        This method can be overwritten to submit many jobs in a more efficient
        way than one-by-one. Note that in any case, for each job, the callback
        functions have to be called individually!
        """
        for job in jobs:
            self.run_job_pre(job)
            self.run_job(job)

    @abstractmethod
    def run_job(
        self,
        job: JobExecutorInterface,
    ):
        """Run a specific job or group job.

        After successfull submission, you have to call self.report_job_submission(job).
        """
        ...

    def run_job_pre(self, job: JobExecutorInterface):
        self.printjob(job)

    def report_job_success(self, job_info: SubmittedJobInfo):
        self.workflow.scheduler.finish_callback(job_info.job)

    def report_job_error(self, job_info: SubmittedJobInfo, msg=None, **kwargs):
        self.print_job_error(job_info, msg, **kwargs)
        self.workflow.scheduler.error_callback(job_info.job)

    def report_job_submission(self, job_info: SubmittedJobInfo):
        self.workflow.scheduler.submit_callback(job_info.job)

    @abstractmethod
    def shutdown(self): ...

    @abstractmethod
    def cancel(self): ...

    def rule_prefix(self, job: JobExecutorInterface):
        return "local " if job.is_local else ""

    def printjob(self, job: JobExecutorInterface):
        job.log_info()

    def print_job_error(self, job_info: SubmittedJobInfo, msg=None, **kwargs):
        job_info.job.log_error(msg, **kwargs)

    @abstractmethod
    def handle_job_success(self, job: JobExecutorInterface): ...

    @abstractmethod
    def handle_job_error(self, job: JobExecutorInterface): ...
