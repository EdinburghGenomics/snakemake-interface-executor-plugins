__author__ = "Johannes Köster"
__copyright__ = "Copyright 2022, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from abc import abstractmethod
from typing import Dict, Optional
from snakemake_interface_executor_plugins.executors.base import (
    AbstractExecutor,
    SubmittedJobInfo,
)
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.settings import ExecMode
from snakemake_interface_executor_plugins.utils import ShellRunner, encode_target_jobs_cli_args
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface


class RealExecutor(AbstractExecutor):
    def __init__(
        self,
        workflow: WorkflowExecutorInterface,
        logger: LoggerExecutorInterface,
        post_init: bool = True,
    ):
        super().__init__(
            workflow,
            logger,
        )
        self.executor_settings = self.workflow.executor_settings
        self.snakefile = workflow.main_snakefile
        if post_init:
            self.__post_init__()

    def __post_init__(self):
        """This method is called after the constructor. By default, it does nothing."""
        pass

    @property
    @abstractmethod
    def cores(self):
        # return "all" in case of remote executors,
        # otherwise self.workflow.resource_settings.cores
        ...

    def report_job_submission(
        self, job_info: SubmittedJobInfo, register_job: bool = True
    ):
        super().report_job_submission(job_info)

        if register_job:
            try:
                job_info.job.register(external_jobid=job_info.external_jobid)
            except IOError as e:
                self.logger.info(
                    f"Failed to set marker file for job started ({e}). "
                    "Snakemake will work, but cannot ensure that output files "
                    "are complete in case of a kill signal or power loss. "
                    "Please ensure write permissions for the "
                    "directory {self.workflow.persistence.path}."
                )

    def handle_job_success(self, job: JobExecutorInterface):
        pass

    def handle_job_error(self, job: JobExecutorInterface):
        pass

    def additional_general_args(self):
        """Inherit this method to add stuff to the general args.

        A dict must be returned. It will be added to the args dict in ShellRunner.append_command()
        """
        return {}

    def get_job_args(self, job: JobExecutorInterface, **kwargs):
        """Returns a dict of args to be added to command for a given job
        """
        args = {}
        args["--target-jobs"] = list(encode_target_jobs_cli_args(job.get_target_spec()))

        args.update(self.additional_general_args())

        # Restrict considered rules for faster DAG computation.
        # This does not work for updated jobs because they need
        # to be updated in the spawned process as well.
        if not job.is_updated:
            args["--allowed-rules"] = job.rules

        # Ensure that a group uses its proper local groupid.
        if job.is_group():
            args["--local-groupid"] = job.jobid

        args["--cores"] = kwargs.get("cores", self.cores)
        args["--attempt"] = job.attempt
        args["--force-use-threads"] = not job.is_group()

        unneeded_temp_files = list(self.workflow.dag.get_unneeded_temp_files(job))
        if unneeded_temp_files:
            args["--unneeded-temp-files"] = unneeded_temp_files

        args["--resources"] = self.get_resource_declarations_dict(job)

        return args

    @property
    def job_specific_local_groupid(self):
        return True

    def get_snakefile(self):
        return self.snakefile

    @abstractmethod
    def get_python_executable(self): ...

    @abstractmethod
    def get_exec_mode(self) -> ExecMode: ...

    @property
    def common_settings(self):
        return self.workflow.executor_plugin.common_settings

    def get_envvar_declarations(self):
        """Return env vars as a dict.

           We leave it to ShellRunner to work out how to pass these to the shell.
        """
        envvars = self.envvars()
        if self.common_settings.pass_envvar_declarations_to_cmd and envvars:
            return envvars
        else:
            return dict()

    def get_job_exec_dir(self, job: JobExecutorInterface) -> Optional[str]:
        return None

    def get_job_exec_suffix(self, job: JobExecutorInterface) -> list:
        return []

    def format_job_exec(self, job: JobExecutorInterface) -> ShellRunner:
        # The precommand function returns a ShellRunner instance
        sr = self.workflow.spawned_job_args_factory.precommand(
            executor_common_settings=self.common_settings
        )

        sr.set_env(self.get_envvar_declarations())
        sr.set_cwd(self.get_job_exec_dir(job))

        # job_args is the dict of args passed to the snakemake command
        job_args = { "--snakefile": self.get_snakefile(),
                     "--mode": self.get_exec_mode().item_to_choice() }
        job_args.update(self.get_job_args(job))
        job_args.update( self.workflow.spawned_job_args_factory.general_args(
                            executor_common_settings=self.common_settings
                         ) )
        if not self.job_specific_local_groupid:
            job_args["--local-groupid"] = self.workflow.group_settings.local_groupid


        sr.append_command([ self.get_python_executable(),
                            "-m", "snakemake" ],
                            args = job_args )
        sr.append_command(self.get_job_exec_suffix(job))

        return sr

    def envvars(self) -> Dict[str, str]:
        return self.workflow.spawned_job_args_factory.envvars()
