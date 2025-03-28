from abc import ABC, abstractmethod
from typing import Mapping, Any

from snakemake_interface_executor_plugins.utils import ShellRunner
from snakemake_interface_executor_plugins.settings import CommonSettings


class SpawnedJobArgsFactoryExecutorInterface(ABC):
    @abstractmethod
    def general_args(
        self,
        executor_common_settings: CommonSettings,
    ) -> dict[str: Any]: ...

    @abstractmethod
    def precommand(self, executor_common_settings: CommonSettings) -> ShellRunner: ...

    @abstractmethod
    def envvars(self) -> Mapping[str, str]: ...
