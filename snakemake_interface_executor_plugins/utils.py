__author__ = "Johannes Köster"
__copyright__ = "Copyright 2023, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import asyncio
import base64
from collections import UserDict
from pathlib import Path
import re
import shlex
import threading
from typing import Any, List
from urllib.parse import urlparse
from collections import namedtuple
import concurrent.futures
import contextlib

from snakemake_interface_common.settings import SettingsEnumBase
from snakemake_interface_common.utils import not_iterable


TargetSpec = namedtuple("TargetSpec", ["rulename", "wildcards_dict"])

class ShellRunner:
    """A class which captures a series of commands to be run. You may specify a working
       directory and/or a custom environment as would be passed to subprocess.run.
    """
    def __init__(self, cwd=None, env=None):
        self.cmds = []
        self.set_cwd(cwd)
        self.set_env(env)

    def set_cwd(self, cwd):
        """Set the directory where all commands will run. You may set this to None.
        """
        if not cwd:
            self.cwd = None
        else:
            # pathlib.Path object will be converted to a string
            self.cwd = str(cwd)

    def set_env(self, env):
        """Set the environment that will be used for commands. You may set this to None,
           meaning that the current environent will be kept.
        """
        # Shallow-copy the dict while ensuring it is a dict (or dict-like)
        if env is None:
            self.env = None
        else:
            self.env = dict(env)

    def append_command(self, cmd, args=None):
        """Add the command to the end of the list of commands to run.
           cmd must be a list or iterable of strings
           args may be a dict of additional arguments
           if the command is empty after processing it will not be added
        """
        new_cmd = self._prep_cmd(cmd, args)

        if new_cmd:
            self.cmds.append(new_cmd)

    def prepend_command(self, cmd, args=None):
        """Add the command to the beginning of the list of commands to run.
           see append_command()
        """
        new_cmd = self._prep_cmd(cmd, args)

        if new_cmd:
            # Could use a deque, but let's keep it vanilla.
            self.cmds[:0] = [new_cmd]

    def _prep_cmd(self, cmd, args):
        """This should be invoked via append_command() or prepend_command().
           Returns a list of strings
        """
        # cmd must be a list or iterable. Any None is removed. Anything else is converted
        # to a str
        new_cmd = [ str(s) for s in cmd if s is not None ]

        if args is None:
            args = {}

        for flag, value in args.items():
            new_cmd.append(flag)
            if value is False or value is None:
                new_cmd.pop() # On second thoughts, remove this flag
            elif value is True:
                pass
            elif isinstance(value, (dict, UserDict)):
                # A list of k=v pairs
                new_cmd.extent(f"{k}={v}" for k, v in value.items())
            elif not_iterable(value):
                new_cmd.append(str(value))
            else:
                new_cmd.extend(str(s) for s in value if s is not None)

        return new_cmd

    def quote_command(self):
        """Return the whole command as a big string, ready to run in Bash
        """
        quoted_cmd = ""
        if self.cwd is not None:
            quoted_cmd += f"cd {shlex.quote(self.cwd)} && "
        if self.env:
            env_items = [ shlex.quote(f"{k}={v}") for k, v in self.env.items() ]
            quoted_cmd += f"export {' '.join(env_items)} && "
        for acmd in self.cmds:
            quoted_cmd += " ".join(shlex.quote(s) for s in acmd)
            if acmd is not self.cmds[-1]:
                quoted_cmd += " && "

        return quoted_cmd

    def check_call(self, **args):
        """Runs each command with subprocess.check_call(), raising subprocess.CalledProcessError
           if any command fails,
        """
        # TODO - might need to combine self.env with os.environ?
        for acmd in self.cmds:
            subprocess.check_call(acmd, cwd=self.cwd, env=self.env, **args)

''' TODO - delete all this
def format_cli_arg(flag, value, quote=True, skip=False, base64_encode: bool = False):
    if not skip and value:
        if isinstance(value, bool):
            value = ""
        else:
            value = format_cli_pos_arg(value, quote=quote, base64_encode=base64_encode)
        return f"{flag} {value}"
    return ""


def format_cli_pos_arg(value, quote=True, base64_encode: bool = False):
    if isinstance(value, (dict, UserDict)):

        def fmt_item(key, value):
            expr = f"{key}={format_cli_value(value)}"
            return encode_as_base64(expr) if base64_encode else repr(expr)

        return join_cli_args(fmt_item(key, val) for key, val in value.items())
    elif not_iterable(value):
        return format_cli_value(value, quote=quote, base64_encode=base64_encode)
    else:
        return join_cli_args(
            format_cli_value(v, quote=quote, base64_encode=base64_encode) for v in value
        )


def format_cli_value(
    value: Any, quote: bool = False, base64_encode: bool = False
) -> str:
    """Format a given value for passing it to CLI.

    If base64_encode is True, str values are encoded and flagged as being base64 encoded.
    """

    def maybe_encode(value):
        return encode_as_base64(value) if base64_encode else value

    if isinstance(value, SettingsEnumBase):
        return value.item_to_choice()
    elif isinstance(value, Path):
        if base64_encode:
            return encode_as_base64(str(value))
        else:
            return shlex.quote(str(value))
    elif isinstance(value, str):
        if is_quoted(value) and not base64_encode:
            # the value is already quoted, do not quote again
            return maybe_encode(value)
        elif quote and not base64_encode:
            return maybe_encode(repr(value))
        else:
            return maybe_encode(value)
    else:
        return repr(value)


def join_cli_args(args):
    try:
        return " ".join(arg for arg in args if arg)
    except TypeError as e:
        raise TypeError(
            f"bug: join_cli_args expects iterable of strings. Given: {args}"
        ) from e
'''

def url_can_parse(url: str) -> bool:
    """
    returns true if urllib.parse.urlparse can parse
    scheme and netloc
    """
    return all(list(urlparse(url))[:2])


def encode_target_jobs_cli_args(
    target_jobs: List[TargetSpec],
) -> List[str]:
    """Yields a series of rule::wc1=v1 strings. The same rule may be
       repeated to add multiple wildcards. The v1 part may contain any
       characters including ':' and '=' and quotes.
    """
    for spec in target_jobs:
        if not spec.wildcards_dict:
            # Rule with no wildcards
            yield f"{spec.rulename}::"
        else:
            for key, value in spec.wildcards_dict.items():
                yield f"{spec.rulename}::{key}={value}"

_pool = concurrent.futures.ThreadPoolExecutor()


@contextlib.asynccontextmanager
async def async_lock(_lock: threading.Lock):
    """Use a threaded lock from threading.Lock in an async context

    Necessary because asycio.Lock is not threadsafe, so only one thread can safely use
    it at a time.
    Source: https://stackoverflow.com/a/63425191
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_pool, _lock.acquire)
    try:
        yield  # the lock is held
    finally:
        _lock.release()

''' FIXME delete this

_is_quoted_re = re.compile(r"^['\"].+['\"]")


def is_quoted(value: str) -> bool:
    return _is_quoted_re.match(value) is not None


base64_prefix = "base64//"

def encode_as_base64(arg: str):
    return f"{base64_prefix}{base64.b64encode(arg.encode()).decode()}"
'''

def maybe_base64(parser_func):
    """Parse optionally base64 encoded CLI args, applying parser_func if not None."""

    def inner(args):
        def is_base64(arg):
            return arg.startswith(base64_prefix)

        def decode(arg):
            if is_base64(arg):
                return base64.b64decode(arg[len(base64_prefix) :]).decode()
            else:
                return arg

        def apply_parser(args):
            if parser_func is not None:
                return parser_func(args)
            else:
                return args

        if isinstance(args, str):
            return apply_parser(decode(args))
        elif isinstance(args, list):
            decoded = [decode(arg) for arg in args]
            return apply_parser(decoded)
        else:
            raise NotImplementedError()

    return inner

