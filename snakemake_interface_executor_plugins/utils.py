__author__ = "Johannes Köster"
__copyright__ = "Copyright 2023, Johannes Köster"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import os
import asyncio
import base64
from collections import UserDict
import shlex
import threading
from typing import Any, List
from urllib.parse import urlparse
from collections import namedtuple
import concurrent.futures
import contextlib
import subprocess

from snakemake_interface_common.settings import SettingsEnumBase
from snakemake_interface_common.utils import not_iterable


TargetSpec = namedtuple("TargetSpec", ["rulename", "wildcards_dict"])

class ShellRunner:
    """A class which captures a series of commands to be run. You may specify a working
       directory and/or a custom environment as would be passed to subprocess.run.

       You may specify one or more commands to be run in the case of an error.

       You may add one or more commands that run finally, regardless of errors.
    """
    def __init__(self, cwd=None, env=None):
        self.cmds = []
        self.set_cwd(cwd)
        self.set_env(env)

        self.on_error_cmds = []
        self.on_exit_cmds = []

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

    def append_on_error(self, cmd, args=None):
        """Append a command to run if there is an error.
        """
        new_cmd = self._prep_cmd(cmd, args)

        if new_cmd:
            self.on_error_cmds.append(new_cmd)

    def append_on_exit(self, cmd, args=None):
        """Add a commands to run at the end whatever happens.
        """
        new_cmd = self._prep_cmd(cmd, args)

        if new_cmd:
            self.on_ecit_cmds.append(new_cmd)

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

            # FIXME - do I need to deal explicitly with SettingsEnumBase?
            assert not isinstance(value, SettingsEnumBase)

            if value is False or value is None:
                pass # Skip this one entirely
            elif value is True:
                new_cmd.append(flag)
            elif isinstance(value, (dict, UserDict)):
                # A list of k=v pairs
                if value.keys():
                    new_cmd.append(flag)
                    new_cmd.extend(f"{k}={v}" for k, v in value.items())
            elif not_iterable(value):
                # Strings, ints, Paths, etc.
                new_cmd.append(flag)
                new_cmd.append(str(value))
            else:
                value = [str(s) for s in value if s is not None]
                if value:
                    new_cmd.append(flag)
                    new_cmd.extend(value)

        return new_cmd

    def quote_command(self, oneline=True):
        """Return all the commands as a big string, ready to run in Bash or Dash
        """
        cmd_prefix = []
        if self.cwd is not None:
            cmd_prefix.append(["cd", self.cwd])
        if self.env:
            env_items = [ f"{k}={v}" for k, v in self.env.items() ]
            cmd_prefix.append(["export", *env_items])

        def qcl(cmd_list):
            """Quote Command List - Turns a list of lists into a list of strings
            """
            return [ " ".join(shlex.quote(s) for s in acmd) for acmd in cmd_list ]

        func = self._assemble_command_oneline if oneline else self._assemble_command_multiline
        return func(qcl(cmd_prefix + self.cmds), qcl(self.on_error_cmds), qcl(self.on_exit_cmds))


    def _assemble_command_multiline(self, cmds, on_error, on_exit):
        full_cmd = "( set -e\n"

        for acmd in cmds:
            # acmd is already a quoted string
            full_cmd += acmd
            full_cmd += "\n"

        full_cmd += ") ; _retval=$?\n"

        # commands to be executed in case of error
        if on_error:
            full_cmd += "if [ $_retval != 0 ] ; then\n"
            for acmd in on_error:
                full_cmd += acmd
                full_cmd += "\n"
            full_cmd += "fi\n"

        # commands to be executed regardless
        for acmd in on_exit:
            full_cmd += acmd
            full_cmd += "\n"
        full_cmd += "[ $_retval = 0 ] || exit $_retval\n"

        return full_cmd

    def _assemble_command_oneline(self, cmds, on_error, on_exit):
        # The structure for quoting out the command on a single line is different enough
        # to warrand a separate function.
        cmd_line = "{ "
        cmd_line += " && ".join(cmds)
        cmd_line += " ; } || { _retval=$? ; "

        # commands to be executed in case of error
        for acmd in on_error:
            cmd_line += acmd + " ; "
        cmd_line += "} ; "

        for acmd in on_exit:
            cmd_line += acmd + " ; "
        cmd_line += '[ "${_retval:-0}" = 0 ] || exit $_retval'

        return cmd_line

    def check_call(self, **args):
        """Runs each command with subprocess.check_call(), raising subprocess.CalledProcessError
           if any command fails,
        """
        #  combine self.env with os.environ
        full_env = dict(os.environ)
        full_env.update(self.env)

        try:
            for acmd in self.cmds:
                subprocess.check_call(acmd, cwd=self.cwd, env=full_env, **args)
        except subprocess.CalledProcessError as e:
            # Run all the on_error commands before raising the exception
            for acmd in self.on_error_cmds:
                subprocess.call(acmd, cwd=self.cwd, env=full_env, **args)
            raise e from None
        finally:
            for acmd in self.on_exit_cmds:
                subprocess.call(acmd, cwd=self.cwd, env=full_env, **args)

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

base64_prefix = "base64//"

''' FIXME delete this

_is_quoted_re = re.compile(r"^['\"].+['\"]")

def is_quoted(value: str) -> bool:
    return _is_quoted_re.match(value) is not None

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

