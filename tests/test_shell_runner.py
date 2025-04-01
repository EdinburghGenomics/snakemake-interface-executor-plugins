from snakemake_interface_executor_plugins.utils import ShellRunner
from textwrap import dedent as dd

# Test the class that aims to robustly run shell commands, and replace these functions:
#  format_cli_arg
#  format_cli_pos_arg
#  is_quoted
#  format_cli_value
#  is_quoted

def test_shell_runner_1():

    sr = ShellRunner( cwd="dir with spaces",
                      env={ 'E1': "env var with spaces",
                            'E2': "env var \"with\" \'quotes\'" } )
    sr.append_command( ["command", "arg1", None, "arg2"],
                       args = { '--foo': True,
                                '--bar': False,
                                '--bam': ["list", "of", "bam things"],
                                '--baz': dict(k1 = "v1", k2 = "v 2") } )

    sr.append_command( ["append"] )
    sr.prepend_command( ["prepend"] )

    expected = ( "{ cd 'dir with spaces'"
                 " && export 'E1=env var with spaces' 'E2=env var \"with\" '\"'\"'quotes'\"'\"''"
                 " && prepend"
                 " && command arg1 arg2 --foo --bam list of 'bam things' --baz k1=v1 'k2=v 2'"
                 " && append ; } || { _retval=$? ; } ;"
                 " [ \"${_retval:-0}\" = 0 ] || exit $_retval" )

    assert(sr.quote_command() == expected)

    expected2 = dd("""\
                      ( set -e
                      cd 'dir with spaces'
                      export 'E1=env var with spaces' 'E2=env var "with" '"'"'quotes'"'"''
                      prepend
                      command arg1 arg2 --foo --bam list of 'bam things' --baz k1=v1 'k2=v 2'
                      append
                      ) ; _retval=$?
                      [ $_retval = 0 ] || exit $_retval
                   """)

    assert(sr.quote_command(oneline=False) == expected2)
