from subprocess import Popen, PIPE

# NB: copied as is from cement.core.shell. See
# https://github.com/cement/cement/blob/master/cement/utils/shell.py
def exec_cmd(cmd_args, shell=False):
    """
    Execute a shell call using Subprocess.
    
    :param cmd_args: List of command line arguments.
    :type cmd_args: list
    :param shell: See `Subprocess <http://docs.python.org/library/subprocess.html>`_
    :type shell: boolean
    :returns: The (stdout, stderror, return_code) of the command
    :rtype: tuple
    
    Usage:
    
    .. code-block:: python
    
        from ratatosk import shell
        
        stdout, stderr, exitcode = shell.exec_cmd(['echo', 'helloworld'])
        
    """
    proc = Popen(cmd_args, stdout=PIPE, stderr=PIPE, shell=shell)
    (stdout, stderr) = proc.communicate()
    proc.wait()
    return (stdout, stderr, proc.returncode)
