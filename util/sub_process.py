import io
import sys
import os
import subprocess
import selectors
import logging


# capture subprocess output and write the stdout as it comes in
def capture_subprocess_output(subprocess_args):
    # Start subprocess
    # bufsize = 1 means output is line buffered
    # universal_newlines = True is required for line buffering
    process = subprocess.Popen(subprocess_args,
                               bufsize=1,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               universal_newlines=True,
                               shell=False,
                               errors='replace')

    # Create callback function for process output
    buf = io.StringIO()

    def handle_output(stream, _mask):
        line = stream.readline()
        while line:
            buf.write(line)
            sys.stdout.write(line)
            line = stream.readline()

    # Register callback for an "available for read" event from subprocess' stdout stream
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, handle_output)

    # Loop until subprocess is terminated
    while process.poll() is None:
        # Wait for events and handle them with their registered callbacks
        events = selector.select()
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)

    # Get process return code
    return_code = process.wait()
    selector.close()

    # Store buffered output
    output = buf.getvalue()
    buf.close()

    return return_code, output


def execute_command(cmd, dry_run=False, success_code=0):
    """
    Execute a single shell command.  pass dry_run=True to print the command instead of running it
    Will raise a runtime error if the command returns an exit code that is different from success_code
    """
    if dry_run:
        logging.info('DRY_RUN: {}'.format(cmd))
        return 0
    else:
        exit_code = os.system(cmd)
        if exit_code != success_code:
            raise(RuntimeError("command '{}' failed with exit code {}".format(cmd, exit_code)))
        return exit_code


def execute_command_with_capture(cmd, dry_run=False, success_code=0):
    """
    execute a single shell command and capture the output.
    pass dry_run=True to print the command instead of running it
    Will raise a runtime error if the command returns an exit code that is different from success_code
    """
    if dry_run:
        logging.info('DRY_RUN: {}'.format(cmd))
        return 0, ""
    else:
        return_code, output = capture_subprocess_output(cmd)
        if return_code != success_code:
            raise(RuntimeError("command '{}' failed with exit code {}".format(cmd, return_code)))
        return return_code, output
