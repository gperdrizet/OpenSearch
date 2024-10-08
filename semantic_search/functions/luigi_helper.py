'''Contains extra functions related to Luigi data pipeline.'''

import pathlib

def force_after(task_name: str = None):
    '''Forces all to be re-run starting with given task by removing their output'''

    # Dictionary of string task names and their output files
    tasks = {

    }

    # Loop on the task dictionary
    remove_output = False

    for task, output_file in tasks.items():

        # When we find the task, flip the value of remove_output to True
        # so that we will remove the output files for this and all
        # subsequent tasks
        if task == task_name:
            remove_output = True

        # If the flag has been flipped remove the output file
        if remove_output is True:
            pathlib.Path(output_file).unlink(missing_ok = True)