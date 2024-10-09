'''Contains extra functions related to Luigi data pipeline.'''

# Standard imports
import pathlib

# Internal imports
import semantic_search.configuration as config

def force_from(data_source: str, task_name: str = None):
    '''Forces all to be re-run starting with given task by removing their output'''

    # Dictionary of string task names and their output files
    tasks = {
        'ExtractRawData': f'{config.DATA_PATH}/{data_source}/{config.EXTRACTION_SUMMARY}'
    }

    # Flag to determine if we remove each file or not
    remove_output=False

    # Loop on the task dictionary
    for task, output_file in tasks.items():

        # When we find the task, flip the value of remove_output to True
        # so that we will remove the output files for this and all
        # subsequent tasks
        if task == task_name:
            remove_output = True

        # If the flag has been flipped remove the output file
        if remove_output is True:
            pathlib.Path(output_file).unlink(missing_ok = True)