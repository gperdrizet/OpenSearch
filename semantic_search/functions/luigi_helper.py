'''Contains extra functions related to Luigi data pipeline.'''

# Standard imports
import pathlib

# Internal imports
import semantic_search.configuration as config

def force_from(data_dir: str, task_name: str = None):
    '''Forces all to be re-run starting with given task by removing their output'''

    # Dictionary of string task names and their output files
    tasks = {
        'ExtractData': [
            f'{config.DATA_PATH}/{data_dir}/{config.EXTRACTION_SUMMARY}',
            f'{config.DATA_PATH}/{data_dir}/{config.EXTRACTED_TEXT}'
        ],
        'ParseData': [
            f'{config.DATA_PATH}/{data_dir}/{config.PARSE_SUMMARY}',
            f'{config.DATA_PATH}/{data_dir}/{config.PARSED_TEXT}'
        ],
        'EmbedData': [
            f'{config.DATA_PATH}/{data_dir}/{config.EMBEDDING_SUMMARY}',
            f'{config.DATA_PATH}/{data_dir}/{config.EMBEDDED_TEXT}'
        ],
        'LoadData': [f'{config.DATA_PATH}/{data_dir}/{config.LOAD_SUMMARY}']
    }

    # Flag to determine if we remove each file or not
    remove_output=False

    # Loop on the task dictionary
    for task, output_files in tasks.items():

        # When we find the task, flip the value of remove_output to True
        # so that we will remove the output files for this and all
        # subsequent tasks
        if task == task_name:
            remove_output = True

        # If the flag has been flipped remove the output file
        if remove_output is True:
            for output_file in output_files:
                pathlib.Path(output_file).unlink(missing_ok = True)
