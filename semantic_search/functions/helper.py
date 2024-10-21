'''Contains extra functions related to starting/running Luigi data pipeline.'''

# Standard imports
import argparse
import pathlib

# Internal imports
import semantic_search.configuration


def parse_arguments() -> argparse.Namespace:
    '''Instantiates the command line argument parser
    Adds and parses arguments, returns parsed arguments.'''

    # Set-up command line argument parser
    parser=argparse.ArgumentParser(
        prog='OpenSearch semantic search ETL pipeline',
        description='Parses data, recovers text, chunks into batches and inserts embeddings into OpenSearch KNN index.',
        formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80)
    )

    # Argument to specify the data source to process
    parser.add_argument(
        '--data_source',
        required=False,
        default=semantic_search.configuration.DEFAULT_DATA_SOURCE,
        help='data source to process, must match basename of data source configuration file (default: %(default)s)',
        metavar='TASK_NAME'
    )

    # Argument to specify Luigi task to force start execution from
    parser.add_argument(
        '--force_from',
        required=False,
        default=semantic_search.configuration.DEFAULT_FORCE_START,
        choices=['None','ExtractText','ParseText','EmbedText','LoadText'],
        help='force Luigi pipeline to start from a specific task (default: %(default)s, choices: [%(choices)s])',
        metavar='DATA_SOURCE'
    )

    args=parser.parse_args()

    return args


def force_from(data_dir: str, task_name: str = None):
    '''Forces all to be re-run starting with given task by removing their output'''

    # Dictionary of string task names and their output files
    data_path=f'{semantic_search.configuration.DATA_PATH}/{data_dir}'

    tasks = {
        'ExtractText': [
            f'{data_path}/{semantic_search.configuration.EXTRACTION_SUMMARY}',
            f'{data_path}/{semantic_search.configuration.EXTRACTED_TEXT}'
        ],
        'ParseText': [
            f'{data_path}/{semantic_search.configuration.PARSE_SUMMARY}',
            f'{data_path}/{semantic_search.configuration.PARSED_TEXT}'
        ],
        'EmbedText': [
            f'{data_path}/{semantic_search.configuration.EMBEDDING_SUMMARY}',
            f'{data_path}/{semantic_search.configuration.EMBEDDED_TEXT}'
        ],
        'LoadText': [f'{data_path}/{semantic_search.configuration.LOAD_SUMMARY}']
    }

    # Flag to determine if we remove each file or not
    remove_output=False

    # Loop on the task dictionary
    for task, output_files in tasks.items():

        # When we find the task, flip the value of remove_output to True
        # so that we will remove the output files for this and all
        # subsequent tasks
        if task == task_name:
            remove_output=True

        # If the flag has been flipped remove the output file
        if remove_output is True:
            for output_file in output_files:
                pathlib.Path(output_file).unlink(missing_ok=True)
