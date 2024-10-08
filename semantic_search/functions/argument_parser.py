'''Function to get and parse command line arguments. Also, sets 
some sane default values based on the task being run.'''

import argparse
import semantic_search.configuration as config

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
        default=config.DEFAULT_DATA_SOURCE,
        help='data source to process, must match basename of data source configuration file',
        metavar='DATA_SOURCE'
    )

    args=parser.parse_args()

    return args