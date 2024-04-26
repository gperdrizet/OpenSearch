'''Function to get and parse command line arguments. Also, sets 
some sane default values based on the task being run.'''

import argparse
from wikisearch import config

def parse_arguments() -> argparse.Namespace:
    '''Instantiates the command line argument parser
    Adds and parses arguments, returns parsed arguments.'''

    # Set-up command line argument parser
    parser=argparse.ArgumentParser(
        prog='wikisearch.py',
        description='Run wikisearch tasks',
        formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80)
    )

    # Add argument for task to run
    parser.add_argument(
        'task',
        choices=['process_xml_dump', 'process_cs_dump', 'test_search'],
        help='[update_xml_dump, process_xml_dump, process_cs_dump, test_search]',
        metavar='TASK_NAME_STRING'
    )

    # Add argument to specify name of input dump file, set
    # default value to None so we can add the correct path
    # for the dump type after arguments are parsed
    parser.add_argument(
        '--dump',
        required=False,
        default=None,
        help='path to input dump file',
        metavar=''
    )

    # Add argument to specify name of target OpenSearch index for insert
    # or testing search, set default value to None so we can add a sane
    # default after we parse the arguments know the task we are running
    parser.add_argument(
        '--index',
        required=False,
        default=None,
        help='name of OpenSearch index for insert or search test',
        metavar=''
    )

    # Add argument to specify number of parse workers, set default value
    # to None so we can add a sane default after we parse the arguments
    # know the task we are running
    parser.add_argument(
        '--parse_workers',
        required=False,
        default=None,
        help='number of parse workers to spawn',
        metavar=''
    )

    # Add argument to specify number of output workers, set default value
    # to None so we can add a sane default after we parse the arguments
    # know the task we are running
    parser.add_argument(
        '--output_workers',
        required=False,
        default=None,
        help='number of output workers to spawn',
        metavar=''
    )

    # Add argument to specify bulk upsert batch size
    parser.add_argument(
        '--upsert_batch',
        required=False,
        default=100,
        help='number of documents per bulk upsert batch',
        metavar=''
    )

    # Add argument for parsed output destination
    parser.add_argument(
        '--output',
        required=False,
        choices=['file', 'opensearch'],
        default='file',
        help='where to output parsed articles: [file, opensearch]',
        metavar=''
    )

    args=parser.parse_args()

    # Set task dependent defaults unless the user has supplied alternatives

    # Task dependent defaults for xml dump processing
    if args.task == 'process_xml_dump':
        if args.dump is None:
            args.dump=config.XML_INPUT_FILE

        if args.index is None:
            args.index=config.XML_INDEX

        if args.parse_workers is None:
            args.parse_workers=config.XML_PARSE_WORKERS

        if args.output_workers is None:
            args.output_workers=config.XML_OUTPUT_WORKERS

    # Task dependent defaults for CirrusSearch dump processing
    if args.task == 'process_cs_dump':
        if args.dump is None:
            args.dump=config.CS_INPUT_FILE

        if args.index is None:
            args.index=config.CS_INDEX

        if args.parse_workers is None:
            args.parse_workers=config.CS_PARSE_WORKERS

        if args.output_workers is None:
            args.output_workers=config.CS_OUTPUT_WORKERS

    # Task dependent defaults for search testing
    if args.task == 'search_test':
        if args.index is None:
            args.index=config.XML_INDEX

    return args