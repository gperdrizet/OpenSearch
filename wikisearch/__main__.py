import argparse
from wikisearch import parse_dump
from wikisearch import search

if __name__ == '__main__':

    # Set-up command line argument parser
    parser=argparse.ArgumentParser(
        prog='wikisearch.py',
        description='Run wikisearch tasks',
        formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80)
    )

    # Add argument for task to run
    parser.add_argument(
        'task',
        choices=['update_dump', 'parse_dump', 'search'],
        help='Task to run'
    )

    # Add argument for parsed output destination
    parser.add_argument(
        '--output',
        required=False,
        choices=['file', 'opensearch'],
        default='file',
        help='Where to output parsed articles'
    )

    args=parser.parse_args()

    # Decide what to do and how to do it based on
    # user provided arguments
    if args.task == 'parse_dump':
        parse_dump.run(args.output)

    elif args.task == 'update_dump':
        pass

    elif args.task == 'search':
        search.run()

    else:
        print('Unrecognized task, exiting.')