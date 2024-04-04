import argparse
from wikisearch import parse_dump

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='wikisearch.py',
        description='Run wikisearch tasks',
        formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=80)
    )

    parser.add_argument(
        'task',
        choices=['update_dump', 'parse_dump'],
        help='Task to run'
    )

    parser.add_argument(
        '--output',
        required=False,
        choices=['file', 'opensearch'],
        default=['file'],
        help='Where to output parsed articles'
    )

    args = parser.parse_args()

    if args.task == 'parse_dump':
        parse_dump.run(args.output)

    elif args.task == 'update_dump':
        pass

    else:
        print('Unrecognized task, exiting.')