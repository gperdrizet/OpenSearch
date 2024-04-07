import argparse
from wikisearch import parse_xml_dump
from wikisearch import insert_cs_dump
from wikisearch import parse_cs_dump
from wikisearch import test_search

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
        choices=['update_xml_dump', 'parse_xml_dump', 'parse_cs_dump', 'test_search'],
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

    # Add argument to specify name of target 
    # OpenSearch index for insert
    parser.add_argument(
        '--index',
        required=False,
        default='enwiki-xml',
        help='Name of OpenSearch index for insert'
    )

    # Add argument to specify name of input dump file
    parser.add_argument(
        '--input',
        required=False,
        default='wikisearch/data/enwiki-20240320-pages-articles-multistream.xml.bz2',
        help='Path to input dump file'
    )

    args=parser.parse_args()

    # Decide what to do and how to do it based on
    # user provided arguments

    # Parses xml dump. Can insert into OpenSearch or
    # write article text to files depending on value
    # of output argument
    if args.task == 'parse_xml_dump':
        
        parse_xml_dump.run(
            input_file=args.input,
            index_name=args.index,
            output_destination=args.output
        )

    # Planned - gets new xml dump
    elif args.task == 'update_xml_dump':
        pass

    # Bulk inserts a CirrusSearch index directly
    # into OpenSearch
    elif args.task == 'parse_cs_dump':

        parse_cs_dump.run(
            input_file=args.input,
            index_name=args.index
        )

    # Runs interactive command line search utility
    elif args.task == 'test_search':
        test_search.run()

    else:
        print('Unrecognized task, exiting.')