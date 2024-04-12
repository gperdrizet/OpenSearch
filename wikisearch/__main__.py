'''Main function for starting wikisearch'''

from bz2 import BZ2File
from gzip import GzipFile

from wikisearch import process_dump
from wikisearch import test_search

from wikisearch.classes.xml_reader import XMLReader
from wikisearch.classes.cirrussearch_reader import CirrusSearchReader

import wikisearch.functions.io_functions as io_funcs
import wikisearch.functions.parsing_functions as parse_funcs

if __name__ == '__main__':

    # Set and parse command line args
    args=io_funcs.make_arg_parser()

    # Decide what to do and how to do it based on
    # user provided arguments

    # Parses xml dump. Can insert into OpenSearch or
    # write article text to files depending on value
    # of output argument
    if args.task == 'process_xml_dump':

        # Start the run
        process_dump.run(
            input_stream=BZ2File(args.xml_input),
            stream_reader=io_funcs.consume_xml_stream,
            index_name=args.xml_index,
            output_destination=args.output,
            reader_instance=XMLReader(),
            parser_function=parse_funcs.parse_xml_article,
            parse_workers=15,
            upsert_workers=1
        )

    # Bulk inserts a CirrusSearch index directly
    # into OpenSearch
    elif args.task == 'process_cs_dump':

        # Start the run
        process_dump.run(
            input_stream=GzipFile(args.cs_input),
            stream_reader=io_funcs.consume_json_lines_stream,
            index_name=args.cs_index,
            output_destination=args.output,
            reader_instance=CirrusSearchReader(),
            parser_function=parse_funcs.parse_cirrussearch_article,
            parse_workers=1,
            upsert_workers=10
        )

    # Runs interactive command line search utility
    elif args.task == 'test_search':
        test_search.run()

    # Planned - gets new xml dump
    elif args.task == 'update_xml_dump':
        pass

    else:
        print('Unrecognized task, exiting.')
