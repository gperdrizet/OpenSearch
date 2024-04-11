from bz2 import BZ2File
from gzip import GzipFile

from wikisearch import process_dump
from wikisearch import test_search

from wikisearch.classes.xml_reader import XMLReader
from wikisearch.classes.cirrussearch_reader import CirrusSearchReader

from wikisearch.functions.IO_functions import make_arg_parser, consume_xml_stream, consume_json_lines_stream
from wikisearch.functions.parsing_functions import parse_xml_article, parse_cirrussearch_article

if __name__ == '__main__':

    # Set and parse command line args
    args=make_arg_parser()

    # Decide what to do and how to do it based on
    # user provided arguments

    # Parses xml dump. Can insert into OpenSearch or
    # write article text to files depending on value
    # of output argument
    if args.task == 'process_xml_dump':

        # Pick the input file - set default or use the command line
        # argument value if set
        if args.input == None:
            input_file='wikisearch/data/enwiki-20240320-pages-articles-multistream.xml.bz2'

        else:
            input_file=args.input

        # Pick the index name - set default or use the command line
        # argument value if set
        if args.index == None:
            index_name='enwiki-xml'

        else:
            index_name=args.index
        
        # Start the run
        process_dump.run(
            input_stream=BZ2File(input_file),
            stream_reader=consume_xml_stream,
            index_name=index_name,
            output_destination=args.output,
            reader_instance=XMLReader(),
            parser_function=parse_xml_article,
            parse_workers=15,
            upsert_workers=1
        )

    # Bulk inserts a CirrusSearch index directly
    # into OpenSearch
    elif args.task == 'process_cs_dump':

        # Pick the input file - set default or use the command line
        # argument value if set
        if args.input == None:
            input_file='wikisearch/data/enwiki-20240401-cirrussearch-content.json.gz'

        else:
            input_file=args.input

        # Pick the index name - set default or use the command line
        # argument value if set
        if args.index == None:
            index_name='enwiki-cs'

        else:
            index_name=args.index

        # Start the run
        process_dump.run(
            input_stream=GzipFile(input_file),
            stream_reader=consume_json_lines_stream,
            index_name=index_name,
            output_destination=args.output,
            reader_instance=CirrusSearchReader(),
            parser_function=parse_cirrussearch_article,
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