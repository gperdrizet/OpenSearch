'''Contains various functions for handling input.output of data
and related helper functions.'''

import time
import multiprocessing
import argparse
from typing import Callable
from bz2 import BZ2File
from gzip import GzipFile
from xml import sax
from opensearchpy import OpenSearch, exceptions
from wikisearch import config
from wikisearch.classes.cirrussearch_reader import CirrusSearchReader

def make_arg_parser() -> argparse.Namespace:
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
    if args.task == 'process_xml_dump':
        if args.dump is None:
            args.dump=config.XML_INPUT_FILE

        if args.index is None:
            args.index=config.XML_INDEX

        if args.parse_workers is None:
            args.parse_workers=config.XML_PARSE_WORKERS

        if args.output_workers is None:
            args.output_workers=config.XML_OUTPUT_WORKERS

    if args.task == 'process_cs_dump':
        if args.dump is None:
            args.dump=config.CS_INPUT_FILE

        if args.index is None:
            args.index=config.CS_INDEX

        if args.parse_workers is None:
            args.parse_workers=config.CS_PARSE_WORKERS

        if args.output_workers is None:
            args.output_workers=config.CS_OUTPUT_WORKERS

    if args.task == 'search_test':
        if args.index is None:
            args.index=config.XML_INDEX

    return args

def consume_xml_stream(
    input_stream: BZ2File,
    reader_instance: Callable
) -> None:

    '''Takes input data stream from file passes it to
    the reader via xml's sax parser.'''

    sax.parse(input_stream, reader_instance)

def consume_json_lines_stream(
    input_stream: GzipFile,
    reader_instance: Callable
) -> None:

    '''Takes input stream containing json lines data, passes it
    line by line to reader class instance'''

    # Loop on lines
    for line in input_stream:

        reader_instance.read_line(line)

def bulk_index_articles(
    output_queue: multiprocessing.Queue,
    batch_size: int
) -> None:
    '''Batch index documents and insert in to OpenSearch from 
    parser output queue'''

    # Start the OpenSearch client and create the index
    client=start_client()

    # List to collect articles from queue until we have enough for a batch
    incoming_articles = []

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Add it to batch
        incoming_articles.extend(output)

        # Once we have 500 articles, process them and index
        if len(incoming_articles) / 2 >= batch_size:

            # Once we have all of the articles formatted and collected, insert them
            # catching any connection timeout errors from OpenSearch
            try:

                # Do the insert
                _=client.bulk(incoming_articles)

                # Empty the list of articles to collect the next batch
                incoming_articles = []

            # If we catch an connection timeout, sleep for a bit and
            # don't clear the cached articles before continuing the loop
            except exceptions.ConnectionTimeout:

                time.sleep(10)


def index_articles(
    output_queue: multiprocessing.Queue,
    index_name: str
) -> None:

    '''Indexes articles one at a time from output queue into OpenSearch'''

    # Start the OpenSearch client and create the index
    client=start_client()

    # Counter var for document id
    document_id=0

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Extract filename and text
        page_title=output[0]
        text=output[1]

        document={
            'title': page_title,
            'text': text
        }

        _=client.index(
            index=index_name,
            body=document,
            id=document_id
        )

        document_id+=1

def write_file(
    output_queue: multiprocessing.Queue,
    article_source: str
) -> None:

    '''Takes documents from parser's output queue, writes to file.'''

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Extract title and text
        title=output[1]['doc']['title']
        #content=output[1]['text']
        content=str(output[0]) + '\n' + str(output[1])

        # Format page title for use as a filename
        file_name=title.replace(' ', '_')
        file_name=file_name.replace('/', '-')

        # Construct output path
        output = f'wikisearch/data/articles/{article_source}/{file_name}'

        # Save article to a file
        with open(output, 'w', encoding="utf-8") as text_file:
            text_file.write(f'{title}\n{content}')


def display_status(
    input_queue: multiprocessing.Queue,
    output_queue: multiprocessing.Queue,
    reader_instance: CirrusSearchReader,
) -> None:

    '''Prints queue sizes and articles read every second'''

    print('\n\n\n')

    while True:
        print('\033[F\033[F\033[F', end='')
        print(f'Input queue size: {input_queue.qsize()}')
        print(f'Output queue size: {output_queue.qsize()}')
        print(f'Reader count: {reader_instance.status_count}')
        time.sleep(1)

def start_client() -> OpenSearch:

    '''Fires up the OpenSearch client'''

    # Set host and port
    host='localhost'
    port=9200

    # Create the client with SSL/TLS and hostname verification disabled.
    client=OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=False,
        timeout=30,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    return client

def initialize_index(index_name: str) -> None:

    '''Set-up OpenSearch index. Deletes index if it already exists
    at run start. Creates new index for run.'''

    client=start_client()

    # Delete the index we are trying to create if it exists
    if client.indices.exists(index=index_name):
        _=client.indices.delete(index=index_name)

    # Create the target index if it does not exist
    if client.indices.exists(index=index_name) is False:

        # Create index
        index_body={
            'settings': {
                'index': {
                    'number_of_shards': 3 # Generic advice is 10-50 GB of data per shard
                }
            }
        }

        _=client.indices.create(index_name, body=index_body)

    # Close client
    client.close()
