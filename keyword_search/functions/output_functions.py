'''Functions for handling output of data to files or indexing into OpenSearch'''

from __future__ import annotations
import time
from opensearchpy import exceptions
from wikisearch import config
import wikisearch.functions.helper_functions as helper_funcs

def output_selector(
    args: dict,
    output_queue: multiprocessing.Queue, # type: ignore
):
    
    '''Selects correct output endpoint for data and 
    sends the output queue to it'''

    # Send output to file
    if args.output == 'file':

        # Set article source for save file path based on task
        article_source='unknown'

        if args.task == 'process_xml_dump':
            article_source='xml'

        elif args.task == 'process_cs_dump':
            article_source='cirrussearch'

        _=write_file(
            output_queue=output_queue,
            article_source=article_source,
            parse_workers=args.parse_workers,
            resume=args.resume
        )
    
    # Send the output to the OpenSearch bulk indexer
    if args.output == 'opensearch':

        _=bulk_index_articles(
            output_queue=output_queue,
            batch_size=args.upsert_batch,
            parse_workers=args.parse_workers,
            resume=args.resume
        )

def write_file(
    output_queue: multiprocessing.Queue, # type: ignore
    article_source: str,
    parse_workers: int
) -> None:

    '''Takes documents from parser's output queue, writes to file.'''

    # Construct output path
    output_path=f'wikisearch/data/articles/{article_source}'

    # Counter to track how many done signals we have received
    done_count=0

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Check for done signal from parser and count it.
        if output[0] == 'done':
            done_count+=1

            # If we have seen a done signal from each parse worker, return
            if done_count == parse_workers:
                return

        # If the queue item is not a done signal, process it
        else:

            # Extract title and text
            title=output[1]['doc']['title']
            content=output[1]['doc']['text']

            # Format page title for use as a filename
            file_name=title.replace(' ', '_')
            file_name=file_name.replace('/', '-')

            # Construct output path
            output = f'{output_path}/{file_name}'

            # Save article to a file
            with open(output, 'w', encoding='utf-8') as text_file:
                text_file.write(f'{title}\n{content}')


def bulk_index_articles(
    output_queue: multiprocessing.Queue, # type: ignore
    batch_size: int,
    parse_workers: int
) -> None:
    
    '''Batch index documents and insert in to OpenSearch from 
    parser output queue'''

    # Start the OpenSearch client and create the index
    client=helper_funcs.start_client()

    # List to collect articles from queue until we have enough for a batch
    incoming_articles = []

    # Counter to track how many done signals we have received
    done_count=0

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Check for done signal from parser and count it.
        if output[0] == 'done':
            done_count+=1

            # If we have seen a done signal from each parse worker, return
            if done_count == parse_workers:
                return

        # If the queue item is not a done signal add it to batch
        else:

            # Add it to batch
            incoming_articles.extend(output)

            # Once we have a full batch, send it to the opensearch bulk insert function
            # the divide by 2 is necessary because each article is represented by two
            # elements, the header and the content
            if len(incoming_articles) // 2 >= int(batch_size):

                # Once we have all of the articles formatted and collected, insert them
                # catching any connection timeout errors from OpenSearch
                try:

                    # Do the insert
                    _=client.bulk(incoming_articles)

                    # Empty the list of articles to collect the next batch
                    incoming_articles = []

                # If we catch an connection timeout or transport error, sleep for a bit and
                # don't clear the cached articles before continuing the loop
                except (exceptions.ConnectionTimeout, exceptions.TransportError):
                    time.sleep(10)