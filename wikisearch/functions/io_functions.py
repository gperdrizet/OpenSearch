'''Contains various functions for handling input.output of data
and related helper functions.'''

from __future__ import annotations
import time
from typing import Callable
from xml import sax
from opensearchpy import exceptions
import wikisearch.functions.helper_functions as helper_funcs

def bulk_index_articles(
    output_queue: multiprocessing.Queue, # type: ignore
    batch_size: int
) -> None:
    '''Batch index documents and insert in to OpenSearch from 
    parser output queue'''

    # Start the OpenSearch client and create the index
    client=helper_funcs.start_client()

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

def write_file(
    output_queue: multiprocessing.Queue, # type: ignore
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