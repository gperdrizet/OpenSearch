import time
import multiprocessing
from opensearchpy import OpenSearch
from wikisearch.classes.wikireader import WikiReader

def bulk_index_articles(
    output_queue: multiprocessing.Queue,
    index_name: str
) -> None:
    '''Batch index documents and insert in to OpenSearch from 
    parser output queue'''

    # Start the OpenSearch client and create the index
    client=start_client(index_name)

    # List to collect articles from queue until we have enough for a batch
    incoming_articles = []

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Add it to batch
        incoming_articles.extend(output)

        # Once we have 500 articles, process them and index
        if len(incoming_articles) / 2 == 500:

            # Once we have all of the articles formatted and collected, insert them
            _=client.bulk(incoming_articles)

            # Empty the list of articles to collect the next batch
            incoming_articles = []


def index_articles(
    output_queue: multiprocessing.Queue,
    index_name: str
) -> None:
    
    '''Indexes articles one at a time from output queue into OpenSearch'''

    # Start the OpenSearch client and create the index
    client=start_client(index_name)

    # Counter var for document id
    id=0

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
            id=id,
            refresh=True
        )

        id+=1

def write_file(
    output_queue: multiprocessing.Queue,
    article_source: str
) -> None:

    # Loop forever
    while True:

        # Get article from queue
        output=output_queue.get()

        # Extract title and text
        title=output[1]['title']
        content=output[1]['text']

        # Format page title for use as a filename
        file_name=title.replace(' ', '_')
        file_name=file_name.replace('/', '-')

        # Save article to a file
        with open(f"wikisearch/data/articles/{article_source}/{file_name}", 'w') as text_file:
            text_file.write(f'{title}\n{content}')


def display_status(
    input_queue: multiprocessing.Queue, 
    output_queue: multiprocessing.Queue, 
    reader: WikiReader
) -> None:
    
    '''Prints queue sizes every second'''

    print('\n\n\n')

    while True:
        print('\033[F\033[F\033[F', end='')
        print(f'Input queue size: {input_queue.qsize()}')
        print(f'Output queue size: {output_queue.qsize()}')
        print(f'Reader count: {reader.status_count}')
        time.sleep(1)

def start_client(index_name: str) -> OpenSearch:

    # Set host and port
    host='localhost'
    port=9200

    # Create the client with SSL/TLS and hostname verification disabled.
    client=OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=True, # enables gzip compression for request bodies
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # Delete the index we are trying to create if it exists
    if client.indices.exists(index=index_name):
        _=client.indices.delete(index=index_name)

    # Create index
    index_body={
        'settings': {
            'index': {
                'number_of_shards': 2 # Generic advice is 10-50 GB of data per shard
            }
        }
    }

    _=client.indices.create(index_name, body=index_body)

    return client