import time
import multiprocessing
from opensearchpy import OpenSearch

def index_articles(
    output_queue: multiprocessing.Queue,
    shutdown: bool
) -> None:

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

    # Create index
    index_name='enwiki'
    index_body={
        'settings': {
            'index': {
                'number_of_shards': 2 # Generic advice is 10-50 GB of data per shard
            }
        }
    }

    response=client.indices.create(index_name, body=index_body)

    # Counter var for document id
    id=0

    # Loop on queue
    while not (shutdown and output_queue.empty()):

        # Get article from queue
        output=output_queue.get()

        # Extract filename and text
        page_title=output[0]
        text=output[1]

        document={
            'title': page_title,
            'text': text
        }

        response=client.index(
            index='enwiki',
            body=document,
            id=id,
            refresh=True
        )

        id+=1

def write_file(
    output_queue: multiprocessing.Queue,
    shutdown: bool
) -> None:

    # Loop on queue
    while not (shutdown and output_queue.empty()):

        # Get article from queue
        output=output_queue.get()

        # Extract title and text
        page_title=output[0]
        text=output[1]

        # Format page title for use as a filename
        filename=page_title.replace(' ', '_')
        filename=filename.replace('/', '-')

        # Save article to a file
        with open(f"wikisearch/data/articles/{filename}", 'w') as text_file:
            text_file.write(text)


def display_status(
    input_queue: multiprocessing.Queue, 
    output_queue: multiprocessing.Queue, 
    reader
) -> None:
    
    '''Prints queue sizes every second'''

    print('\n\n\n')

    while True:
        print('\033[F\033[F\033[F', end='')
        print(f'Input queue size: {input_queue.qsize()}')
        print(f'Output queue size: {output_queue.qsize()}')
        print(f'Reader count: {reader.status_count}')
        time.sleep(1)