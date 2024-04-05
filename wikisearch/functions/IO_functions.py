import time
import multiprocessing
from opensearchpy import OpenSearch

def bulk_index_articles(
    output_queue: multiprocessing.Queue,
    index_name: str,
    shutdown: bool
) -> None:
    '''Batch index documents and insert in to OpenSearch from 
    parser output queue'''

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
    index_body={
        'settings': {
            'index': {
                'number_of_shards': 2 # Generic advice is 10-50 GB of data per shard
            }
        }
    }

    _=client.indices.create(index_name, body=index_body)

    # List to collect articles from queue until we have enough for a batch
    incoming_articles = []

    # Counter for insert id
    article_id = 0

    # Loop on queue
    while not (shutdown and output_queue.empty()):

        # Get article from queue
        output=output_queue.get()

        # Add it to batch
        incoming_articles.append(output)

        # Once we have 50 articles, process them and index
        if len(incoming_articles) == 50:
            
            # Empty list for batch
            batch = []

            for article in incoming_articles:

                # Extract title and text
                page_title=article[0]
                text=article[1]

                # Count it
                article_id+=1

                # Create formatted dicts for the request and the content
                request_header={ "index" : { "_index" : "enwiki", "_id" : article_id } }
                formatted_article={ "title" : page_title, "text" : text }

                # append the new dicts to the existing batch
                batch.append(request_header)
                batch.append(formatted_article)

            # Once we have all of the articles formatted and collected, insert them
            response=client.bulk(batch)

            # Empty the list of articles to collect the next batch
            incoming_articles = []


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