'''Collection of functions for loading data into OpenSearch.'''

# Standard imports
import time

# PyPI imports
from opensearchpy import OpenSearch # pylint: disable = import-error

# Internal imports
import semantic_search.configuration as config

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

        index_body={
            "settings": {
                "index": {
                "number_of_shards": 3,
                "knn": "true",
                "knn.algo_param.ef_search": 100
                }
            },
            "mappings": {
                "properties": {
                    "text_embedding": {
                        "type": "knn_vector",
                        "dimension": 768,
                        "space_type": "l2",
                        "method": {
                            "name": "hnsw",
                            "engine": "lucene",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 24
                            }
                        }
                    }
                }
            }
        }

        _=client.indices.create(index_name, body=index_body)

    # Close client
    client.close()


def index_batch(client, bulk_insert_batch: list, source_config: dict, record_count: int):
    '''Formats bulk insert batch for indexing and submits it to OpenSearch'''

    # Build the requests
    knn_requests=[]

    for embedded_text in bulk_insert_batch:

        record_count+=1

        knn_request_header={
            'index': {
                '_index': source_config['target_index_name'],
                '_id': record_count
            }
        }

        knn_requests.append(knn_request_header)

        request_body={'text_embedding': embedded_text}

        knn_requests.append(request_body)

        # Do the insert
        _=client.bulk(knn_requests)

    # Clear the batch
    bulk_insert_batch=[]

    # Return the updated record count
    return record_count

