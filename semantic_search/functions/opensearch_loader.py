'''Collection of functions for loading data into OpenSearch.'''

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
                "number_of_shards": 3,
                "index.knn": "true",
                "default_pipeline": f'{config.INGEST_PIPELINE_ID}'
            },
            "mappings": {
                "properties": {
                    "text_embedding": {
                        "type": "knn_vector",
                        "dimension": 768,
                        "method": {
                        "engine": "lucene",
                        "space_type": "l2",
                        "name": "hnsw",
                        "parameters": {}
                        }
                    },
                    "text": {
                        "type": "text"
                    }
                }
            }
        }

        _=client.indices.create(index_name, body=index_body)

    # Close client
    client.close()
