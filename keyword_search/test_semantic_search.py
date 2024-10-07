'''Simple command line utility to test semantic search on embeddings.'''

from opensearchpy import OpenSearch
from wikisearch import config

def run(test_search_index: str) -> None:
    '''Simple command line utility to try out searching'''

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

    # Loop forever
    while True:

        # Get query from user
        q=input("Search query: ")

        # Construct OpenSearch query
        query={
            "query": {
                "nested": {
                "score_mode": "max",
                "path": "text_chunk_embedding",
                "query": {
                    "neural": {
                    "text_chunk_embedding.knn": {
                        "query_text": q,
                        "model_id": config.MODEL_ID
                    }
                    }
                }
                }
            }
            }

        # Do the search
        response=client.search(
            body=query,
            index=test_search_index
        )

        # Print the result
        print(f'\nResult is: {type(response)}')
        print('Response contains:')

        for key, value in response.items():

            print(f'{key}: is type {type(value)}')

        print('\n Hits contains:')

        for hit_key in response['hits'].keys():
            print(f'  {hit_key}')

        print(f"\n Hits hits is: {type(response['hits']['hits'])}")
        print(f" Hits has {len(response['hits']['hits'])} elements")
        print(f" Hits first element contains: {response['hits']['hits'][0].keys()}")

        print(f" _source is {type(response['hits']['hits'][0]['_source'])}")
        print(f" _source contains: {response['hits']['hits'][0]['_source'].keys()}")

        print(f" text_chunk is {type(response['hits']['hits'][0]['_source']['text_chunk'])}")
        print(f" text_chunk has {len(response['hits']['hits'][0]['_source']['text_chunk'])} elements")
        
        print(f" _score is {type(response['hits']['hits'][0]['_score'])}")
        print(f" _score: {response['hits']['hits'][0]['_score']}")