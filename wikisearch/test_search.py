'''Simple command line utility to test OpenSearch index.'''

from opensearchpy import OpenSearch

def run() -> None:
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
            'size': 5,
            'query': {
                'multi_match': {
                    'query': q,
                    'fields': ['title', 'text']
                }
            }
        }

        # Do the search
        response=client.search(
            body=query,
            index='enwiki'
        )

        # Print the result
        print(response)
        