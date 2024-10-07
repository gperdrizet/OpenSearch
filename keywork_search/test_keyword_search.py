'''Simple command line utility to test OpenSearch index.'''

from opensearchpy import OpenSearch

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
            index=test_search_index
        )

        # Print the result
        print(f'Result is: {type(response)}')

        for key, value in response.items():

            if key != 'hits':
                print(f'{key}: {value}')
            
            elif key == 'hits':
                print(f'\nHits contains:')

                for hit, content in response['hits'].items():

                    if hit != 'hits':
                        print(f' {hit}: {content}')

                    elif hit == 'hits':
                        print(f'\n Hit 1 contains:')
                        for hit_key, hit_val in content[1].items():
                            print(f'  {hit_key}')
        