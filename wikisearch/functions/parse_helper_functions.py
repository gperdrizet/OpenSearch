import time

def process_article(aq, shutdown):
    while not (shutdown and aq.empty()):
    
        # Get the page title and the content source from the article queue
        page_title, source = aq.get()

        print(f'{page_title}:\n{source}')

        # At this point, we would need to insert the article into OpenSearch
        # or maybe collect a bunch of parsed articles for bulk insert


def display(aq, reader):
    '''Prints queue sizes every second'''

    while True:
        print(f'Article queue size: {aq.size()}. reader status count: {reader.status_count}')
        time.sleep(1)