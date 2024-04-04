import time
import multiprocessing

def write_file(
    output_queue: multiprocessing.Queue,
    shutdown: bool
) -> None:

    while not (shutdown and output_queue.empty()):

        # Get article from queue
        output=output_queue.get()

        # Extract filename and text
        filename=output[0]
        text=output[1]

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