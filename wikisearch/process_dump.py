from threading import Thread
from multiprocessing import Manager, Process
from wikisearch.functions.IO_functions import initialize_index, display_status, write_file, bulk_index_articles

def run(
    input_stream,
    stream_reader,
    index_name: str,
    output_destination: str,
    reader_instance,
    parser_function,
    parse_workers,
    upsert_workers
) -> None:
    
    '''Main function to parse and upsert dumps'''

    # Start multiprocessing manager
    manager=Manager()

    # Set-up queues
    output_queue=manager.Queue(maxsize=2000)
    input_queue=manager.Queue(maxsize=2000)

    # Add the input queue's put function to the reader class's 
    # callback method
    reader_instance.callback=input_queue.put

    # Initialize the target index
    initialize_index(index_name)

    # Start the status monitor printout
    status=Thread(
        target=display_status, 
        args=(input_queue, output_queue, reader_instance)
    )

    status.start()

    # Start parser jobs
    for _ in range(parse_workers):

        parse_process=Process(
            target=parser_function, 
            args=(input_queue, output_queue, index_name)
        )

        parse_process.start()

    # Target the correct output function

    # Start writer jobs
    for _ in range(upsert_workers):

        # Save to file
        if output_destination == 'file':

            write_process=Process(
                target=write_file, 
                args=(output_queue, 'cirrus_search')
            )

        # Insert to OpenSearch
        elif output_destination == 'opensearch':

            write_process=Process(
                target=bulk_index_articles, 
                args=(output_queue, index_name)
            )

        # Not sure what to do - warn user
        else:
            print(f'Unrecognized output destination: {output_destination}.')

        # Start the output writer thread
        write_process.start()

    # Send the data stream to the reader
    stream_reader(input_stream, reader_instance)
