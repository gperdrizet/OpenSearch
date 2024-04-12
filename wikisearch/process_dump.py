'''Processes dump in XML or CirrusSearch format, indexes to OpenSearch
or writes documents to file.'''

from typing import Union, Callable
from bz2 import BZ2File
from gzip import GzipFile
from threading import Thread
from multiprocessing import Manager, Process
from wikisearch.classes.cirrussearch_reader import CirrusSearchReader
import wikisearch.functions.io_functions as io_funcs

def run(
    input_stream: Union[GzipFile, BZ2File],
    stream_reader: Callable,
    index_name: str,
    output_destination: str,
    reader_instance: CirrusSearchReader,
    parser_function: Callable,
    parse_workers: int,
    upsert_workers: int
) -> None:

    '''Main function to parse and upsert dumps'''

    # Start multiprocessing manager
    manager=Manager()

    # Set-up queues
    output_queue=manager.Queue(maxsize=2000)
    input_queue=manager.Queue(maxsize=2000)

    # Add the input queue's put function to the reader class's callback method
    reader_instance.callback=input_queue.put

    # Initialize the target index
    io_funcs.initialize_index(index_name)

    # Start the status monitor printout
    Thread(
        target=io_funcs.display_status,
        args=(input_queue, output_queue, reader_instance)
    ).start()

    # Start parser jobs
    for _ in range(parse_workers):

        Process(
            target=parser_function,
            args=(input_queue, output_queue, index_name)
        ).start()

    # Target the correct output function

    # Start writer jobs
    for _ in range(upsert_workers):

        # Save to file
        if output_destination == 'file':

            write_process=Process(
                target=io_funcs.write_file,
                args=(output_queue, 'cirrus_search')
            )

        # Insert to OpenSearch
        elif output_destination == 'opensearch':

            write_process=Process(
                target=io_funcs.bulk_index_articles,
                args=(output_queue,)
            )

        # Not sure what to do - warn user
        else:
            print(f'Unrecognized output destination: {output_destination}.')

        # Start the output writer thread
        write_process.start()

    # Send the data stream to the reader
    stream_reader(input_stream, reader_instance)
