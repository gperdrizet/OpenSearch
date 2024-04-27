'''Processes dump in XML or CirrusSearch format, indexes to OpenSearch
or writes documents to file.'''

from __future__ import annotations
from typing import Union, Callable
from threading import Thread
from multiprocessing import Manager, Process
import wikisearch.functions.helper_functions as helper_funcs
import wikisearch.functions.output_functions as output_funcs

def run(
    input_stream: Union[GzipFile, BZ2File], # type: ignore
    stream_reader: Callable,
    reader_instance: Union[XMLReader,CirrusSearchReader], # type: ignore
    parser_function: Callable,
    args: dict
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
    helper_funcs.initialize_index(args.index)

    # # Start the status monitor printout
    # Thread(
    #     target=helper_funcs.display_status,
    #     args=(input_queue, output_queue, reader_instance)
    # ).start()

    # Start parser jobs
    for _ in range(args.parse_workers):

        Process(
            target=parser_function,
            args=(input_queue, output_queue, args.index)
        ).start()

    # Start writer jobs
    for _ in range(args.output_workers):

        # Send output queue to output selector so write traffic
        # gets sent to the correct place
        write_process=Process(
            target=output_funcs.output_selector,
            args=(args, output_queue)
        )

        # Start the output writer thread
        write_process.start()

    # Send the data stream to the reader
    stream_reader(input_stream, reader_instance)
