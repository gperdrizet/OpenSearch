'''Reader class for CirrusSearch JSON lines data.'''

import json

class CirrusSearchReader():
    '''Class to XML objects from CirrusSearch dump.'''

    def __init__(self, parse_workers: int):

        # Add empty callback function
        self.callback=self._callback_placeholder

        # Buffer to accumulate header and content before
        # sending to parser's input queue
        self.buffer = []

        # Start article count
        self.status_count=['running', 0]

        # Number of parse workers that need to see the
        # done signal when we are finished
        self.parse_workers=parse_workers

    def _callback_placeholder(self, _):
        '''Placeholder for callback functions. Exists to allow 
        instantiation of the reader before we know what callback
        we are going to use.'''
        return

    def read_line(self, line):
        '''Accumulates lines from JSON lines data until buffer
        is full, then flushed buffer to parser input queue.'''

        # Check for done signal from stream reader, when
        # we find it, put done in the buffer and flush
        if line == 'done':

            self.status_count[0]='done'

            for _ in range(self.parse_workers):
                self.buffer.extend(['done','done'])
                self.flush_buffer()

        # If it's not the done signal, process it
        else:

            # Convert line to dictionary
            line=json.loads(line)

            # Add it to the buffer
            self.buffer.append(line)

            # If we have two lines in the buffer
            # flush it
            if len(self.buffer) == 2:

                self.flush_buffer()

                # Update article count
                self.status_count[1] += 1

    def flush_buffer(self):
        '''Sends contents of buffer, along with count of articles
        read to input queue.'''

        # Add the article number and put the buffer
        # contents into the parser input queue
        self.buffer.append(self.status_count)
        self.callback(self.buffer)

        # Clear the buffer
        self.buffer = []
        