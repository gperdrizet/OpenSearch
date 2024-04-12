'''Reader class for CirrusSearch JSON lines data.'''

import json

class CirrusSearchReader():
    '''Class to XML objects from CirrusSearch dump.'''

    def __init__(self):

        # Add empty callback function
        self.callback=self._callback_placeholder

        # Buffer to accumulate header and content before
        # sending to parser's input queue
        self.buffer = []

        # Start article count
        self.status_count=0

    def _callback_placeholder(self, _):
        '''Placeholder for callback functions. Exists to allow 
        instantiation of the reader before we know what callback
        we are going to use.'''
        return

    def read_line(self, line):
        '''Accumulates lines from JSON lines data until buffer
        is full, then flushed buffer to parser input queue.'''

        # Convert line to dict
        line=json.loads(line)

        # Add it to the buffer
        self.buffer.append(line)

        # If we have two lines in the buffer
        # flush it
        if len(self.buffer) == 2:
            self.flush_buffer()

    def flush_buffer(self):
        '''Sends contents of buffer, along with count of articles
        read to input queue.'''

        # Add the article number and put the buffer
        # contents into the parser input queue
        self.buffer.append(self.status_count)
        self.callback(self.buffer)

        # Clear the buffer
        self.buffer = []

        # Update article count
        self.status_count += 1
        