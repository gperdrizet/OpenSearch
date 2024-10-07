'''Reader class to take XML data from sax, parse tags
and send to document parser's input queue.'''

from xml.sax import ContentHandler
class XMLReader(ContentHandler):
    '''Class to read tags parsed from bz2 data stream 
    via xml.sax's parser'''

    def __init__(self, parse_workers):
        super().__init__()

        # Add empty callback function
        self.callback=self._callback_placeholder

        # Set some initial values
        self.read_stack=[]
        self.read_text=None
        self.read_title=None
        self.read_namespace=None

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


    def startElement(self, name, attrs):
        '''Takes tag name and stores content, adds to stack'''

        # Decide what to do based on the tag name

        # Empty namespace for new value
        if name == 'ns':
            self.read_namespace=None

        # If we found a page empty the title and text
        # for new values
        elif name == 'page':
            self.read_text=None
            self.read_title=None

        # If we found a title, empty the title string
        elif name == 'title':
            self.read_title=''

        # If we found text, empty the text string
        elif name == 'text':
            self.read_text=''

        # Make sure the opening mediawiki tag gets
        # added to the stack so that we can catch
        # the closing one to end the parse
        elif name == 'mediawiki':
            pass

        # If it's none of those, do nothing
        else:
            return

        # Add the tag to the stack
        self.read_stack.append(name)


    def endElement(self, name):
        '''Takes tag name, routes based on identity'''

        # If we decided not to save this tag, do nothing
        if len(self.read_stack) == 0:
            return

        # If this tag matches the stack, delete
        # and process further
        if name == self.read_stack[-1]:
            del self.read_stack[-1]

        # If it's a page tag
        if name == 'page':

            # That has text
            if self.read_text is not None:

                # And has namespace 0 (article)
                if self.read_namespace == 0:

                    # And is not a redirect page
                    if 'REDIRECT' not in self.read_text.split('\n')[0].upper():

                        # Call the callback to add the article title and text
                        # to the parser's input queue
                        self.callback((self.read_title, self.read_text, self.status_count))

                        # Count
                        self.status_count[1] += 1

        # If it's the closing mediawiki tag, we are done, tell the parser
        if name == 'mediawiki':

            self.status_count[0]='done'

            # Put one done signal in the parser queue for each parse worker
            for _ in range(self.parse_workers):
                self.callback(('done', 'done', self.status_count))


    def characters(self, content):
        '''Handles tag content'''

        # If we decided not to save this tag, also skip
        # the content
        if len(self.read_stack) == 0:
            return

        # If the tag is text, add the content
        if self.read_stack[-1] == 'text':
            self.read_text += content

        # If the tag is title, add the title
        if self.read_stack[-1] == 'title':
            self.read_title += content

        # If it's a namespace tag, update the namespace
        if self.read_stack[-1] == 'ns':
            self.read_namespace=int(content)
