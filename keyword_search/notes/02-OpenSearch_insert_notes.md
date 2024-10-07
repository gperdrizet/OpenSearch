# OpenSearch insert function build notes

OK, here we go - this is the good part. Starting from the tutorial in the [opensearch-py documentation](https://opensearch.org/docs/latest/clients/python-low-level/) from the OpenSearch site.

Only real question here is can we/do we need to use multiple writers to keep up with the parser. Let's try with just one and see what happens.

## Shard number

Generic advice is ~10-50 GB per shard. Since we have ~20 GB of data and two worker nodes, let's start with 2 shards. This could be optimized in the future. Too many small shards wastes resources on metadata while too few big shards degrades performance. Come to think of it, I don't actually know how big the dataset is when decompressed.

## Testing

OK - seems pretty easy. I'm sure there is a lot we can improve but it seems to be working. Need to set up some logging, but if we print the response from the client.index class we see tons of the following message flying by in the terminal:

```text
{'_index': 'enwiki', '_id': '1493', '_version': 1, 'result': 'created', 'forced_refresh': True, '_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_seq_no': 771, '_primary_term': 1}
```

So, let's set-up a simple command line utility to try out searching the database.

Wow, it works great! Searching for 'acid' yields the following:

```text
Search query: Acid
{'took': 35, 'timed_out': False, '_shards': {'total': 2, 'successful': 2, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 100, 'relation': 'eq'}, 'max_score': 6.8411174, 'hits': [{'_index': 'enwiki', '_id': '50', '_score': 6.8411174, '_source': {'title': 'Acid', 'text': '\nAn acid is a molecule or ion capable of either donating a proton (i.e. hydrogen ion, H+), known as a Brønsted-Lowry acid, or forming a covalent bond with an electron pair, known as a Lewis acid.IUPAC Gold Book - acid\n\nThe first category of acids are the proton donors, or Brønsted-Lowry acids. In the special case of aqueous solutions, proton donors form the hydronium ion H3O+ and are known as Arrhenius acids. Brønsted and Lowry generalized the Arrhenius theory to include non-aqueous solvents....
```

Only issue I see here is it pulls the whole article. But, I guess that's not unexpected... No built in AI summarizer here - if we want that, we have to do it!

## Full insert run

Let's see if we can delete the enwiki index we just created from the dashboard and the do a full run, adding all of wikipedia to it.

Ok, slight issue - the output queue pretty quickly overflows. This means that we can't keep up with inserting all of the articles we are parsing. Couple of options here:

1. Use less parse workers to slow the flow of data down.
2. Use workers for output and see if that helps keep up.
3. Use bulk insert to our single output worker is better able to keep up.

Don't like option one at all - let's save that one for a last resort. A possibly sticky issue with option two is the document id. If we have multiple workers indexing documents from multiple threads we will need a way to avoid ID collisions. I think option 3 is probably the way to go - it's the officially supported strategy for indexing many documents efficiently.

## Bulk insert

OK, seems like it's working. Not sure if will actually be able to keep up yet. Followed the [tutorial](https://opensearch.org/docs/latest/clients/python-low-level/) in the opensearch-py docs. Again issues with the documentation.

The documentation says:

"Note that the operations must be separated by a \n and the entire string must be a single line:"

```text
movies = '{ "index" : { "_index" : "my-dsl-index", "_id" : "2" } } \n { "title" : "Interstellar", "director" : "Christopher Nolan", "year" : "2014"} \n { "create" : { "_index" : "my-dsl-index", "_id" : "3" } } \n { "title" : "Star Trek Beyond", "director" : "Justin Lin", "year" : "2015"} \n { "update" : {"_id" : "3", "_index" : "my-dsl-index" } } \n { "doc" : {"year" : "2016"} }'

client.bulk(movies)
```

This is not true - the payload must be a list of dicts rather than newline delimited string and it seems to work fine. Here is the output we see when printing the response from the client.bulk call:

```text
{'took': 89, 'errors': False, 'items': [{'index': {'_index': 'enwiki', '_id': '351', '_version': 1, 'result': 'created', '_shards': {'t
otal': 2, 'successful': 2, 'failed': 0}, '_seq_no': 145, '_primary_term': 1, 'status': 201}}, {'index': {'_index': 'enwiki', '_id': '35
2', '_version': 1, 'result': 'created', '_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_seq_no': 205, '_primary_term': 1, 'sta
tus': 201}}, {'index': {'_index': 'enwiki', '_id': '353', '_version': 1, 'result': 'created', '_shards': {'total': 2, 'successful': 2, 
'failed': 0}, '_seq_no': 206, '_primary_term': 1, 'status': 201}}, {'index': {'_index': 'enwiki', '_id': '354', '_version': 1, 'result'
: 'created', '_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_seq_no': 207, '_primary_term': 1, 'status': 201}}, {'index': {'_i
ndex': 'enwiki', '_id': '355', '_version': 1, 'result': 'created', '_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_seq_no': 14
6, '_primary_term': 1, 'status': 201}}, {'index': {'_index': 'enwiki', '_id': '356', '_version': 1, 'result': 'created', '_shards': {'t
otal': 2, 'successful': 2, 'failed': 0}, '_seq_no': 147, '_primary_term': 1, 'status': 201}}, {'index': {'_index': 'enwiki', '_id': '35
7', '_version': 1, 'result': 'created', '_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_seq_no': 148, '_primary_term': 1, 'sta
tus': 201}}, {'index': {'_index': 'enwiki', '_id': '358', '_version': 1, 'result': 'created', '_shards': {'total': 2, 'successful': 2, 
'failed': 0}, '_seq_no': 149, '_primary_term': 1, 'status': 201}}....
```

And now... It's keeping up! Let's finally do a complete run and time it on the wall clock so we can see what we are working with. I don't even know how big the dataset is when uncompressed or how many articles we are going to end up with. Probably in the millions. Let's see.

Whelp, it's still singing - just ran to the grocery stor for about and hr and we are up in 2.7 million articles indexed and the OpenSearch dashboard reports 7.2 GB in the primaries index. Queues look good, still bouncing between 10 and a few hundred articles each. CPU use is around ~50% by a quick glance at htop and we aren't hardly using any memory at all. The network interfaces aren't working very hard either - bond0 is seeing one tick spikes of a few MB a second. Wonder what the bottleneck is? I bet we could index much larger batches and use more parse workers. At some point I would think that the xml stream from the bz2 file would be the limiting factor, but I dunno the light load on the storage array and network makes me think we aren't even close yet. One easy quality-of-life improvement - what to do when we use a carriage return to over-write a longer line? Maybe print a blank line first?

## CirrusSearch revisited

Can't believe that I didn't make this connection before - but 'CirrusSearch bulk insert format' is just that - the bulk import format that we just parsed and formatted the xml dump into. Wow, maybe it's much easier to just load that - we will probably get a much more rich database (i.e. more fields that just title and text) and it will probably be WAY faster. No need to mess around with queues and workers and all of that. Can't believe I didn't figure that out yesterday.

Learning more about the CS dump - turns out it's actually in json lines format. Each line contains one JSON object. This means we could process it line by line, but I would rather be able to just read it without unzipping it first. Let's see...

Yep - seems to work great - we can open the file, then loop on the lines and convert them to dict with JSON loads.

OK, done. It works, and it's fast and MUCH simpler than the XML parse/insert strategy. My only concern is that the resulting database is much more dirty. We've done no cleanup of the text and we have who know what fields in there. Well, at least we have both now to play with.

## Additional considerations

### Performance - CirrusSearch

Added threading to the bulk indexing function - this required a switch from creating to upserting records and the addition of single create index call early in execution, before the upsert threads start. Works great after some tinkering.

To upsert, our JSON lines object need to look like this:

```text
{'update': {'_id': 1751, '_index': 'enwiki-cs'}}
{'doc': {'content_key: 'content_value'}, 'doc_as_upsert': True}
```

Now we have to manually tune a few parameters to keep the queues flowing:

1. Number of parser processes (current: 1)
2. Number of upserter processes (current: 10)
3. Size of each bulk upsert (current: 500)

Also, added another OpenSearch node and gave each 32 GB memory. Still running into some 'help, I'm swamped!' type errors. Will need to tinker a bit more to get a good insert speed that doesn't choke. Latest error is:

```text
opensearchpy.exceptions.ConnectionTimeout: ConnectionTimeout caused by - ReadTimeoutError(HTTPConnectionPool(host='localhost', port=9200): Read timed out. (read timeout=10))
```

Adding *timeout=30* and turning gzip compression off in the client instantiation seems to have fixed it.

Good news is it's way faster now - we are doing ~780 articles per second, which is about 10x our XML rate. Using ~90% of CPU and ~100 GB system memory. Network is seeing spikes of RX/TX 30/30 MiB/sec with a consistent load of 20/20 MiB/sec.

### Performance - XML

Same considerations as above, but the parser is slower and indexing is faster so the optimal worker counts etc. are different - more parser threads and less upserter threads. Overall, XML parsing/indexing is less performant than CirrusSearch. We are doing much more parsing/cleaning of the wikicode source from the XML dumps, rather than just jamming the whole thing into OpenSearch like we do for CirrusSearch. If we should be doing the same cleaning with CirrusSearch remains to be decided. Here are some numbers:

1. Number of parser processes (current: 15)
2. Number of upserter processes (current: 1)
3. Size of each bulk upsert (current: 500)

Gives an insert rate of ~65 articles per second, or more than 10x slower than CirrusSearch.

### Error handling

Still sporadically seeing a few sporadic connection errors. We need to have some handling in place for this, especially if we expect this to be run by other people and on other machines. Can't expect folks to hand tune the insert batch size and numbers of workers. Here is the stack trace:

```text
Process Process-6:
Traceback (most recent call last):
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 467, in _m
ake_request
    six.raise_from(e, None)
  File "<string>", line 3, in raise_from
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 462, in _m
ake_request
    httplib_response = conn.getresponse()
  File "/usr/lib/python3.8/http/client.py", line 1348, in getresponse
    response.begin()
  File "/usr/lib/python3.8/http/client.py", line 316, in begin
    version, status, reason = self._read_status()
  File "/usr/lib/python3.8/http/client.py", line 277, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
  File "/usr/lib/python3.8/socket.py", line 669, in readinto
    return self._sock.recv_into(b)
socket.timeout: timed out

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/opensearchpy/connection/http_urllib3.py", l
ine 271, in perform_request
    response = self.pool.urlopen(
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 799, in ur
lopen
    retries = retries.increment(
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/util/retry.py", line 525, in increm
ent
    raise six.reraise(type(error), error, _stacktrace)
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/packages/six.py", line 770, in rera
ise
    raise value
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 715, in ur
lopen
    httplib_response = self._make_request(
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 469, in _m
ake_request
    self._raise_timeout(err=e, url=url, timeout_value=read_timeout)
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 358, in _raise_timeout
    raise ReadTimeoutError(
urllib3.exceptions.ReadTimeoutError: HTTPConnectionPool(host='localhost', port=9200): Read timed out. (read timeout=30)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/usr/lib/python3.8/multiprocessing/process.py", line 315, in _bootstrap
    self.run()
  File "/usr/lib/python3.8/multiprocessing/process.py", line 108, in run
    self._target(*self._args, **self._kwargs)
  File "/mnt/arkk/enwiki-opensearch/wikisearch/functions/io_functions.py", line 134, in bulk_index_articles
    _=client.bulk(incoming_articles)
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/opensearchpy/client/utils.py", line 180, in _wrapped
    return func(*args, params=params, headers=headers, **kwargs)
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/opensearchpy/client/__init__.py", line 460, in bulk
    return self.transport.perform_request(
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/opensearchpy/transport.py", line 447, in perform_request
    raise e
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/opensearchpy/transport.py", line 408, in perform_request
    status, headers_response, data = connection.perform_request(
  File "/mnt/arkk/enwiki-opensearch/.venv/lib/python3.8/site-packages/opensearchpy/connection/http_urllib3.py", line 285, in perform_request
    raise ConnectionTimeout("TIMEOUT", str(e), e)
opensearchpy.exceptions.ConnectionTimeout: ConnectionTimeout caused by - ReadTimeoutError(HTTPConnectionPool(host='localhost', port=9200): Read timed out. (read timeout=30))
```
