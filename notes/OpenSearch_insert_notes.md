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
