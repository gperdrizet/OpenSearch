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