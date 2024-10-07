# Data source

The first data source and the one used for development will be English Wikipedia in CirrusSearch dump format. These dumps contain JSON formatted data for use with Elasticsearch, so it should be perfect for our use case. They are published ~weekley, so we can keep our local copy up-to-date if we like. The dumps come down as single large gzip compressed files. Each dump has two files associated with it, for example on 2024-09-30 we have:

```text
enwiki-20240930-cirrussearch-content.json.gz
enwiki-20240930-cirrussearch-general.json.gz
```

Let's get those two file and take a look at what's inside.

```text
curl https://dumps.wikimedia.org/other/cirrussearch/20240930/enwiki-20240930-cirrussearch-content.json.gz
curl https://dumps.wikimedia.org/other/cirrussearch/20240930/enwiki-20240930-cirrussearch-general.json.gz
```

