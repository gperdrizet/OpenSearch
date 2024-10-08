# Data source

The first data source and the one used for development will be English Wikipedia in CirrusSearch dump format. These dumps contain JSON formatted data for use with Elasticsearch, so it should be perfect for our use case. They are published approximately weekly, so we can keep our local copy up-to-date if we like. The dumps come down as single large gzip compressed files. Each dump has two files associated with it, for example on 2024-09-30 we have:

```text
enwiki-20240930-cirrussearch-content.json.gz
enwiki-20240930-cirrussearch-general.json.gz
```

Let's get those two file and take a look at what's inside.

```text
wget https://dumps.wikimedia.org/other/cirrussearch/20240930/enwiki-20240930-cirrussearch-content.json.gz
wget https://dumps.wikimedia.org/other/cirrussearch/20240930/enwiki-20240930-cirrussearch-general.json.gz
```

OK, here they are...

```text
$ du -sh ./*
38G    ./enwiki-20240930-cirrussearch-content.json.gz
54G    ./enwiki-20240930-cirrussearch-general.json.gz
```

Lets's unzip them an see what we get:

```text
$ gunzip ./enwiki-20240930-cirrussearch-content.json.gz
$ gunzip ./enwiki-20240930-cirrussearch-general.json.gz

$ du -sh ./*
156G    ./enwiki-20240930-cirrussearch-content.json
267G    ./enwiki-20240930-cirrussearch-general.json
```

OK, that's a lot of text. Both are too big to read into memory. Let's start with streaming from the file.
