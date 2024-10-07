# Semantic search ingest pipeline

Version 1.0 of this project has become kind of a mess. It is trying to do too many things. We have ingest from Wikipedia XML and CirrusSearch dumps, semantic and keyword search databases, and all of it is too dependent on the source format. I think it's time to rewrite the project as a fully modular semantic search first data pipeline. Here's how I'm imagining the steps.

## Data pipeline

1. Start with a run specific config (and maybe a general one two for things like IP address and project paths).
2. Run a dataset specific reader. This should be the only part of the pipeline which is dataset specific. Its only job is to get the data into a consistent format. It also gets metadata like the number of records and stores it somewhere and assigns each record unique ID. This step could maybe be parallelized based on the source data format.
3. Next, the formatted data goes into a data parser/prepper. This handles cleaning up the text, possibly chunking it for embedding and batching it for embedding. This could be parallelized over the records from the first step.
4. Then comes the embedder, this step calculates embeddings for each chunk/record in each batch. This could be parallelized over the batches dependent on GPU memory requirement.
5. Last is the inserter. This stage just puts the embeddings into OpenSearch. This could again be parallelized over the batches.

## Notes

- Use HDF5 to store intermediate data at each step. This way metadata and chunks/batches can be stored in one place. Also, each step except the first could/should have the ability to resume runs.
- If we are smart, we can use some testing to determine how many jobs to run, especially for the embedder. I.e, test embed a few articles and monitor memory, then decide how many can fit on each GPU.
- In an effort to keep this as simple as possible, just use the text embedding processor from OpenSearch, this way we don't really have to deal with managing and prompting our own mode. It also removes a step from the pipeline. Now it looks like this:

1. **Configuration file**: Data source/run specific configuration.
2. **Reader**: Scans data and reads it into a standard format, assigns record ID and stores to HDF5. Also records some metadata. This step is the last thing in the pipeline that should be specific to the dataset.
3. **Parser**: Cleans up text and chunks/batches data for embedding and indexing. Could be parallelized over records. Also, could implement some cool tricks from [Sarthi et al. (2024)](http://arxiv.org/abs/2401.18059) here in the future.
4. **Indexer**: Standard [[OpenSearch]] neural/semantic search ingest following the [Neural Search Tutorial](https://opensearch.org/docs/latest/search-plugins/neural-search-tutorial#step-1a-choose-a-language-model)

Much better. This should be vastly more simple. To use a different data source all we will need to do is set up a configuration file and make a modified reader module for that source.
