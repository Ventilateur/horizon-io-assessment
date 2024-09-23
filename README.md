# Sequence take-home exercise

## DISCLAIMER!!!

Due to the scope as well as the time and effort constraints of this exercise, I intentionally omit some aspects that 
I would do if it was a real product. I will list them here along with explanation and potential real world solution.

- Architecture:
  - While I stream data from the sample file to reduce memory footprint, I still keep the whole aggregation in-memory. 
    In real world, aggregation might become very big if the cardinality is high and will cause memory exhaustion. An 
    intermediate database or a spill-to-disk mechanism can be used in such case.
  - Any subsequence run will simply append to the result table, not updating it. I put an ingestion id as parameter to 
    distinguish between runs. It is intentional because otherwise it invalidates the meaning of using Go to aggregate
    data. To update the table is also a kind of aggregation, Bigquery can simply do all of it.
  - The tool is not fault-tolerant. Any kind of third-party hiccup or minor data corruption will cause it to stop. 
    Implementing fault-tolerant will need sophisticate domain study and communication with the stakeholder.
  - Logging is also omitted because it needs a lot of configurations. Logging can be done with logrus 
    along with info and debug logs on rows of data. Also, the error messages can be improved for better debugging. 
- Functional requirements:
  - I don't pull historical prices from Coingecko, given that the prices can be fetched daily in a batch job. As a 
    result, I only provide codes to fetch current prices. However, as the sample data is from the past, I simply 
    duplicate the current prices to a few days that are present in the sample data (see [Setup](#setup) section).
  - Using currency symbol to identify a coin is not reliable because multiple coins can bear the same symbol. So as I am 
    not sure how to map a symbol to a price in usd, I took a random one. Surely it's not the correct way to do it.
- Build and deploy:
  - No Dockfile nor build script as this tool is to be run locally and it's simple enough.
  - No CI/CD because I am the only one who work in this.
  - No Terraform because it seems to be out-of-scope.
- Testing:
  - I did not provide exhaustive testing because there are a lot of third-party components (Coingecko, GCS, Bigquery)
    that need to be mocked. Interfacing the structs and using [gomock](https://github.com/uber-go/mock) is how 
    I usually do it, it's quite simple to implement but is still cumbersome and require time to do. There are unit 
    tests present though.

## How to run

### Prerequisite

You need to have `gcloud` CLI and a Google Cloud project. Your account needs sufficient read/write permission 
for GCS and Bigquery. Also, you need a Coingecko demo account API key.

### Setup

Prepare a bucket name, a dataset name and a table name and run the command below:

```shell
cd scripts
BUCKET=<bucket> DATASET=<dataset> TABLE=<table> ./init.sh
```

The script does the following setups:

1. Create a bucket.
2. Create a dataset and a table with a schema corresponding to the expected data.
3. Seed price files for the following dates: 2024/04/01, 2024/04/02, 2024/04/15, 2024/04/16 with the same data in 
    [scripts/prices.csv](scripts/prices.csv), which is the data from 2024/09/21.
4. Seed sample data to `<bucket>/sample_data.csv` with data in [scripts/sample_data.csv](scripts/sample_data.csv).

### Fetch current coin prices

```shell
COINGECKO_API_KEY=<api-key> go run main.go fetch-prices --bucket <bucket> --prices-file-name prices.csv
```

This will produce a `prices.csv` file in `gs://<bucket>/<year>/<month>/<day>/prices.csv`.  

### Aggregate data

```shell
go run main.go aggregate \
  --bucket <bucket> \
  --prices-file-name prices.csv \
  --input-file sample_data.csv \
  --dataset <dataset> \
  --table <table> \
  --ingestion-id 0001
```

This will aggregate data into Bigquery `<dataset>.<table>` with `ingestion_id` field `0001`.

To query the data:

```shell
echo "select * from <dataset>.<table> where ingestion_id='0001'" | bq query
```
