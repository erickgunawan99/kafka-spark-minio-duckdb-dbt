Simple data stack end to end project

Tools:
- Apache Kafka
- Apache Spark
- DBT
- Duckdb


Process:
- Using Java to simulate a transaction by generating high throughput transaction data to Apache Kafka topic
- Consume data from topic using Apache Spark (Pyspark) and write it to Minio (s3 runs locally)
- Utilizing Duckdb, an OLAP in-memory Database, that can read data directly from Object Storage to read it directly from Minio and run some transformations and aggregations, all through DBT project connected to Duckdb as the compute source

Useful stack for a small and medium size businesses that dont have massive amount of data.