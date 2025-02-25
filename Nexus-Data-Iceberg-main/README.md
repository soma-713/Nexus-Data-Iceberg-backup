# Nexus Data Iceberg

### Iceberg Architecture:
Apache Iceberg is a high-performance format for huge analytic tables that brings the reliability and simplicity of SQL tables to big data.

![Iceberg Architecture](https://media.datacamp.com/cms/google/ad_4nxdtzjzfgdcxrgl9qgbloazunglq8kiqiiltqormlwabc7joxb6ibawyqlpw0biswekxx60evfodhohdsq59xcora597v6jtm43in7y0adv-lbi_4sztcupayna6d10xi2rnkjgygdxlwaai54lagc9rv5hx.png)

### Technologies:
- Apache Spark 
- Apache Iceberg
- Minio
- PostgresSQL
- Docker
- PySpark
- Jupyter Notebook

### Getting Started
1. Clone the repository:
```sh
git clone https://github.com/TalenticaSoftware/Nexus-Data-Iceberg.git
cd Nexus-Data-Iceberg
```

2. Start the services using Docker Compose:
```sh
docker-compose up -d
```

- To down the docker containers:
```sh
docker-compose down
```

3. You can then run any of the following commands to start a Spark session.
- SparkSQL: 
```sh
docker exec -it spark-iceberg spark-sql
```
- Spark-Shell: 
```sh
docker exec -it spark-iceberg spark-shell
```
- PySpark:
```sh
docker exec -it spark-iceberg pyspark
```
- Jupyter-notebook: The notebook server will be available at http://localhost:8888
```sh
docker exec -it spark-iceberg notebook 
```

4. Access the services:
- Spark Web UI: http://localhost:8080
- Jupyter Notebook: http://localhost:8888
- MinIO Web UI: http://localhost:9001 -> username & password in docker-compose file
- Postgres DB: Accessible on port 5432

### Services Overview
This project sets up a Spark environment with Iceberg, MinIO, PostgreSQL, and a REST service using Docker Compose. The setup includes the following services:

#### Spark Iceberg
* Runs Spark with Iceberg, enabling data processing and analytics with support for Iceberg tables.

#### Iceberg REST Fixture
* Provides a REST interface for Iceberg, allowing interaction with Iceberg tables via REST API.

#### MinIO
* Provides S3-compatible object storage for storing data and metadata.

#### MinIO Client (mc)
* Used to manage MinIO buckets and policies.

#### PostgreSQL
* Used as the metadata store for Iceberg. Stores the catalog information.

### Scripts Overview:
#### Spark Data Insertion Notebook (`spark_data_insertion.ipynb`):
- Demonstrates how to perform data insertion and update operations on an Iceberg table using PySpark.
- Includes functions to measure the size of metadata files and the time taken for various operations.

#### Data Generation Notebook (`data_generation.ipynb`):
- Demonstrates how to generate synthetic data and save it in either Parquet or CSV format.
- Includes functions to generate random records and save them efficiently.

### File Structure:
- config
    - postgresql-42.6.0.jar -> postgres jdbc connector 
    - spark-defaults.conf -> spark catalog & iceberg metadata storage related configurations

- input_data
    - csv -> contains all input csv files
    - parquet -> contains all input parquet files

- notebooks
    - data_generation.ipynb -> data generation
    - spark_data_insertion.ipynb -> read the input files, load to iceberg table, perform update operation & generate analysis sheet of size & time.

- minio: when you start the container this dir will be automatically created.
    - data
        - warehouse: This directory will contain all the data dir & metadata of iceberg.

- postgres_data: when you start the container this dir will be automatically created. It is used to persist the postgres data.


### References for learning:
* Understand the structure of Iceberg: 
    - https://www.datacamp.com/tutorial/apache-iceberg
    - https://karlchris.github.io/data-engineering/projects/spark-iceberg/#setup-iceberg-session

* Apache Iceberg looking at append, update and delete operations:
    - https://medium.com/@MarinAgli1/learning-apache-iceberg-looking-at-append-update-and-delete-operations-179ad63cb6cb

* Spark-iceberg docker quickstart:
    - https://iceberg.apache.org/spark-quickstart/

* Other links:
    - https://www.velotio.com/engineering-blog/iceberg---introduction-and-setup-part---1


