# Simple Parquet Convertor

## What is Simple Parquet Convertor?
**Simple Parquet Convertor** as its name suggests, is a simple tool to convert between parquet and various other data formats.
Currently it's designed to run as a standalone java application, but it uses Apache Spark, therefore in future we plan to implement support for submitting the application on Yarn clusters.

## Supported Data Formats
Currently the convertor supports:
- Parquet
- CSV
- ORC
- JSON
- Text
These are the data formats supported by Spark SQL by default.

## Supported Storage Locations
Data can be converted from/to:
- Local file system
- HDFS
- S3

## How to Build
#### Build requirements:
 - **Maven 3.5.4+**
 - **Java 8**

#### Build commands:
- Without tests:  `mvn clean package -DskipTests `
- With unit tests:  `mvn clean package`

## How to Run
- Help command will display all available options: `java -cp simple-parquet-convertor.jar africa.absa.parquet.convertor.SimpleParquetConvertor --help`
- Simple conversion between csv and parquet example: `java -cp ./simple-parquet-convertor-0.0.1-SNAPSHOT.jar africa.absa.parquet.convertor.SimpleParquetConvertor --source-format csv --source-path ./myDatasetCsv.csv --target-path ./myDatasetParquet --target-format parquet`

## Available Options
- `--source-format`: **REQUIRED** Source data format. 
- `--target-format`: **REQUIRED** Target data format.
- `--source-path`: **REQUIRED** The path of source file/folder to be converted.
- `--target-path`: **REQUIRED** The path of target folder to save the converted data into.
- `--infer-schema`: **OPTIONAL** This options attempts to infer strongly typed schema from (un/loose)-typed data sources.
- `--csv-header`: **OPTIONAL** This options specifies whether CSV files (for read and write) have headers.
- `--csv-quote`: **OPTIONAL** This options specifies the CSV quote character. Default is "
- `--csv-escape`: **OPTIONAL** This options specifies the CSV escape character. Default is \
- `--csv-comment`: **OPTIONAL** This options specifies the CSV comment character. Default is #
- `--charset`: **OPTIONAL** This options specifies the charset for text based data formats. Default is UTF-8.
- `--num-partitions`: **OPTIONAL** ADVANCED: Number of partitions (files) produced. Higher the number, higher the parallelizm achievable. Causes shuffling (high network cost).
- `--num-driver-cores`: **OPTIONAL** Number of CPU cores to be used for the conversion (driver side).
