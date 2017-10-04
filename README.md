# Spark log analysis

All of the scripts below are designed to analyze Spark event logs and print out various statistics.
To produce a log from your Spark application, the following configuration options need to be set:

    spark.eventLog.enabled true
    spark.eventLog.dir hdfs://namenode/shared/spark-logs

Each script takes a log file name as an argument.
Below is a list of all the scripts and the data they produce as output.

## graph-job.py

Produces a graph of the application in [DOT](http://www.graphviz.org/content/dot-language) format which can be fed to Graphviz to produce a visualization of a spark application.

## rdd-sizes.py

Note that this script requires a log produced by a [modified version of Spark](https://github.com/michaelmior/spark/tree/track-rdd-size).

| Column | Description |
| --- | --- |
| RDD ID | ID of the estimated RDD partition |
| Partition | partition corresponding to the size estimate |
| Estimated Size | average estimated size for the partition |

## uses-caching.py

| Column | Description |
| --- | --- |
| Job ID | ID of a particular job |
| Runtime | total runtime of the job |
| Cached Partitions | partitions read from the cache |
| Cacheable Partitions | partitions which could have been cached because they were previously computed |
| Total Partitions | total number of partitions for all RDDs in the job |
