# Spark log analysis

All of the scripts below are designed to analyze Spark event logs and print out various statistics.
To produce a log from your Spark application, the following configuration options need to be set:

    spark.eventLog.enabled true
    spark.eventLog.dir hdfs://namenode/shared/spark-logs

Each script takes a log file name as an argument.
Below is a list of all the scripts and the data they produce as output.

## extract-cache-levels.py

Produces JSON output which in a format suitable for feeding back into Spark to modify the storage level of each partition of an RDD during each stage of execution.
Note that this script requires a [modified version of Spark](https://github.com/michaelmior/spark/tree/track-rdd-size).

## graph-job.py

Produces a graph of the application in [DOT](http://www.graphviz.org/content/dot-language) format which can be fed to Graphviz to produce a visualization of a spark application.

## rdd-sizes.py

Note that this script requires a log produced by a [modified version of Spark](https://github.com/michaelmior/spark/tree/track-rdd-size).

| Column | Description |
| --- | --- |
| RDD ID | ID of the estimated RDD partition |
| Partition | partition corresponding to the size estimate |
| Estimated Size | average estimated size for the partition |

## rdd-summary.py

| Column | Description |
| --- | --- |
| RDD ID | ID of the RDD |
| Name | name for the RDD defined in the application |
| Storage Level | persistence level for this RDD (does not account for changes) |
| Callsite | information on where this RDD was created |

## uses-caching.py

| Column | Description |
| --- | --- |
| Job ID | ID of a particular job |
| Runtime | total runtime of the job |
| Cached Partitions | partitions read from the cache |
| Cacheable Partitions | partitions which could have been cached because they were previously computed |
| Annotated Partitions | partitions which were annotated for caching that should be in the cache (unless evicted) |
| Total Partitions | total number of partitions for all RDDs in the job |
