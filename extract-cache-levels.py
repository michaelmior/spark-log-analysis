#!/usr/bin/env python

import json
import sys

# Store caching levels for each RDD
cache_info = {'Stages': []}

# Loop over all events in the file
for line in open(sys.argv[1]).readlines():
    event = json.loads(line)

    # Only look at new stages
    if event['Event'] == 'SparkListenerStageSubmitted':
        stage = {
            'Stage ID': event['Stage Info']['Stage ID'],
            'Cached RDDs': []
        }

        # Get data for each stage
        for rdd in event['Stage Info']['RDD Info']:
            for partition in range(rdd['Number of Partitions']):
                stage['Cached RDDs'].append({
                    'RDD ID': rdd['RDD ID'],
                    'Storage Level': rdd['Storage Level'],
                    'Partition': partition
                })

        cache_info['Stages'].append(stage)

# Output the caching info
print(json.dumps(cache_info, indent=4))
