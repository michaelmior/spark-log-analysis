#!/usr/bin/env python

import json
import sys

# Convert storage levels to their string representation
# TODO: Consider replication level
def storage_level_name(sl):
    if sl['Use Memory']:
        if sl['Use Disk']:
            if sl['Deserialized']:
                return 'MEMORY_AND_DISK'
            else:
                return 'MEMORY_AND_DISK_SER'
        elif sl['Deserialized']:
            return 'MEMORY_ONLY'
        else:
            return 'MEMORY_ONLY_SER'
    else:
        return 'DISK_ONLY'

# Map to store output for each RDD
rdds = {}

# Loop over lines in the file and parse events
for line in open(sys.argv[1]).readlines():
    event = json.loads(line)
    if event['Event'] == 'SparkListenerStageSubmitted':
        # Collect all RDDs from new stages
        for rdd in event['Stage Info']['RDD Info']:
            rid = rdd['RDD ID']
            name = rdd['Name']
            sl = rdd['Storage Level']

            # Save a line to output later
            rdds[rid] = '%d,"%s",%s,"%s"' % (
                rid,
                name,
                storage_level_name(sl),
                rdd['Callsite'])

# Print information for each RDD in order by ID
print('"RDD ID","Name","Storage Level","Callsite"')
for rid in sorted(rdds.keys()):
    print(rdds[rid])
