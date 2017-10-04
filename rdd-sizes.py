#!/usr/bin/env python

from collections import defaultdict
import json
import sys

sizes = defaultdict(lambda: 0)
size_counts = defaultdict(lambda: 0)

for line in open(sys.argv[1]).readlines():
    event = json.loads(line)
    if event['Event'] == 'SparkListenerRDDSizesUpdated':
        for size in event['RDD Sizes']:
            # Ignore size estimates without partition information
            if 'Partition' not in size:
                continue

            # Track a size estimate for this RDD partition
            key = (size['RDD ID'], size['Partition'])
            sizes[key] += size['Estimated Size']
            size_counts[key] += 1

# Print average sizes for each partition
print('"RDD ID","Partition","Estimated Size"')
for ((rid, pid), size) in sizes.items():
    print('%d,%d,%f' % (rid, pid, size * 1.0 / size_counts[(rid, pid)]))
