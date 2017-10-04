#!/usr/bin/env python

from collections import defaultdict
import json
import sys

# Stores data on RDDs for each job
jobs = defaultdict(lambda: {})

# Track the RDDs which we have already calculated
seen_rdds = set()

for line in open(sys.argv[1]).readlines():
    event = json.loads(line)
    if event['Event'] == 'SparkListenerJobStart':
        job = {
            'rdds': [],
            'could_cache': 0,
            'cached': 0,
            'total': 0
        }
        job['start'] = event['Submission Time']

        # Examine all RDDs in the job
        for stage in event['Stage Infos']:
            for rdd in stage['RDD Info']:
                job['rdds'].append(rdd['RDD ID'])

                # If we have already seen this RDD, it would be possible to cache it
                if rdd['RDD ID'] in seen_rdds:
                    job['could_cache'] += rdd['Number of Partitions']

                job['cached'] += rdd['Number of Cached Partitions']
                job['total'] += rdd['Number of Partitions']

                # Track the appearance of this RDD
                seen_rdds.add(rdd['RDD ID'])

        jobs[event['Job ID']] = job
    elif event['Event'] == 'SparkListenerJobEnd':
        # Store completion time for runtime calculation
        jobs[event['Job ID']]['end'] = event['Completion Time']

print('"Job ID","Runtime","Cached Partitions","Cacheable Partitions","Total Partitions"')
for (jobid, job) in jobs.items():
    print('%d,%d,%d,%d,%d' % (
        jobid, job['end'] - job['start'],
        job['cached'],
        job['could_cache'],
        job['total']))
