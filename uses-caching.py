#!/usr/bin/env python

from collections import defaultdict
import json
import sys

# Stores data on RDDs for each job
jobs = defaultdict(lambda: {})

# Number of partitions in the cache for a given RDD
cache = defaultdict(lambda: 0)

# Track the RDDs which we have already calculated
seen_rdds = set()

for line in open(sys.argv[1]).readlines():
    event = json.loads(line)
    if event['Event'] == 'SparkListenerTaskEnd':
        for block in event['Task Metrics']['Updated Blocks']:
            if not block['Block ID'].startswith('rdd'):
                continue

            rid, pid = map(int, block['Block ID'].split('_')[1:])
            sl = block['Status']['Storage Level']
            if sl['Use Memory'] or sl['Use Disk']:
                cache[rid] += 1
            else:
                cache[rid] -= 1

    elif event['Event'] == 'SparkListenerJobStart':
        job = {
            'rdds': [],
            'could_cache': 0,
            'should_cache': 0,
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

                    sl = rdd['Storage Level']
                    if sl['Use Memory'] or sl['Use Disk']:
                        job['should_cache'] += rdd['Number of Partitions']

                job['cached'] += cache[rdd['RDD ID']]
                job['total'] += rdd['Number of Partitions']

                # Track the appearance of this RDD
                seen_rdds.add(rdd['RDD ID'])

        jobs[event['Job ID']] = job
    elif event['Event'] == 'SparkListenerJobEnd':
        # Store completion time for runtime calculation
        jobs[event['Job ID']]['end'] = event['Completion Time']

print('"Job ID","Runtime","Cached Partitions","Cacheable Partitions","Annotated Partitions","Total Partitions"')
for (jobid, job) in jobs.items():
    print('%d,%d,%d,%d,%d,%d' % (
        jobid, job['end'] - job['start'],
        job['cached'],
        job['could_cache'],
        job['should_cache'],
        job['total']))
