#!/usr/bin/env python

from collections import defaultdict, namedtuple
import json
import sys

RDD = namedtuple('RDD', ['id', 'name', 'parents', 'cached'])

# Stages for each job
jobs = defaultdict(lambda: set())

# RDDs for each stage
stages = defaultdict(lambda: set())

# Information on each RDD
rdds = {}

for line in open(sys.argv[1], 'r').readlines():
    event = json.loads(line)
    if event['Event'] == 'SparkListenerJobStart':
        jobid = event['Job ID']
        for sinfo in event['Stage Infos']:
            sid = sinfo['Stage ID']
            for rdd in sinfo['RDD Info']:
                sl = rdd['Storage Level']
                cached = sl['Use Memory'] or sl['Use Disk']

                # Find or create a new RDD structure
                rid = rdd['RDD ID']
                if rid not in rdds:
                    name = rdd['Name']

                    # Add the scope to the name if it exists
                    if 'Scope' in rdd:
                        name += "\n" + json.loads(rdd['Scope'])['name']

                    rddt = RDD(rid, name, tuple(rdd['Parent IDs']), cached)
                else:
                    rddt = rdds[rid]

                # Track the stages for each job and the RDDs for each stage
                jobs[jobid].add(sid)
                stages[sid].add(rddt)
                rdds[rid] = rddt

print('digraph G {')

for (job, sids) in jobs.items():
    # Create a cluster for each job
    print('  subgraph cluster_j%d {' % sid)

    for sid in sids:
        stage = stages[sid]

        # Create a cluster for each stage
        print('    subgraph cluster_s%d {' % sid)

        for rdd in stage:
            attribs = ['label="%s"' % rdd.name]

            # Colour RDDs annotated for persistence green
            if rdd.cached:
                attribs.append('fillcolor=palegreen')
                attribs.append('style=filled')

            print('      rdd%d [%s];' % (rdd.id, ','.join(attribs)))

        print('      label="Stage %d";' % sid)
        print('    }')

    print('    label="Job %d";' % job)
    print('  }')

# Add edges connecting RDDs with their ancestors
for rdd in rdds.values():
    for parent in rdd.parents:
        print('  rdd%d -> rdd%d;' % (parent, rdd.id))

print('}')
