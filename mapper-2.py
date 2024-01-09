#!/usr/bin/env python

import sys
import random

# setting reservoir size
reservoir_size = 50000
reservoir = []

# iterating over the records and implementing reservoir sampling
for no, line in enumerate(sys.stdin, start=1):
    line = line.strip()
    random_no = random.randint(0, no)
    if len(reservoir) < reservoir_size:
        reservoir.append(line)
    elif random_no < reservoir_size:
        reservoir[random_no] = line
#printing reservoir contents
for element in reservoir:
    print(element)
