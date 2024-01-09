# Reservoir-Sampling-Hadoop-Spark

## Overview
This repository contains code used to generate a randomized subset comprising 50,000 entries from the extensive dataset (read below) employing the Reservoir Sampling algorithm. With a dataset size of approximately **_900,000_** entries, the specified sample size (k) is set at 50,000. Implement this process efficiently by leveraging both the **_Hadoop MapReduce_** and **_Spark_** frameworks, ensuring a robust and scalable approach to the creation of the desired subset. This strategic utilization of distributed computing technologies will facilitate the seamless extraction of a representative sample from the extensive dataset, enabling further analysis and exploration while maintaining the integrity of the original data distribution.

P.S: Importantly, the dataset is presented as an unbounded stream of items with an unknown length, and the constraint is that ywe can only traverse the data once. Considering the anticipated largeness of the dataset, the development of the program necessitates the creation of a statistically accurate representative sample. To achieve this, the implementation of an algorithm is required, one that randomly selects items from the data stream with the assurance that each item has an equal probability of being chosen.

## Reservoir Sampling Algorithm
Reservoir sampling is designed to select a simple random sample, without replacement, of k items from a population with an unknown size, denoted as n, during a single iteration through the items. The algorithm operates under the constraint that the overall size of the population (n) is undisclosed and often too large to accommodate all items within the available main memory. As the population unfolds progressively, the algorithm gains visibility into it, yet it is unable to retrospectively access prior items. At any given moment, the algorithm's current state should enable the extraction of a simple random sample, without replacement, of size k from the observed portion of the population.
![image](https://github.com/khuranayashika31/Reservoir-Sampling-Hadoop-Spark/assets/51834607/d5254f47-a80c-436c-97b8-54e59be4649d)
Image by: André Müller

## About the Dataset
Can be found here: https://en.wikipedia.org/wiki/Self-Monitoring,_Analysis_and_Reporting_Technology
This comprehensive dataset encompasses the real-time SMART (Self-Monitoring, Analysis, and Reporting Technology) logs for every hard drive deployed within a data center throughout the initial quarter of 2019. It is imperative to acknowledge the dynamic nature of the dataset, as it captures the unfolding events over the three-month period, including instances of drive failures and the introduction of new hardware. Boasting a vast repository of information, the dataset comprises over **900,000 entries**, qualifying for the application of Big Data solutions to handle.

## How To Run
* Hadoop Implementation: Consider mapper2.py & reducer2.py
* Spark Implementation: Consider spark_reservoir.ipynb

Hadoop Map-Reduce jobs were run on NYU's Dataproc, which requires access credentials.

