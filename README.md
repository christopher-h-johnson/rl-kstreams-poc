# JOB POSTING PROCESSOR

## Problem
Build an optimized data caching service so as to minimize calls to the gRPC endpoint. 

You have full freedom over how you build both the caching service and the data augmentation service. 

Your submission will be graded primarily on the efficiency of the caching service.

## Solution

Kafka Streams!  

The problem of building highly scalable and performant API response caching is solved in this proof of concept 
KStreams application.  The solution uses a single model to represent non-augmented and augmented job posts where the 
augmentation is performed by routing records with a zero default seniority into a distinct branch topic.

Records from this branch topic ("jobs-call-api") call the service API and are augmented with the response value.  The topic
is then emitted and persisted as the "seniority topic".  This branch topic is also merged into the default branch topic 
that will only emit records that have the seniority augmentation.

Since the branch topic with augmented records is persisted as the "seniority topic", 
subsequent job posts having the same key combination of company and title can be left joined 
with the materialized topic as a KTable and augment seniority without calling the API.

The solution is highly scalable as running multiple instances of the application can parallelize the input work load with 
each instance consuming from a distinct source topic partition.  A producer using the default partitioner will allocate 
and distribute similarly keyed records to partitions with a murmur3 hash key.  

Distributing the processing across multiple replicas enables concurrent API clients which may reduce the 
need for API request batching. API requests can also be rate limited in the value mapper if needed.  Since the value 
mapper is called once for each input record, each API batch request and response are handled as singletons.
A production deployment of this test application sourcing from a topic with multiple partitions and 
having multiple replicas could easily process millions of requests per day.

## Preconditions

This solution assumes that mechanisms exist for producing the incoming job postings into a Kafka source topic, as well
as sinking the augmented output topic into the desired S3 location. It also assumes that a Kafka cluster and Container 
cluster to deploy the KStreams application exist.  

Using Kafka Connect is a convenient way to source and sink from/to S3 buckets.  
An S3 source connector can be configured to poll buckets within the specified update interval of 1 minute and continuously
produce the source records for processing by the application.  Alternatively, a "push microservice" could also consume 
from the job posting source directly (before or in conjunction with S3 persistence) and produce directly into the Kafka source topic.

## Testing
The Integration Test depends on Docker and the Docker Compose test container.  The target build is JDK 21.  
The test objects have randomly generated values except for company and title which are selected from given values.
The given titles are paired with the appropriate seniority ordinals.  It has also been assumed that the UUID for the API request
is sourced from a path in the job posting URL and that the UUID is a string.

## Evaluation
The overall performance of the work flow is constrained primarily by the number of calls to the API, which will
typically be the most expensive operation.  Given sources that primarily have pre-existing augmented records in the cache topic,
the end to end latency (from the time the record is produced into the source topic to the time it is emitted in the output) 
in this relatively simple single join topology will be in microseconds.

Profiling the integration tests reveals that the CPU time (including initialization) for the first join test of 20 input 
records is 1608 ms and the second API test of 10 records (and 10 cached) with an output of 20 total is 484 ms.
Memory allocations for the first test are 19.68 MB and the second test is 8.53 MB.

The strength of this solution exists in the horizontal scalability of Kafka Streams and the distributed architecture of
Kafka itself. Running parallel replicas can dramatically reduce total processing time proportionally via concurrency
and yield optimal gains with minimal vertical scaling overhead.
