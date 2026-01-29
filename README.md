# CDC Streaming Pipeline with Apache Flink

## Overview

This project implements a **Change Data Capture (CDC) streaming pipeline** using **Apache Flink**.
The pipeline consumes CDC events produced by a legacy system, enriches user records, and applies **create, update, and delete** operations to a target MySQL table.

## CDC Event Model

Each CDC event represents a mutation to a user record:

Sample json
{
  "op": "u",
  "before": { "userId": "123", "name": "Alice" },
  "after":  { "userId": "123", "name": "Alice Smith" },
  "timestamp": 1700000000000
}

This design prioritizes:

- Correct ordering per user
- Event-time correctness
- Idempotent database writes
- Exactly once operations on the target DB table
- Low operational complexity
- High throughput with bounded latency

---

Issues and Solutions:

1. Kafka does not guarantee event ordering across partitions that Flink's checkpoint cycle in a single transaction. And need to handle out-of-order events appropriately.
	Solution:
	  1. Solve the issues as far upstream as possible: Have the CDC event producer 
		a. use the same Kafka partition for different events for the same user.
		b. put the CDC events for the same user on Kafka in the right order based on their timestamps.
	
	  2. On Flink side, use keyBy to ensure all CDC events for the same user are processed together in one Flink task.

3. Ensure exactly-once processing where possible, specifically ensure exactly once operations on the target DB table, 
	Solutions:
	
	1. use sink and two-phase commit so the database transaction commit is synchronized to Flink's checkpointing, so either both fail or both succeed. 

	2. Flink does checkpointing first
	3. Sink does all DB operations in
	
4. Performance considerations for data enrichment:
	Solutions: 
	1. for this exercise, keep it super simple with hard-coded data. For production, data enrichment
   would likely involve data look-up. To minimize such latency and for performance and scalability, in production 
   consider	either
	   1. Storing such look-up data in an in-memory database such as Apache Ignite
	   2. Prefetching the data from the database into Redis cache.
   
   Option 1 is preferred, because there is no extra infrastructure and there is no stale cache issue.
   
5. Error handling for data enrichment:
   Solutions: To do in production. Not implemented in this exercise
	1. Create a error_data_enrichment table.
	2. Log the CDC events with data enrichment error to the error_data_enrichment table.
	3. Skip its processing and other CDC events for the same user, so CDC events for other users 
	   can still be processed. 
	4. Use a HashMap and clear for each Flink's database sink cycle.
	5. The key of the Hashmap is the user id.
	6. When data enrichment fails, add that CDC event to the Hashmap.
	7. Before a CDC event is processed, see if there was already another CDC event for the same user id
	   that already failed the data enrichment. If so, skip it

6. Optional: What if the CDC event producer has a bug where it puts CDC events for the same user out of order in tersm of timestamps
   Solutions:
	1. Use a HashMap and clear for each Flink's database sink cycle.
	2. The key of the Hashmap is the user id.
	3. The value is a list of CDC events in each Flink's checkpoint cycle.
	4. Each CDC event is added to the HashMap after it is processed successfully. 
	5. When the database operation of a CDC event fails, check the HashMap to see if there are preceding
	   events that are out of order. If so, record this condition so the event producer can be fixed.
	   
7. Operations: 
   Solutions: At the end of each database sink,
   1. Log various pipeline metrics so tools such as CloudWatch and Splunk can be used to monitor and trigger alerts.
   2. Store the pipleline metrics in the database table called user_cdc_pipeline_metrics, so their trends
      can be observed over time. Not implemented in this exercise
   
Testing Approach:
1. Generate different sample data for different data conditions such as 
	a. create only
	b. create followed by update
	c. created followed by delete
	d. created followed by update and then delete
	
2. If the solutions for Error handling for data enrichment are implemented, simulate error conditions for data enrichment.

3. If the solutions for out of order events are implemented, generate events for the same user but are out of order.