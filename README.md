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

## Evaluation Focus
•	CDC Event Handling Correctness: Proper handling of create/update/delete operations, event ordering, state management
•	Enrichment Logic Design: Thoughtful enrichment strategy, performance considerations, error handling
•	Code Quality & Error Handling: Clean code structure, comprehensive error handling, testing approach
•	Documentation & Operational Thinking: Clear documentation, setup instructions, monitoring considerations

## Key Considerations
•	Event Ordering: How do you handle out-of-order CDC events?
•	Exactly-Once Processing: What guarantees can you provide?
•	State Management: How do you manage enrichment state in Flink?
•	Error Handling: What happens when enrichment fails or target DB is unavailable?
•	Schema Evolution: How would your pipeline handle schema changes?

## Architectural Decisions:

1. Kafka does not guarantee event ordering across partitions. Need to handle out-of-order events appropriately.  
	Solution:
	  1. Solve the issues as far upstream as possible: Have the CDC event producer 
		a. use the same Kafka partition for different events for the same user.
		b. put the CDC events for the same user on Kafka in the right order based on their timestamps.
	
	  2. On Flink side, use keyBy to ensure all CDC events for the same user are processed together in one Flink task.

3. Ensure exactly-once processing where possible, specifically ensure exactly once operations on the target DB table, 
	Solutions:
	
	1. Synchronize all database operations within a Flink's checkpoint cycle to Flink's checkppointing and 
	   handle them as a single transaction.
	2. Use sink and two-phase commit so the database transaction commit is synchronized to Flink's checkpointing, so either both fail or both succeed. 

	3. Flink does checkpointing first
	4. Sink does all DB operations in
	
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
	   
7. Operational and monitoring strategies: 
   Solutions: At the end of each database sink,
   1. Log various pipeline metrics so tools such as CloudWatch and Splunk can be used to monitor and trigger alerts.
   2. Store the pipleline metrics in the database table called user_cdc_pipeline_metrics, so their trends
      can be observed over time. Not implemented in this exercise
   
8. Besides processing the CDC events one by one from the Flink application, a potential alternative solution is listed below. 
   The premise of this solution is often there are common use cases and edge cases. Instead of coming up with a single comprehensive
   solution that cover both types of use cases, I break the larger scope into smaller scopes and provide one solution
   for the common use cases and one solution for the edge cases.
	1. Create a table called user_cdc_events.
	2. Do data enrichment.
	3. On the Sink, delete any data from user_cdc_events from the previous checkpointing or run.
	4. Write all CDC events into a file on S3 and import them into user_cdc_events table.
	5. Separate CDC events into two groups: 
		1. CDC events where there is one and only one event for a user.  We can use SQL 
		   select * from user_cdc_events group up user_id having count(1) = 0 
		   to determine this group of 
		2. CDC events that are have the same user.
	5. For the first group of CDC events, use SQL query to do the operations below in 
	   * bulk * and in the sequence below from 
      user_cdc_events table to the enriched_user table. 
		a. Create
		b. Update
		c. Delete
	6. For the second group of CDC events, for each user, walk through its CDC events and apply them
	   individually based on their timestamps.
		
	This solution is likely to have higher performance and scalability due to the nature of this business domain
	where within short time interval, it is likely Create, Update, and Delete are done for different users.
	and it is unlikely the same user is created, updated, and deleted. 
	So the solution is optimized for common use cases rather than edge cases.
	
	The advantages of this solution over the in-line database operations within Flink are
	1. It does not rely on the event producers to put all CDC events for the same user on the same partition. 
	   This in turn avoid hot partitions where too many events may be assigned to the same Kafka partition.
	   This in turn improve the system's performance and scalability.
	2. Better performance if the velocity of CDC events are high, because the database operations are done in bulk.
	3. Even if the event producer has a bug where it puts different CDC events for the same user out of order,
	   Create, Update, and Delete are still handled in the right sequence.
	4. This is also one way how I would scale the solution for its likely higher performance and scalability.
	

   
## Testing Approach:
1. Generate different sample data for different data conditions such as 
	a. create only
	b. create followed by update
	c. created followed by delete
	d. created followed by update and then delete
	
2. If the solutions for Error handling for data enrichment are implemented, simulate error conditions for data enrichment.

3. If the solutions for out of order events are implemented, generate events for the same user but are out of order.

