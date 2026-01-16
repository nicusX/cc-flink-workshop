# Lab 5

### Statement Operations

Use demo data from Lab 1

#### Error Handling
https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html#custom-error-handling

ALTER TABLE `error_handling` SET (
  'error-handling.mode' = 'log',
  'error-handling.log.target' = 'dlq_topic'
)


#### Carry Over Offset
https://docs.confluent.io/cloud/current/flink/operate-and-deploy/carry-over-offsets.html

set 'client.statement-name' = 'initial-statement-6';

CREATE TABLE carryover
  WITH ('kafka.consumer.isolation-level'='read-uncommitted')
  AS
SELECT *,`offset` FROM stocks
