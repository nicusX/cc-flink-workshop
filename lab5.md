
# Lab 5

### Statement Operations

Use demo data from Lab 1

#### Error Handling
Custom error handling for deserialization errors
https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html#custom-error-handling


Insert valid data
```
INSERT INTO customers_pk (account_number, customer_name, city)
VALUES ('ACC1000111', 'Bob Smith', 'Munich');
```

Try to insert invalid data
```
INSERT INTO customers_pk (account_number, customer_name, date_of_birth)
VALUES ('ACC1000112', 'Invalid Customer Date Format', '1.1.2015');
```

Try to insert NULL  for key
```
INSERT INTO customers_pk (account_number, customer_name, city)
VALUES (CAST(NULL AS STRING), 'Ghost User', 'London');
```

Try to insert incorrect column count
```
INSERT INTO customers_pk 
SELECT 
    'ACC999' as account_number, 
    'Broken Entry' as customer_name; 
```

Create a new table and copy some correct messages from faker. Stop it after 1 minute.
```
CREATE TABLE customers_poisoned AS
SELECT *
FROM `customers_faker`;
```

Enable error handling for the source table
```
ALTER TABLE `customers_poisoned` SET (
  'error-handling.mode' = 'log',
  'error-handling.log.target' = 'dlq_topic'
) 
```
Check if enabled
```
SHOW CREATE TABLE `customers_poisoned`
```

Start insert from the poisoned topic
```
INSERT INTO `customers_pk` SELECT * FROM `customers_poisoned`
```

Manually insert couple incorrect message to the Kafka topic `customers_poisoned` (produce without schema)
```
{"id": 1, "payload": "poisoned pill"}
```

Check DLQ log
```
SELECT * FROM `dlq_topic`
```

#### Carry Over Offset
https://docs.confluent.io/cloud/current/flink/operate-and-deploy/carry-over-offsets.html

Create new trasactions_materialized table with transaction data from the faker
```
CREATE TABLE trasactions_materialized AS
SELECT * FROM `transactions_faker`;
```

Stop the Statement and check for the latest offsets.
Migrate it to the new version from the previous CTAS
```
SET 'sql.tables.initial-offset-from'  = 'workshop-988cdb34-58a1-4331-b79d-02b829b587ae';
SET 'client.statement-name' = 'my-new-statement123';
CREATE TABLE trasactions_materialized_carryover AS
SELECT * FROM `trasactions_materialized`;
```
