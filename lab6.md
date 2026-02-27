# Lab 6: Error Handling and Statement Operations

In this lab we will see how to set up custom error handling and execute common statement operations.

### Prerequisites

In this lab we will be using the customers and transaction fake data created in [Lab 1](./lab1.md).
If you destroyed those tables, go back to [Lab 1](./lab1.md) and create both `transactions_faker` and `customers_faker` tables.

We will also use the `customers_pk` table created in [Lab 1](./lab1.md).

> ⚠️ If the CTAS statement copying data into `customers_pk` is no longer running, you may not see any incoming data. 
> Start and keep running the following statement: `INSERT INTO customers_pk SELECT * FROM customers_faker`.

### 1 - Error Handling

When you write data to a table Flink enforces schema validation. 

Everything is fine as long as the inserted data matches the schema:

```sql
INSERT INTO customers_pk (account_number, customer_name, city)
VALUES ('ACC1000111', 'Bob Smith', 'Munich');
```

#### 1.1 - Default error handling (`fail`)

However, if you try to insert a record that does not match the schema, by default you get an error and the statement fails.

Invalid data type:

```sql
INSERT INTO customers_pk (account_number, customer_name, date_of_birth)
VALUES ('ACC1000112', 'Invalid Customer Date Format', '1.1.2015');
```

Null PK:

```sql
INSERT INTO customers_pk (account_number, customer_name, city)
VALUES (CAST(NULL AS STRING), 'Ghost User', 'London');
```

Incorrect column count:

```sql
INSERT INTO customers_pk 
SELECT 
    'ACC999' as account_number, 
    'Broken Entry' as customer_name; 
```

#### 1.2 - Custom Error Handling (`log` - aka DLQ)

You can set up custom error handling to send any record violating the schema (i.e. failing deserialization)
 to a DLQ (Dead Letter Queue) table.

See [Custom error handling](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html#custom-error-handling) for more details.


1. Use a CTAS statement to create a new table and copy some correct messages from faker. ⚠️ Stop this statement after about one minute.

```sql
CREATE TABLE customers_poisoned AS
SELECT *
FROM `customers_faker`;
```

2. Enable error handling on this table:

```sql
ALTER TABLE `customers_poisoned` SET (
  'error-handling.mode' = 'log',
  'error-handling.log.target' = 'dlq_topic'
) 
```


> ℹ️ Enabling the DLQ on a table automatically creates a new table and a new topic for the DLQ, in this case called `dlq_topic` (the name is arbitrary)

3. Check if error handling is correctly enabled:

```sql
SHOW CREATE TABLE `customers_poisoned`
```

4. Start inserting record from the poisoned topic into `customers_pk`

```sql
INSERT INTO `customers_pk` SELECT * FROM `customers_poisoned`
```

> ⚠️ Keep this statement running. Run the next as a separate statement.

5. Manually insert some incorrect messages into the `customers_poisoned` Kafka topic.
   To insert records that violate the schema we cannot use Flink. 
   We need to put the record directly into the topic. Using the UI, navigate to the topic:

  - *Environments* > [select your environment] > *Clusters* > [select your cluster] > *Topics* > `customers_poisoned`
  - *Actions* > *Produce a new message* 
  - *Value* > select *Produce without schema*
  - Copy & Paste the following message in *Value*

```json
{"id": 1, "payload": "poisoned pill"}
```

  - Click *Produce*


6. Go back to the Flink SQL Workspace and check the DLQ, where you will find our offending message:

```sql
SELECT * FROM `dlq_topic`
```

---


### 2 - Carry-over Offset

When you have production Flink statements continuously running, you may want to deploy a new version of the statement.
You normally want to start the new statement exactly from the point (offset) in the source topic where the old statement stopped.

You can use [Carry-over Offset](https://docs.confluent.io/cloud/current/flink/operate-and-deploy/carry-over-offsets.html) for this goal.


We should already have a table called `city_spend_report` from [Lab 5](./lab5.md).

1. Let's run a statement which inserts some records in this table (ignore the warning). Keep this statement running for now.

```sql
INSERT INTO city_spend_report
  SELECT 
    SUM(t.amount),
    c.city
FROM transactions_faker t
JOIN customers_pk c ON t.account_number = c.account_number
GROUP BY c.city;
```

2. While the previous statement is running, go to the Flink Compute Pool UI 
   (*Environments* > [select your environment] > *Compute pools* > [select your compute pool]).
   Scroll down the page to *Statements associated with this pool*

3. Find the running statement and copy the statement ID. It should be something like `<workspace-name>-abcde1234-abcd-1234-0123456789ab`   

4. Create a new destination table which we will use for a new statement that will replace our previous 

```sql
CREATE TABLE city_spend_report_carryover(
  city STRING,
  total_spent DOUBLE,
  total_transactions INT,
  PRIMARY KEY(`city`) NOT ENFORCED
)
```

5. Stop the previous `INSERT INTO city_spend_report...` statement

> ℹ️ If you cannot find the statement in the SQL Workspace, you can stop it from the Flink Compute Pool UI


> ℹ️ Once the statement is stopped, in the Flink Compute Pool UI you can find the latest offsets consumed from `customers_pk`.
> However, we do not need to keep note of them because *Carry-over Offset* will do the job for us, automatically.


6. Create a new statement to replace the one we just stopped.
   Replace the value of the option `sql.tables.initial-offset-from` with the ID of the statement we just stopped:

```sql
SET 'sql.tables.initial-offset-from'  = '<previous statement ID>';
INSERT INTO city_spend_report_carryover
  SELECT 
    c.city,
    SUM(t.amount) AS total_spent,
    CAST (COUNT(t.amount) AS INT) AS total_transactions 
FROM transactions_faker t
JOIN customers_pk c ON t.account_number = c.account_number
GROUP BY c.city;
```

> ⚠️ If you run the new statement before stopping the statement referred by `sql.tables.initial-offset-from`, the new statement stays in *Pending* until you stop the old one.


7. Only new cities are sent to the new table (⚠️ run this as a separate statement):

```sql
SELECT * FROM `city_spend_report_carryover`
```

---
---

You have reached the end of the workshop.

We hope this gave you a better understanding of Flink SQL capabilities and how to optimize your Flink workloads.

Do not forget to stop all your statements, shut down the Compute Pool and the Cluster, if you created them for this workshop.
 