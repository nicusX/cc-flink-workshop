# Lab 4: Changelog Modes and Advanced Operators

In this lab we will learn more about different Changelog Modes.
We will also get a better understanding of some of the operators that comprise the physical plan of a statement and that may have impact on performance and stability of your Flink statements.

### Prerequisites

In this lab we will be using the customers and transaction fake data created in [Lab 1](./lab1.md).
If you destroyed those tables, go back to [Lab 1](./lab1.md) and create both `transactions_faker` and `customers_faker` tables.

We will also use the `customers_pk` table created in [Lab 1](./lab1.md).
If the CTAS statement is not running, you may not see any incoming data. However, you need to drop the `customers_pk` table before re-running the CTAS statement which creates and populates it.


### 1 - Modify Changelog Modes

In the previous lab, we have seen how you can modify [table property](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#flink-sql-with-options) using `ALTER TABLE`.


The `changelog.mode` table property controls the [*Changelog Mode*](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#changelog-mode) emitted by the table. 

#### Show the Changelog Mode of a table

Verify the current changelog mode of the `customers_pk` table (should be `upsert` if you haven't changed it):

```sql
SHOW CREATE TABLE `customers_pk`
```

#### Visualize the Changelog

You can also see it by directly querying the table and switching to *Changelog View*

```sql
SELECT * FROM `customers_pk`
```

Switching to *Changelog View* you should see `+I`, `-U`, and `+U` records flowing.


#### Modify Changelog mode


Modify the Changelog Mode to `append` ( ⚠️ Stop the select query before modifying):

```sql
ALTER TABLE `customers_pk` SET ('changelog.mode' = 'append')
```

#### Show and visualize the new Changelog Mode

Verify the table definition:

```sql
SHOW CREATE TABLE `customers_pk`
```

Scan the table again:

```sql
SELECT * FROM `customers_pk`
```

Switching to *Changelog View* you should now see only `+I` records.

---

### 2 - Operators examples

In [Lab 1](./lab1.md), we have seen how we can use `EXPLAIN` to see the query plan (physical plan) of a statement, and the operators it comprises.

Some of these operators can be particularly impactful on performance and stability of the query. Let's see some examples.

* [**StreamExchange**](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#streamexchange) (shuffle): this operator indicates redistribution of data between parallel instances, to ensure all the records with the same key are processed by the same Flink internal parallel task. Having some *StreamExchange* in a statement is often inevitable. However, several *StreamExchange* may impact performance.
* [**StreamGroupAggregate**](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#streamgroupaggregate): performs group aggregations. Can become state heavy if there is no TTL and the number of keys keep growing.
* [**StreamChangelogNormalize**](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#streamchangelognormalize): when an Append stream has to be written into a table with a Primary Key, Flink has to convert it into an Upsert/Retract stream. This can become state-heavy as Flink has to keep in state one record for each key.
* [**StreamSink**](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#streamsink) **with Upsert mode**: Similarly to *StreamChangelogNormalize*, sinks in upsert mode may retain one record for each key, with potentially high state impact.

See [Physical Operators](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#physical-operators) for a descriptions of all operator types returned by `EXPLAIN`.


Let's see a couple of examples of not-so-great queries.


#### 2.1 - Example 1 - State-intensive operators without TTL

We want to calculate the total spend of every city based on incoming transactions.

The following query can do the job:

```sql
SELECT 
    c.city,
    SUM(t.amount) AS total_spent,
    COUNT(t.txn_id) AS transaction_count
FROM transactions_faker AS t
JOIN customers_pk AS c 
    ON t.account_number = c.account_number
GROUP BY c.city;
```

Let's explain it:

```sql
EXPLAIN SELECT 
    c.city,
    SUM(t.amount) AS total_spent,
    COUNT(t.txn_id) AS transaction_count
FROM transactions_faker AS t
JOIN customers_pk AS c 
    ON t.account_number = c.account_number
GROUP BY c.city;
```

The output should be similar to the following:

```
== Physical Plan ==

StreamSink [13]
  +- StreamGroupAggregate [12]
    +- StreamExchange [11]
      +- StreamCalc [10]
        +- StreamJoin [9]
          +- StreamExchange [5]
          :  +- StreamCalc [4]
          :    +- StreamWatermarkAssigner [3]
          :      +- StreamCalc [2]
          :        +- StreamTableSourceScan [1]
          +- StreamExchange [8]
            +- StreamCalc [7]
              +- StreamTableSourceScan [6]

== Physical Details ==

[1] StreamTableSourceScan
Table: `my-env`.`cluster_0`.`transactions_faker`
Changelog mode: append
State size: low
Startup mode: earliest-offset

[6] StreamTableSourceScan
Table: `my-env`.`cluster_0`.`customers_pk`
Primary key: (account_number)
Changelog mode: append
Upsert key: (account_number)
State size: low
Startup mode: earliest-offset
Key format: avro-registry
Key registry schemas: (:.:customers_pk/100003)
Value format: avro-registry
Value registry schemas: (:.:customers_pk/100002)

[7] StreamCalc
Changelog mode: append
Upsert key: (account_number)

[8] StreamExchange
Changelog mode: append
Upsert key: (account_number)

[9] StreamJoin
Changelog mode: append
State size: high
State TTL: never

[12] StreamGroupAggregate
Changelog mode: retract
Upsert key: (city)
State size: medium
State TTL: never

[13] StreamSink
Table: Foreground
Changelog mode: retract
Upsert key: (city)
State size: low

== Warnings ==

Critical for entire query: Your query includes one or more highly state-intensive operators but does not set a time-to-live (TTL) value, which means that the system potentially needs to store an infinite amount of state. This can result in a DEGRADED statement and higher CFU consumption. If possible, change your query to use a different operator, or set a time-to-live (TTL) value. For more information, see https://cnfl.io/high_state_intensive_operators.

```

You can see the query has multiple *StreamExchange*. 
Also, the *StreamJoin* and *StreamGroupAggregate* do not have any TTL defined.


#### 2.2 - Example 2 - Multiple shuffle


Look for customers who have spent a significant amount of money and aggregate them by location.

The following query does the job:

```sql
SELECT 
    city,
    COUNT(*) as high_spender_count
FROM (
    SELECT 
        c.email,
        c.city,
        SUM(t.amount) as total_spent
    FROM transactions_faker t
    JOIN customers_pk c ON t.account_number = c.account_number 
    GROUP BY c.email, c.city
)
WHERE total_spent > 1000
GROUP BY city;
```

However, if you `EXPLAIN` it you can see this simple query has 4 *StreamExchange*

#### 2.3 - Example 3 - Primary key mismatch

When you have an `INSERT INTO... SELECT` (or an equivalent CTAS statement), which writes into a table with a Primary Key, but the derived key from the `SELECT` statement does not match the PK of the destination, the sink becomes heavily stateful.

See. [Primary key differs from derived upsert key](https://docs.confluent.io/cloud/current/flink/how-to-guides/resolve-common-query-problems.html#primary-key-differs-from-derived-upsert-key) for more details.


Create a table with a Primary Key (`total_spent`):

```sql
CREATE TABLE city_spend_report (
  total_spent DOUBLE,
  city STRING,
  PRIMARY KEY(`total_spent`) NOT ENFORCED
)
```

Insert into this table from a query with a different key (`city`):


```sql
INSERT INTO city_spend_report
  SELECT 
    SUM(t.amount),
    c.city
FROM transactions_faker t
JOIN customers_pk c ON t.account_number = c.account_number
GROUP BY c.city;
```

If you `EXPLAIN` the `INSERT INTO...` statement you can notice the *StreamSink* operator with high state, and the warnings about the the state-heavy sink and lack of TTL:


```
== Physical Details ==

[...]

[14] StreamSink
Table: `my_env`.`cluster_0`.`city_spend_report`
Primary key: (total_spent)
Changelog mode: upsert
Upsert key: (city)
State size: high
State TTL: never
Key format: avro-registry
Key registry schemas: (:.:city_spend_report/100011)
Value format: avro-registry
Value registry schemas: (:.:city_spend_report/100012)

== Warnings ==

1. Critical for StreamSink [14]: The primary key does not match the upsert key derived from the query. If the primary key and upsert key don't match, the system needs to add a state-intensive operation for correction. Please revisit the query (upsert key: [city]) or the table declaration for `my_env`.`cluster_0`.`city_spend_report` (primary key: [total_spent]). Grouping, partitioning, and joining operations should reference the upsert keys of the source tables. For more information, see https://cnfl.io/primary_vs_upsert_key.
2. Critical for entire query: Your query includes one or more highly state-intensive operators but does not set a time-to-live (TTL) value, which means that the system potentially needs to store an infinite amount of state. This can result in a DEGRADED statement and higher CFU consumption. If possible, change your query to use a different operator, or set a time-to-live (TTL) value. For more information, see https://cnfl.io/high_state_intensive_operators.

```

#### 2.4 - Example 4 - JOIN vs OVER WINDOW (Retract vs Append Changelog)

Sometimes, multiple approaches return the same results, but the output has a different Changelog mode.

For example, to combine customers and transactions, we can take two different approaches.

##### Full Outer Join

```sql
SELECT 
    COALESCE(t.account_number, c.account_number) as account_id,
    t.txn_id,
    t.amount,
    c.customer_name,
    c.city
FROM transactions_faker t
FULL OUTER JOIN customers_faker c
    ON t.account_number = c.account_number;
```

The output will be a stream with the most recent transaction and corresponding customer.

If you `EXPLAIN` this query, you can see the output is a [Retract](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#changelog-modes) stream.
The *StreamJoin* operator has a *medium* state size (because the row is small). However, without a TTL the state can potentially grow unbounded and cause problems.


##### UNION ALL + OVER WINDOW 

An alternative approach to solve this problem is by using `UNION ALL` to "merge" the two streams, and then combining matching transactions and customers by an OVER WINDOW.

This approach may sound counterintuitive but is actually more efficient. You can find more details in [Combine Streams and Track Most Recent Records with Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/how-to-guides/combine-and-track-most-recent-records.html).


```sql
WITH combined_data AS (
    SELECT
        account_number,
        txn_id,
        amount,
        CAST(NULL AS STRING) as customer_name,
        CAST(NULL AS STRING) as city,
        `timestamp` as event_time
    FROM transactions_faker
    UNION ALL
    SELECT
        account_number,
        CAST(NULL AS STRING) as txn_id,
        CAST(NULL AS DOUBLE) as amount,
        customer_name, 
        city,
        created_at as event_time
    FROM customers_faker
)
SELECT
    account_number,
    LAST_VALUE(txn_id) OVER w AS last_txn_id,
    LAST_VALUE(amount) OVER w AS last_amount,
    LAST_VALUE(customer_name) OVER w AS last_customer_name,
    LAST_VALUE(city) OVER w AS city
FROM combined_data
WINDOW w AS (
    PARTITION BY account_number 
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
```


If you `EXPLAIN` this statement you can see how all operators have *low* state size and the output is an [Append](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html#changelog-modes) stream.