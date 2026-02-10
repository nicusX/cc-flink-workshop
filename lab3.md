# Lab 3: Options and SQL Hints, VIEWS & ALTER TABLE

In this lab we will learn how to set options at statement level and via SQL hints.
We will explore some of the most useful options.
We will then see how Views and Common Table Expressions (CTE) can improve readability of complex pipelines.  

### Prerequisites

In this lab we will be using the customers and transaction fake data created in [Lab 1](./lab1.md).
If you destroyed those tables, go back to [Lab 1](./lab1.md) and create both `transactions_faker` and `customers_faker` tables.

We will also use the `customers_pk` table created in [Lab 1](./lab1.md).
If the CTAS statement is not running you may not see any incoming data. However, you need to drop the `customers_pk` table before re-running the CTAS statement which creates and populates it.

### 1 - SET options

`SET` allows setting options that are applied to all operators in the same statement.

You can prepend one or more `SET ...` to a SQL statement.
For example: 

```sql
SET 'sql.state-ttl' = '2d';
SET 'sql.tables.scan.startup.mode' = 'latest-offset';
SET 'sql.local-time-zone' = 'CET';
SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```

See [Available SET Options](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#available-set-options)
for a complete list.

We have already used the `sql.state-ttl` option in [lab 2](lab2.md). 
Let's explore some other commonly used options.

#### 1.1 - Dry run

When you set Dry Run = true Flink will just parse and validate the query and then do nothing: no table is created, no data processing is started.


```sql
SET 'sql.dry-run' = 'true';
SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```

#### 1.2 - Snapshot Queries

> ⚠️ as of February 2026, Snapshot Queries are currently in Early Access. Skip this section unless this feature is enabled in your Confluent Cloud organization.

Enabling snapshot query forces Flink to process data in a topic as a bounded set, from the earliest available record to the latest offset when the query is submitted.
Differently from the default streaming mode, a snapshot query ends when it has processed all data up to the latest record.

If Tableflow is not enabled on the topic, the query will read the old data from the topic, up to the topic retention.
If Tableflow is enabled on the topic, the query will also read the historical data from the Iceberg table and then read only the latest records from the topic.


See [Snapshot Queries in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html) for more details.

To enable Snapshot Queries just set `sql.snapshot.mode` = `now` (no other value is supported at the moment):


```sql
SET 'sql.snapshot.mode' = 'now';
SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```

A Snapshot Query doesn't provide any intermediate results. The complete result set is emitted only when the query has completed.
This may take some time.

##### Batch vs Stream operators

Enabling Snapshot Query mode substantially changes the operators which Flink uses to process the data.

Try explaining the same query, with and without snapshot mode enabled.


```sql
-- (default) Streaming mode
EXPLAIN SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```


```sql
-- Snapshot mode
SET 'sql.snapshot.mode' = 'now';
EXPLAIN SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```

As you can see, in the default streaming mode, all operators are called `Stream*`, while in Snapshot Queries they are called `Batch*`.

Also note, batch operators used by Snapshot Queries have no state.


#### 1.3 - Disable Watermark Alignment

[Watermark alignment](https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html#flink-sql-watermarks-watermark-alignment) is enabled by default in Confluent Cloud Flink.

Watermark alignment improves performance and stability when you are doing temporal joins or similar operations between two streams, and one stream progress significantly faster than the other. This is especially true when reprocessing historical data from long retention topics.

However, there are situations where Watermark alignment may block processing. For example, if you have two sources, one of them is considerably slower than the other, and you are not performing any temporal join between them.

In these cases, you may want to disable Watermark Alignment, setting the `sql.tables.scan.watermark-alignment.max-allowed-drift` to `0`.

```sql
SET 'sql.tables.scan.watermark-alignment.max-allowed-drift' = '0';
SET 'sql.tables.scan.idle-timeout' = '500ms';
SET 'sql.state-ttl' = '5 seconds';
SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```

---

### 2 - HINTS

Instead of setting an option at statement level, using `SET`, you can specify or override them at operator level (i.e. for each table or alias in a statement).

This is attained by using [Hints](https://docs.confluent.io/cloud/current/flink/reference/statements/hints.html) after the `SELECT` or `FROM` keywords in the query.

There are two different types of Hints: one for setting [table property](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#flink-sql-with-options) and another to set [State TTL](https://docs.confluent.io/cloud/current/flink/reference/statements/hints.html#state-ttl-hints)


> ⚠️ If you use both `SET` and Hints in the same statement, `SET` specify the default while hints override it for specific operators.

#### 2.1 - Override Table Options with Hints


You can, for example, override the default [`scan.startup.mode`](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#scan-startup-mode) for a specific table only in a specific statement.

```sql
SELECT /*+ OPTIONS('scan.startup.mode' = 'latest-offset' ) */
  account_number,
  customer_name,
  created_at
FROM customers_pk
```

> ℹ  this approach differs from adding the same table option to the `customers_pk`, using the `WITH` block or `ALTER TABLE ... SET ...` as we have seen in [Lab 1](lab1.md).
> An option added directly to the table applies to all statements using that table.
> Conversely, a Hint only applies to the current query

#### 2.2 - State TTL Hints

Hints can be used to set TTL per topic.

You can also combine table option hints and TTL hints in the same query.

```sql
SELECT /*+ STATE_TTL('t'='10s', 'c'='1h') */
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker /*+ OPTIONS('scan.startup.mode'='latest-offset') */  AS t
JOIN customers_pk /*+ OPTIONS('scan.startup.mode'='earliest-offset') */  AS c 
    ON t.account_number = c.account_number;
```



---

### 3 - ALTER TABLE

We have already used the [`ALTER TABLE`](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html) statement in [Lab 1](lab1.md).
Let's see some more examples.

> ⚠️ In Flink `ALTER TABLE` has limitations, compared to relational databases. In fact, you cannot alter the table schema except for adding computed and metadata columns. See [ALTER TABLE Statement in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html#description) for more details.


#### 3.1 - Change a table property

You can modify a [table property](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#flink-sql-with-options), i.e. the properties you can set in `CREATE TABLE ... WITH ( ... )`:

```sql
ALTER TABLE `transactions_faker` SET ('rows-per-second' = '20')
```

This modifies the table definition permanently. 

#### 3.2 - Modify table metadata

You can add or modify table metadata colums, such as watermarks:

```sql
ALTER TABLE `transactions_faker` 
MODIFY WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECONDS;
```


#### 3.3 - Add computed columns

You can add computed columns

```sql
ALTER TABLE `transactions_faker` 
ADD my_time AS CONVERT_TZ(CAST(`timestamp` AS STRING), 'UTC', 'Europe/Berlin')
```

> ⚠️ You cannot add simple colums. For example, this will return an error: `ALTER TABLE transactions_faker ADD new_column STRING`


##### Verify the changes

You can verify the changes to the `transactions_faker` table:

```sql
DESCRIBE EXTENDED `transactions_faker`
```

```sql
SHOW CREATE TABLE `transactions_faker`
```


#### 3.4 - Modify Changelog Mode

The `customers_pk` should be using `upsert` changelog mode at the moment, as you can verify showing the table definition:

```sql
SHOW CREATE TABLE `customers_pk`
```

You can also see it directly querying the table and switching to *Changelog View*

```sql
SELECT * FROM `customers_pk`
```

Switching to *Changelog View* you should see `+I`, `-U`, and `+U` records flowing.

> ⚠️ Stop the select query before modifying Changelog Mode.


Modify the Changelog Mode to `append`:

```sql
ALTER TABLE `customers_pk` SET ('changelog.mode' = 'append')
```

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

### 4 - Common Table Expressions (CTE)

If you have a complex statement, you can define an alias for a subquery using `WITH ... AS ( ... )` to improve readability.

These definitions are called *Common Table Expressions (CTE)*

For example, we want to calculate the total withdrawal amount for each account number. Flag & Filter those transactions which breach the $10000 threshold and also Flag & Filter  those transactions if the total amount spent on a particular currency is more than $6000 in the last 2 hours.


```sql
WITH flagged_withdrawal AS (
    SELECT 
        account_number,
        currency,
        transaction_type,
        amount,
        merchant, 
        location,
        SUM(amount) OVER w AS total_value,
        CASE 
            WHEN SUM(amount) OVER w > 1000 THEN 'YES' 
            ELSE 'NO' 
        END AS flag
    FROM transactions_faker
    WHERE transaction_type = 'withdrawal'
    WINDOW w AS (
        PARTITION BY account_number, transaction_type
        ORDER BY `timestamp` ASC
        RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW
    )
)
SELECT 
    * FROM flagged_withdrawal 
WHERE flag = 'YES'
```


CTE are not only used to improve readability. They are also useful to overcome some limitations of Flink SQL.

For example, you cannot include two separate `OVER()` windows in the same query or subquery. A workaround would be creating separate queries but this is wasteful and redundant. We can instead use multiple CTE to calculate the aggregations over different `OVER` windows.


```sql
WITH flagged_withdrawal_tx AS (
    SELECT
        txn_id,
        account_number,
        SUM(amount) OVER w AS total_value,
        CASE
            WHEN SUM(amount) OVER w > 500 THEN 'YES'
            ELSE 'NO'
        END AS withdraw_flag
    FROM transactions_faker
    WHERE transaction_type = 'withdrawal' 
      AND status = 'Successful'
    WINDOW w AS (
        PARTITION BY account_number, transaction_type
        ORDER BY `timestamp` ASC
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    )
),
flagged_currency_tx AS (
    SELECT
        txn_id,
        account_number,
        SUM(amount) OVER w AS total_value,
        CASE
            WHEN SUM(amount) OVER w > 300 THEN 'YES'
            ELSE 'NO'
        END AS fx_flag
    FROM transactions_faker
    WHERE transaction_type <> 'refund' 
      AND status = 'Successful'
    WINDOW w AS (
        PARTITION BY account_number, currency
        ORDER BY `timestamp` ASC
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    )
)
SELECT
    a.*,
    b.total_value AS withdrawal_total,
    COALESCE(b.withdraw_flag, 'NO') AS withdraw_flag,
    c.total_value AS forex_total,
    c.fx_flag
FROM transactions_faker AS a
LEFT JOIN flagged_withdrawal_tx AS b
    ON a.txn_id = b.txn_id 
    AND a.account_number = b.account_number
LEFT JOIN flagged_currency_tx AS c
    ON a.txn_id = c.txn_id 
    AND a.account_number = c.account_number
WHERE b.withdraw_flag = 'YES' 
   OR c.fx_flag = 'YES'
```

---

### 5 - Views

Sometimes, instead of creating complex statement with multiple subqueries or CTE, it may be convenient to split a complex statement 
into [views](https://docs.confluent.io/cloud/current/flink/reference/statements/create-view.html). 

Views can help with readability and also testability, during the development. You can query them directly as if they were tables and see the intermediate results. 

Refactor the previous query, moving the two CTE into separate views:

```sql
CREATE VIEW flagged_withdrawal_tx AS
SELECT
    txn_id,
    account_number,
    SUM(amount) OVER w AS total_value,
    CASE
        WHEN SUM(amount) OVER w > 500 THEN 'YES'
        ELSE 'NO'
    END AS withdraw_flag
FROM transactions_faker
WHERE transaction_type = 'withdrawal' 
  AND status = 'Successful'
WINDOW w AS (
    PARTITION BY account_number, transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
);
```

```sql
CREATE VIEW flagged_currency_tx AS
SELECT
    txn_id,
    account_number,
    SUM(amount) OVER w AS total_value,
    CASE
        WHEN SUM(amount) OVER w > 300 THEN 'YES'
        ELSE 'NO'
    END AS fx_flag
FROM transactions_faker
WHERE transaction_type <> 'refund' 
  AND status = 'Successful'
WINDOW w AS (
    PARTITION BY account_number, currency
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
);
```

You can now refer to the views, in the main query, as if they were tables:

```sql
SELECT
    a.*,
    b.total_value AS withdrawal_total,
    COALESCE(b.withdraw_flag, 'NO') AS withdraw_flag,
    c.total_value AS forex_total,
    c.fx_flag
FROM transactions_faker AS a
LEFT JOIN flagged_withdrawal_tx AS b
    ON a.txn_id = b.txn_id 
    AND a.account_number = b.account_number
LEFT JOIN flagged_currency_tx AS c
    ON a.txn_id = c.txn_id 
    AND a.account_number = c.account_number
WHERE b.withdraw_flag = 'YES' 
   OR c.fx_flag = 'YES';
```

Note that you can also query a view directly, to explore the intermediate results:

```sql
SELECT * FROM `flagged_withdrawal_tx`
```


> ℹ Confluent Cloud Flink charges only for DML statements which process data, not for purely DDL statements like `CREATE TABLE` or `CREATE VIEW`. Moving subqueries or CTE into Views does not change the actual billing.
> Note that a [CTAS](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#create-table-as-select-ctas) statement is not purely DDL. It actually "runs" and uses resources, therefore it is billed.



Any query can be transformed into a view, and be referenced in other statements as a table.
In fact, you can transform the entire nested query into a view:

```sql
CREATE VIEW flagged_trx_view AS

WITH flagged_withdrawal_tx AS (
    SELECT
        txn_id,
        account_number,
        SUM(amount) OVER w AS total_value,
        CASE
            WHEN SUM(amount) OVER w > 500 THEN 'YES'
            ELSE 'NO'
        END AS withdraw_flag
    FROM transactions_faker
    WHERE transaction_type = 'withdrawal' 
      AND status = 'Successful'
    WINDOW w AS (
        PARTITION BY account_number, transaction_type
        ORDER BY `timestamp` ASC
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    )
),
flagged_currency_tx AS (
    SELECT
        txn_id,
        account_number,
        SUM(amount) OVER w AS total_value,
        CASE
            WHEN SUM(amount) OVER w > 300 THEN 'YES'
            ELSE 'NO'
        END AS fx_flag
    FROM transactions_faker
    WHERE transaction_type <> 'refund' 
      AND status = 'Successful'
    WINDOW w AS (
        PARTITION BY account_number, currency
        ORDER BY `timestamp` ASC
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    )
)
SELECT
    a.*,
    b.total_value AS withdrawal_total,
    COALESCE(b.withdraw_flag, 'NO') AS withdraw_flag,
    c.total_value AS forex_total,
    c.fx_flag
FROM transactions_faker AS a
LEFT JOIN flagged_withdrawal_tx AS b
    ON a.txn_id = b.txn_id 
    AND a.account_number = b.account_number
LEFT JOIN flagged_currency_tx AS c
    ON a.txn_id = c.txn_id 
    AND a.account_number = c.account_number
WHERE b.withdraw_flag = 'YES'
   OR c.fx_flag = 'YES';
```

> ℹ Note that no data processing happens when you create a view. 
> Conversely, submitting a `SELECT` query starts data processing and results being emitted to the UI.



#### Review the View definition and emitted schema


You can review the View definition:

```sql
SHOW CREATE VIEW flagged_trx_view
```

You can also show the schema emitted by the query:


```sql
DESCRIBE flagged_trx_view
```

#### Referring to a View in a query

As we have seen, you can refer to a View in any query, as if it were a table:

```sql
SELECT 
    txn_id,
    account_number,
    CONCAT(
        'Dear user ', 
        CAST(account_number AS STRING),
        ', your transaction of ',
        CAST(amount AS STRING),
        ' cannot be processed due to lack of ',
        CAST(currency AS STRING),
        ' balance available on your card.'
    ) as message
FROM flagged_trx_view 
    WHERE withdraw_flag = 'YES';
```

