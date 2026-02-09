# Lab 2 - part 1

In this lab we will examine state-intensive operations such as joins and aggregations.

## Joins

In the first part of this lab we will explore some of the most commonly used join patterns.
We will also use `EXPLAIN` to understand when a statement may be state-intensive and potentially cause stability and performance issues in the long term.

### 0 - Generated data 

In this lab we will be using the customers and transaction fake data created in [Lab 1](./lab1.md).

If you destroyed those tables, go back to [Lab 1](./lab1.md) and create both `transactons_faker` and `customers_faker` tables.


### 1 - Temporal Join

Let's start joining the fast moving `transactions_faker` stream (left input), 
with the versioned dimension table `customers_pk` (right input), representing the state of each customer over time.


> This statement uses the `customers_pk` from [Lab 1](./lab1.md).
> Make sure the CTAS statement which defines and populate `customers_pk` is still running. 
> If you have stopped it, no new data are generated. Drop the `customers_pk` table and re-created it. 
> Keep that statement running while you experiment with the next query.


```sql
SELECT 
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_pk FOR SYSTEM_TIME AS OF t.`timestamp` AS c
    ON t.account_number = c.account_number;
```

This query joins the incoming transactions with the customer record (with the corresponding `account_number`) retrieving the customer record version valid at the point in time of the transaction `timestamp`.

> ⚠️ Note that the versioned table we are joining is `customers_pk`, which has a primary key, and not the *faker* table `customers_faker`.
> Temporal joins require that the versioned table has a primary key.
> Conversely, no primary key is required for the left input stream.


### 2 - Interval Join

Alternatively, we can join transaction records with corresponding customer records with a creation time (`created_at`) 
within a given interval from the transaction timestamp.

```sql
SELECT 
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number
WHERE t.`timestamp` BETWEEN c.created_at AND c.created_at + INTERVAL '10' SECOND;
```

In this case, a transaction is joined with the corresponding customer record created between 10 seconds
**before** the transaction and the transaction timestamp 
(or, put the other way around, the transaction `timestamp` must lay between the customer record creation time and 10 seconds later).

> ⚠️ Note that the interval join does not require any primary key. In fact, we can join directly `transactions_faker` with `customers_faker`.


When you ran the interval join statement you might have noticed a warning about "state-intensive operators without TTL".
Without a TTL, the state of this statement may potentially grow unbounded causing performance and stability issues.


#### Use EXPLAIN to identify state-intensive operators

We can use `EXPLAIN` on the previous statement to identify whether it contains a state-intensive operator and whether it has a TTL.

Just prepend `EXPLAIN` to the query to see the query plan:

```sql
EXPLAIN SELECT 
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number
WHERE t.`timestamp` BETWEEN c.created_at AND c.created_at + INTERVAL '10' SECOND;
```


In the `== Physical Details ==`, which shows the details of each operator, you may notice an entry similar to the following:

```
[8] StreamJoin
Changelog mode: append
State size: high
State TTL: never
```

This hightlights an operator potentially state-intensive which has no TTL applied.

> Note: the Inteval join we tested before also has a state-intensive operator with no TTL

Before learning how to set a TTL, lets see another type of join.

### 3 - Regular Joins (without TTL)

A regular join, also called equi-join, is similar to a query you normally run in a relational database.
Records on the right side are matched with all records on the left side which satisfy the join condition (same `account_number` in this case).

Execute the following statement:

```sql
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

You may notice that each transaction is actually matched with all the customers which are created over time: 
Apply a filter on the `txn_id` column and select a single value. You will see how the customers matching a single transaction change over time.

Our transactions and customers tables are in fact unbounded and change over time.
To match any transactions with any customers, past or future, with a matching `account_number` Flink retains all transactions and all customers in state... forever by default.
This may cause the state of the join operator to grow unbounded, causing issues.

#### Use EXPLAIN to identify the state-intensive operator

`EXPLAIN` the query above to see the state-intensive operator:

```sql
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

In the `== Physical Details ==` you will notice a state-intenstive operator without TTL:

```
[8] StreamJoin
Changelog mode: append
State size: high
State TTL: never
```


### 4 - Regular join with TTL

You can set a state TTL for a statement setting the `sql.state-ttl` property, before the statement itself.


```sql
SET 'sql.state-ttl' = '2d';

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

Running this statement you will notice no warning is issues.

#### EXPLAIN the new query

`EXPLAIN` the new statement, to see how it has changed (notice, the `EXPLAIN` goes after the `SET` property statement):

```sql
SET 'sql.state-ttl' = '2d';

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

Scrolling through `== Physical Details ==` you will notice that the `StreamJoin` operator has now changed:

```
[8] StreamJoin
Changelog mode: append
State size: medium
State TTL: 2 d
```

State size is now `medium` and we have an TTL. 


### 3 - Lookup joins

Lookup joins allow enriching a streaming dataset with a dataset residing in an external database, looking up the matching entries by key.

Lookup joins use a `CONNECTION` to an external database or endpoint, 
and the [`KEY_SEARCH_AGG`](https://docs.confluent.io/cloud/current/ai/external-tables/key-search.html)
function to lookup data in the external source.

In this workshop we will use MongoDB Atlas and a sample dataset freely available.

> Differently from temporal, interval, and regular joins, lookup joins are not state intensive.

#### 3.0 - Set up MongoDB

To demonstrate lookup joins we need to set up a connection to a MongoDB Atlas cluster containing the `sample_mflix` dataset.

If you don't have a MongoDB cluster with this sample dataset, you can create a free cluster following
the [MongoDB Atlas setup](mongodb-setup.md) instructions.

You require the following information to proceed:
* Atlas cluster endpoint - something like `mongodb+srv://mycluster.abc1234.mongodb.net/`
* DB username
* DB password

> ⚠️ the MongoDB cluster must allow connecting from any IP

> ⚠️ the MongoDB cluster must contain the `sample_mflix` sample database 


#### 3.1 - Create a MongoDB connection

Create a MongoDB connection in Flink to set up the connectivity, replacing endpoint, username, and password with the actual values:

```sql
CREATE CONNECTION mongodb_connection
  WITH (
    'type' = 'mongodb',
    'endpoint' = '<atlas_endpoint>',
    'username' = '<atlas_username>',
    'password' = '<atlas_password>'
  );
```

Verify the connection has been correctly created:

```sql
DESCRIBE CONNECTION mongodb_connection
```

#### 3.2 - Create Flink table representing the MongoDB document schema

For the lookup we will use the `movie` table which is part of the `sample_mflix` sample database available in MongoDB.

The schema of the Flink table must match the fields of the MongoDB Document.
You do not need to include all fields in the Document. Only those you want to use in Flink.

Run the following statement to create the MongoDB table:

```sql
CREATE TABLE mongodb_movies_key_search (
  title STRING,
  genres ARRAY<STRING>,
  rated STRING,
  plot STRING,
  `year` INT
) WITH (
  'connector' = 'mongodb',
  'mongodb.connection' = 'mongodb_connection',
  'mongodb.database' = 'sample_mflix',
  'mongodb.collection' = 'movies',
  'mongodb.index' = 'default'
);
```

#### 3.3 - Lookup join with the external table

For the sake of this workshop, we want to do a join between customers and movies, from the MongoDB table.
We want to match each customer with all movies released in their year of birth.

For more details about lookup to MongoDB from Flink
see [Key search with MongoDB](https://docs.confluent.io/cloud/current/ai/external-tables/key-search.html#key-search-with-mongodb) documentation.

Run the following query:

```sql
SELECT 
  customer_name, 
  date_of_birth, 
  search_results 
FROM customers_faker,
  LATERAL TABLE(
     KEY_SEARCH_AGG(
       `mongodb_movies_key_search`, 
        descriptor(`year`), 
        CAST(YEAR(date_of_birth) AS INT))
  )
```

> For each record `KEY_SEARCH_AGG` returns and array containing all attributes defined in the `mongodb_movies_key_search`
> for **all** matching movies.
