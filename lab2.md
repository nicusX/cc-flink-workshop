# Lab 2

### Highly State-Intensive Operators

Use demo data from Lab 1

#### Temporal Join
Join the transactions_faker (the heart-beat stream) with customers_pk (the versioned dimension table).

```
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
    
#### Interval Join
Interval Join looks for occurrences where a transaction and a customer creation event happened within a specific time window of each other.

```
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

#### Regular Join with TTL

Without TTL
```
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

With TTL
```
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
Use `EXPLAIN` with the above joins to understand the different operators used. 

#### MongoDB Lookup Join

Define connectivity to the MongoDB (requires username/password)

```
CREATE CONNECTION mongodb_connection
  WITH (
    'type' = 'mongodb',
    'endpoint' = 'mongodb+srv://janscluster.emg9svf.mongodb.net/',
    'username' = '<atlas_username>',
    'password' = '<atlas_password>'
  );
```

Create table with MongoDB connectivity with the lookup schema
```
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

Select movies from the year that customer was born

```
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
https://docs.confluent.io/cloud/current/ai/external-tables/key-search.html#key-search-with-mongodb
