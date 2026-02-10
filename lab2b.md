# Lab 2 - part 2: Aggregations and Windowing

> This lab uses the `transactions_faker` table created in [Lab 1](./lab1.md). Make sure this table exists before proceeding.

In the second part of this lab we will explore aggregations and windowing.

### 1 - GROUP BY

The simplest aggregation uses `GROUP BY` to aggregate records globally across a stream.

For example, let's sum all withdrawal transactions larger than 500, by `account_number`.


```sql
SELECT 
  account_number,
  transaction_type,
  SUM(amount) 
FROM `transactions_faker` 
WHERE transaction_type = 'withdrawal' 
GROUP BY account_number,transaction_type
HAVING SUM(amount) > 500
```

As you can see, groups keep being added and updated while transactions are generated.

#### State impact of GROUP BY

The state of this statement is not particularly large. Only the key (`account_number`,`transaction_type`) 
and a single value (sum of the amounts) are retained for each key.

However, without setting a TTL the state of this statement may grow unbounded.

You can `EXPLAIN` the query to see the impact of the statement. You will notice an operator like this one:

```
[5] StreamGroupAggregate
Changelog mode: retract
Upsert key: (account_number,transaction_type)
State size: medium
State TTL: never
```

As you can see, state size is estimated to `medium`, but no TTL is set. 
This means the state will only grow over time, potentially causing issues in the long term.


### 2 - OVER() for Running Totals

The `OVER` aggregation is used to produce running totals on every input record.

For example, let's calculate the running amount withdrawn from each account.
We also add a flag (a simple 'YES' or 'NO' string) to highlight when an account has passed the threshold of 500.


```sql
SELECT
    `account_number`,
    transaction_type,
    amount,
    SUM(amount) OVER w as total_value,
    CASE WHEN SUM(amount) OVER  w  > 500 THEN 'YES' ELSE 'NO' END AS FLAG
FROM `transactions_faker`
WHERE transaction_type = 'withdrawal'
WINDOW w AS (
 PARTITION BY account_number,transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
```

This query appends a new record for each new transaction.


An `OVER` aggregation is always defined over a finite range of records, in terms of time interval or number of rows.
In this case we are considering the transactions in the previous hour 
(`RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW`).

The fact that the range is always bounded limits the impact on state, without requiring any TTL

> If you `EXPLAIN` the previous query you can see the state size of all operators is `low`


### 3 - GROUP BY TUMBLE WINDOW

Instead of aggregating globally, "forever", you can define aggregations over time windows.

The simplest is a tumbling window. Each tumbling window has fixed duration, and the next window starts immediately after the previous window ends.

Time windows are by default based on the event time.

For example, let's count all the failed payment transactions for each merchant, over 1 minute intervals:

```sql
SELECT 
  window_start,
  window_end,
  merchant,
  COUNT(*) AS total_tx_failed
FROM
TUMBLE(
  TABLE `transactions_faker`, 
  DESCRIPTOR (`timestamp`), 
  INTERVAL '1' MINUTE
  )
WHERE transaction_type ='payment' AND status = 'Failed'
GROUP BY 
  window_start, 
  window_end,
  merchant
```

> ⚠️ the query emits the first results after 1 minute, when the first window ends. Then new results at 1 minute intervals.


### 4 - GROUP BY SESSION WINDOW

A different type of time window aggregation uses [Session windows](https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html#session).

A session is defined as a continuous sequence of records separated by less than a specified gap of time.

For example, let's count the number of consecutive transactions to each merchant, separated by a maximum gap of 5 seconds.
i.e. if there is a gap of more than 5 seconds between consecutive transactions to a given merchant, a new session window is created for that merchant.

```sql
SELECT
  window_start,
  TIMESTAMPDIFF(SECOND, window_start, window_end) AS sec_duration,
  merchant,
  COUNT(*) AS total_tx
FROM
  SESSION(
    DATA => TABLE `transactions_faker` PARTITION BY merchant, 
    TIMECOL => DESCRIPTOR(`timestamp`), 
    GAP => INTERVAL '5' SECONDS) 
GROUP BY
  window_start,
  window_end,
  merchant;
```

Note that, differently from tumble windows, Session windows can be different for each key (each merchant).


### 5 - OVER WINDOW (Alternative Example)

We have already seen the usage of aggregations `OVER` an interval of time (a window).

Let's see another example: count the number of failed payment transactions, for each merchant, in the last minute.


```sql
SELECT 
  `timestamp`,
  merchant,
  COUNT(*) OVER w AS total_tx_failed_last_minute
FROM `transactions_faker`
WHERE transaction_type = 'payment' AND status = 'Failed'
WINDOW w AS (
  PARTITION BY merchant 
  ORDER BY `timestamp` 
  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
);
```

Note that, differently from `GROUP BY` on `TUMBLE` windows, in this case the result is emitted immediately
when a new matching record is encountered.

### 6 - Combining GROUP BY and OVER WINDOW

`GROUP BY` and `OVER` can be combined in the same query.

For example, we want to:
1. (group by) count the number of successful transactions every minute, per merchant
2. (over) compare the count of the current window with the count of the previous window, using the `LAG` function 


```sql
SELECT 
  window_start,
  window_end,
  merchant,
  transaction_type,
  COUNT(*) as total_successful,
  LAG(COUNT(*),1) OVER w as prev_total_successful,
  COUNT(*) - LAG(COUNT(*),1) OVER w as delta
FROM
TUMBLE(
  TABLE `transactions_faker`, 
  DESCRIPTOR (`timestamp`), 
  INTERVAL '1' MINUTE
  )
WHERE transaction_type ='payment' AND status = 'Successful'
GROUP BY 
  window_start, 
  window_end,
  window_time,
  merchant, 
  transaction_type
WINDOW w as (
  PARTITION BY merchant,transaction_type 
  ORDER BY window_time
  ) 
```

---

### 7 (bonus) - Aggregations, event time, and idle sources

In Flink, many aggregations depend on the continuous flow of new events. In particular event-time based aggregations.
If one of the sources is idle, event-time may not progress and you may see no result emitted, regardless other sources are receiving data.

To ensure data is regularly emitted, you may employ the so called *Heartbeat pattern*. 
Practically, an additional source of data which regularly emits dummy events. These dummy events are disregarded in the calculations but allow the time to progress.

See [Heartbeat pattern](https://github.com/charmon79/cc-flink-demos/blob/main/patterns/heartbeat-pattern.md) for an explanation and example of this pattern.
