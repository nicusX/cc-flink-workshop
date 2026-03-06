# Lab 8: Pattern Matching with MATCH_RECOGNIZE

In this lab we will learn how to detect complex event patterns in streaming data using the `MATCH_RECOGNIZE` clause.

`MATCH_RECOGNIZE` is a powerful SQL extension that allows you to detect sequences of events in a stream — for example, fraud patterns, behavioral anomalies, or multi-step workflows spanning multiple records.

### Prerequisites

In this lab we will be using the transactions fake data created in [Lab 1](./lab1.md).
If you destroyed those tables, go back to [Lab 1](./lab1.md) and recreate `transactions_faker`.

---

### MATCH_RECOGNIZE Syntax

A `MATCH_RECOGNIZE` clause has the following structure:

```sql
SELECT *
FROM source_table
MATCH_RECOGNIZE (
  PARTITION BY <column>           -- Detect patterns independently per partition
  ORDER BY <time column>          -- Must be ordered by event time
  MEASURES                        -- What columns to include in the output
    <expression> AS <alias>, ...
  ONE ROW PER MATCH               -- Emit one summary row per match (default)
  AFTER MATCH SKIP ...            -- Where to start looking for the next match
  PATTERN (<pattern>)             -- The pattern to detect (like a regex)
  WITHIN INTERVAL '...'          -- Optional: time bound for the full pattern
  DEFINE                          -- What each pattern variable means
    <variable> AS <condition>, ...
)
```

Key concepts:

- **Pattern variables** (e.g., `A`, `FAILED`, `SUCCESS`): Labels for event types in the pattern
- **PATTERN**: A regular expression over variables — e.g., `A B+ C` means "A, then one or more B, then C"
- **DEFINE**: The condition each variable must satisfy
- **MEASURES**: What columns to expose in the output row for each match
- **AFTER MATCH SKIP**: Controls where to resume pattern matching after a match is found
- **WITHIN**: Bounds the time window within which the full pattern must occur

---

### 0 - Create Source Views

We pre-filter `transactions_faker` into two views that we will reuse across all exercises.

Payment transactions only:

```sql
CREATE VIEW payment_transactions AS
SELECT * FROM transactions_faker
WHERE transaction_type = 'payment';
```

Withdrawal transactions only:

```sql
CREATE VIEW withdrawal_transactions AS
SELECT * FROM transactions_faker
WHERE transaction_type = 'withdrawal';
```

---

### 1 - Detect Consecutive Failed Transactions

Detect accounts with **3 or more consecutive failed payment transactions**.

This could indicate a card being tested by a fraudster (brute-force PIN or CVV attempts).

```sql
SELECT *
FROM payment_transactions
MATCH_RECOGNIZE (
  PARTITION BY account_number
  ORDER BY `timestamp`
  MEASURES
    FIRST(F.`timestamp`)     AS first_failed_time,
    LAST(F.`timestamp`)      AS last_failed_time,
    COUNT(F.txn_id)          AS failed_count
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (F{3,}?)
  WITHIN INTERVAL '1' MINUTE
  DEFINE
    F AS F.status = 'Failed'
)
```

**What this does:**
- `PARTITION BY account_number`: Patterns are detected independently for each account
- `PATTERN (F{3,}?)`: Matches 3 or more consecutive events satisfying variable `F`
- `DEFINE F AS F.status = 'Failed'`: An event qualifies as `F` if it is a failed transaction
- `WITHIN INTERVAL '1' MINUTE`: The entire pattern must occur within 1 minute
- `AFTER MATCH SKIP PAST LAST ROW`: After a match, skip past the last matched event to avoid overlapping detections

> ℹ The `?` after the quantifier makes it **reluctant** (non-greedy). A reluctant quantifier fires as soon as the minimum count is satisfied (3 failures here), rather than waiting for a non-matching event to terminate the sequence. Flink requires the last element of a pattern to be either a simple variable or a reluctant quantifier.

---

### 2 - Failed Attempts Followed by a Successful Transaction

A common fraud pattern: multiple failed attempts (wrong PIN or CVV) immediately followed by a successful transaction — possibly after the fraudster obtained the correct credentials.

```sql
SELECT *
FROM payment_transactions
MATCH_RECOGNIZE (
  PARTITION BY account_number
  ORDER BY `timestamp`
  MEASURES
    COUNT(FAILED.txn_id)           AS failed_attempts,
    FIRST(FAILED.`timestamp`)      AS first_attempt_time,
    SUCCESS.`timestamp`            AS success_time,
    SUCCESS.amount                 AS amount,
    SUCCESS.merchant               AS merchant
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (FAILED{2,} SUCCESS)
  WITHIN INTERVAL '1' MINUTE
  DEFINE
    FAILED  AS FAILED.status  = 'Failed',
    SUCCESS AS SUCCESS.status = 'Successful'
)
```

**What this does:**
- `PATTERN (FAILED{2,} SUCCESS)`: Two or more consecutive failed events, immediately followed by a successful one
- Two named pattern variables (`FAILED`, `SUCCESS`) each with their own `DEFINE` condition
- `MEASURES` extracts the count of failed attempts, their time range, and details of the successful transaction

> ℹ Flink evaluates patterns greedily by default: `FAILED` will match as many events as possible before trying `SUCCESS`.

---

### 3 - Escalating Withdrawal Amounts

Detect **three consecutive withdrawals where each amount is larger than the previous** — a pattern that may indicate progressive account draining or limit-testing behavior.

In `DEFINE`, you can reference *other pattern variables* to compare values across events in the sequence:

```sql
SELECT *
FROM withdrawal_transactions
MATCH_RECOGNIZE (
  PARTITION BY account_number
  ORDER BY `timestamp`
  MEASURES
    A.amount         AS first_amount,
    B.amount         AS second_amount,
    C.amount         AS third_amount,
    A.`timestamp`    AS start_time,
    C.`timestamp`    AS end_time
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A B C)
  WITHIN INTERVAL '2' MINUTES
  DEFINE
    A AS TRUE,
    B AS B.amount > A.amount,
    C AS C.amount > B.amount
)
```

**What this does:**
- `PATTERN (A B C)`: Three consecutive events, each assigned a different variable
- `DEFINE B AS B.amount > A.amount`: B must have a higher amount than A
- `DEFINE C AS C.amount > B.amount`: C must have a higher amount than B
- `AFTER MATCH SKIP TO NEXT ROW`: Start looking for the next match from the second row of the previous match, allowing overlapping detections (e.g., rows 1-2-3 and rows 2-3-4 can both match)

---

Next: [Lab 9: AI Model Inference and Streaming Agents](lab9.md)
