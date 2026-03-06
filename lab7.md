# Lab 7: Table API Examples in Java

This lab provides Java implementations of the Flink Table API examples from the previous labs. These examples demonstrate how to build data pipelines using the Table API instead of SQL, covering aggregations, windowing, and end-to-end data pipeline patterns.

## Prerequisites

Before running these examples, ensure you have:
- The `transactions_faker` and `customers_faker` tables created in [Lab 1](./lab1.md)
- A valid `cloud.properties` file in `flink-demo-table-api/src/main/resources/` with your Confluent Cloud credentials
- Maven installed to build the project
- Java 11 or later. Java 17 preferred https://docs.confluent.io/cloud/current/flink/reference/table-api.html

## Table API Examples

All examples are located in:
```
flink-demo-table-api/src/main/java/io/confluent/flink/table/lab3/
```

For a complete reference of available Table API functions and operators, see the [Confluent Cloud Flink Table API Functions documentation](https://docs.confluent.io/cloud/current/flink/reference/functions/table-api-functions.html).

### 1 - GROUP BY Aggregation (Table API)

**File:** [`GroupByAggregation.java`](./flink-demo-table-api/src/main/java/io/confluent/flink/table/lab3/GroupByAggregation.java)

Implements the GROUP BY aggregation using the Table API fluent interface.

Demonstrates:
- Filtering data with `.filter()`
- Grouping with `.groupBy()`
- Aggregating with `.select()` and `.sum()`
- HAVING clause with post-aggregation filtering

```java
Table result = transactions
    .filter($("transaction_type").isEqual("withdrawal"))
    .groupBy($("account_number"), $("transaction_type"))
    .select(
        $("account_number"),
        $("transaction_type"),
        $("amount").sum().as("total_amount")
    )
    .filter($("total_amount").isGreater(500));
```

### 2 - GROUP BY Aggregation (SQL)

**File:** [`GroupByAggregationSQL.java`](./flink-demo-table-api/src/main/java/io/confluent/flink/table/lab3/GroupByAggregationSQL.java)

Same aggregation as example 1, but using SQL with `env.executeSql()`.

Demonstrates:
- Executing SQL queries directly via the Table API
- Using `TableResult` to handle query results
- `ConfluentTools.printChangelog()` for result visualization

Useful for scenarios where SQL is more readable or as a bridge from SQL to Table API.

### 3 - GROUP BY Aggregation (Complete Pipeline)

**File:** [`GroupByAggregationPipeline.java`](./flink-demo-table-api/src/main/java/io/confluent/flink/table/lab3/GroupByAggregationPipeline.java)

A complete end-to-end data pipeline with the following components:

Demonstrates:
- Creating an output table with explicit schema definition
- Defining a primary key to create an upsert table (required for update-producing aggregations)
- Building a `TablePipeline` from source to sink
- Executing the pipeline with `.insertInto()`
- Collecting and displaying results

Key concepts:
- Output tables for aggregations must be **upsert tables** (with primary key) to accommodate update changes
- Uses `ConfluentTableDescriptor.forManaged()` to create managed Kafka topics
- Demonstrates the full lifecycle: create table → build pipeline → execute → read results

```java
env.createTable(OUTPUT_TABLE,
    ConfluentTableDescriptor.forManaged()
        .schema(Schema.newBuilder()
            .column("account_number", DataTypes.STRING().notNull())
            .column("transaction_type", DataTypes.STRING().notNull())
            .column("total_amount", DataTypes.DECIMAL(10, 2).notNull())
            .primaryKey("account_number", "transaction_type")
            .build())
        .build());
```

### 4 - OVER Window Aggregation (Running Totals)

**File:** [`OverWindowAggregation.java`](./flink-demo-table-api/src/main/java/io/confluent/flink/table/lab3/OverWindowAggregation.java)

Implements the OVER window aggregation for running totals.

Demonstrates:
- Defining window specifications with `.window()`
- Partitioning and ordering within windows
- Using time-based bounds with `preceding()` (RANGE PRECEDING)
- Aggregating over windows with `.over()`
- Conditional expressions with `ifThenElse()`

Key features:
- Emits a result for **every incoming record** (not batched)
- Uses a **time-based (range) window** of 1 hour looking backward
- State impact is **moderate** due to the 1-hour time window
- Automatically manages state expiration based on watermarks
- Confluent Cloud supports RANGE PRECEDING only (RANGE FOLLOWING not yet supported)

```java
.window(Over.partitionBy($("account_number"), $("transaction_type"))
    .orderBy($("timestamp"))
    .preceding(lit(1).hours())
    .as("w"))
.select(
    $("amount").sum().over($("w")).as("total_value"),
    ifThenElse($("amount").sum().over($("w")).isGreater(500),
        lit("YES"), lit("NO")).as("FLAG")
)
```

### 5 - TUMBLE Window Aggregation

**File:** [`GroupByTumbleWindow.java`](./flink-demo-table-api/src/main/java/io/confluent/flink/table/lab3/GroupByTumbleWindow.java)

Implements GROUP BY aggregation over tumbling time windows.

Demonstrates:
- Creating tumbling windows with `.window(Tumble.over())`
- Accessing window boundaries with `$("w").start()` and `$("w").end()`
- Counting records within windows

Key features:
- Fixed-size time windows that do not overlap
- Results are **emitted once per window** (when the window closes)
- First results appear after 1 minute (first window end)
- Subsequent results appear at regular intervals (every 1 minute)

```java
.window(Tumble.over(lit(1).minutes())
    .on($("timestamp"))
    .as("w"))
.groupBy($("w"), $("merchant"))
.select(
    $("w").start().as("window_start"),
    $("w").end().as("window_end"),
    $("merchant"),
    $("merchant").count().as("total_tx_failed")
)
```

## Building and Running

### Build the project:
```bash
cd flink-demo-table-api
mvn clean package
```

### Run an example:
```bash
java -cp target/classes:target/dependency/* io.confluent.flink.table.lab3.GroupByAggregation
```

Or run from your IDE by right-clicking the class and selecting "Run".

## Credentials

The examples use `cloud.properties` for Confluent Cloud connection configuration. See `cloud.properties.example` for the required fields. Never commit `cloud.properties` with real credentials to version control.

---

Next: [Lab 8: User Defined Functions](lab8.md)

