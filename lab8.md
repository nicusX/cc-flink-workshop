# Lab 8: User Defined Functions (UDF)

In this lab we demonstrate how to implement user defined functions in Java and use them in your SQL code.

Flink supports two types of stateless user defined functions:
* **Scalar User Defined Functions** (UDF) - expect scalar parameters and return a single scalar value
* **User Defined Table Functions** (UDTF) - expect scalar parameters and return zero or more values, often representing an entire row.

> ℹ️ In this lab we do not cover the new stateful *Process Table Functions* (PTF), at the time of writing (3/2026) not yet publicly available.

### Tools Prerequisites

To build the Java UDF you need:
* JDK 17+
* Maven

### Test Data

This lab uses the test data from the `marketplace` catalog in the `examples` environment available in any Confluent Cloud Flink organization. 
Ensure you have access to it.

---

### Build and Upload the Artifact (JAR)

Before using the UDFs, we need to compile and package them in a JAR, and upload the artifact in Confluent Cloud.

#### 1. Build

Switch to the `./flink-udf-examples` subfolder and build the Maven project:

```shell
cd flink-udf-examples
mvn clean package
```

This creates `./flink-udf-examples/target/udf-examples-1.0.jar`.

#### 2. Upload

Upload the JAR (execute from the project folder):

```shell
confluent flink artifact create \
  --environment <env-id> \
  --cloud-provider <gcp|aws|azure> \
  --region <region> \
  --artifact-name udf-examples \
  --artifact-file flink-udf-examples/target/udf-examples-1.0.jar
```

The CLI returns an Artifact ID (`cfa-xxxxxxx`).

Take note of this Artifact ID. 
You will need it later to register the functions.

> ℹ️ You can see the uploaded artifacts from the UI at *Environments > (your environment) > Artifacts*

We are now ready to test our user defined functions in SQL.

---
---

## Test the User Defined Functions


> ⚠️ Make sure in your SQL Workspace you select Catalog and Database corresponding to your environment and cluster.
> Do not select `examples` and `marketplace`.


### 1. Scalar User Defined Functions (UDFs)

#### 1.1 Simple Scalar Function: Concatenate Multiple Strings with a Separator

UDF source code:
[ConcatWithSeparator](./flink-udf-examples/src/main/java/io/confluent/flink/examples/udf/scalar/ConcatWithSeparator.java)


Simple scalar function concatenating two or more string parameters with a specified separator.
This example shows how you can overload the `eval()` method having different versions with different parameters. As you can see, 3 versions of `eval()` accepting 2, 3, and 4 strings (plus the separator).



Register the Function, replacing `<artifact-id>` with your Artifact ID:
```sql
CREATE FUNCTION `concat_with_separator`
    AS 'io.confluent.flink.examples.udf.scalar.ConcatWithSeparator'
  USING JAR 'confluent-artifact://<artifact-id>'
```

Test the UDF, concatenating different numbers of strings with a separator.


```sql
SELECT
  concat_with_separator(`name`, `brand`, ' - ') AS long_name,
  concat_with_separator(`name`, `brand`, `vendor`, ' : ') AS extended_name,
  concat_with_separator(`name`, `brand`, `vendor`, `department`, ', ') AS extra_long_name
FROM `examples`.`marketplace`.`products`
```

Note: if you try to invoke the UDF with parameters not matching any of the `eval()` implementations, you get an error. For example:

```sql
SELECT
  concat_with_separator(`name`, ', ') AS simple_name
FROM `examples`.`marketplace`.`products`
```

Causes an error similar to the following:
```
SQL validation failed. Error from line 2, column 3 to line 2, column 37.

Caused by: Function 'concat_with_separator(<CHARACTER>, <CHARACTER>)' does not exist or you do not have permission to access it.
Using current catalog 'my-catalog' and current database 'my-database'.
Supported signatures are:
concat_with_separator(STRING, STRING, STRING)
concat_with_separator(STRING, STRING, STRING, STRING, STRING)
concat_with_separator(STRING, STRING, STRING, STRING)
```

Describe the function to see the supported signatures:

```sql
DESCRIBE FUNCTION EXTENDED `concat_with_separator`
```

As you can see, this shows the argument names, types, and return type.

When a function has multiple overloaded `eval()` methods, like in this case, you can see all the supported versions.

---

#### 1.2 Custom Logging

UDF source code:
[LogOutOfRange](./flink-udf-examples/src/main/java/io/confluent/flink/examples/udf/scalar/LogOutOfRange.java)

This example demonstrates how you can use custom logging in a user defined function.

In this case, the function is passthrough and simply returns the first parameter passed, but logs a message at `WARN` level when certain conditions are met.

This technique is useful for debugging.
Logging only when certain conditions are met, as opposed to "flooding" the logs, logging at every message, simplifies reading the logs.
It also avoids missing important entries due to the logging throttling at 1000 entries/sec.


Register the Function. Replace `<artifact-id>` with your Artifact ID:
```sql
CREATE FUNCTION `log_out_of_range`
    AS 'io.confluent.flink.examples.udf.scalar.LogOutOfRange'
    USING JAR 'confluent-artifact://<artifact-id>'
```

Test the function, logging when prices < 20 or > 90 are encountered:
```sql
SELECT
    order_id,
    log_out_of_range(price, 20, 90) AS _price
FROM  `examples`.`marketplace`.`orders`
```


To see the logs, go to *Environments > (your environment) > Flink > Statements > (select your statement) > Logs*.

You can filter log entries by Source (`Function`) and Log level (`WARN`).

---

#### 1.3 Non-deterministic functions

UDF source code:
[RandomString](./flink-udf-examples/src/main/java/io/confluent/flink/examples/udf/scalar/RandomString.java)

This example shows how to implement a non-deterministic UDF, a function whose return value does not only depend deterministically 
on the input parameters. If not implemented correctly, when a non-deterministic function is invoked with only constant parameters 
(or no parameter), Flink may decide to "optimize" it and run the function only once, during the pre-flight phase. 
The same output will be reused for all subsequent invocations.
This is normally not what you want to achieve. For example, if you implement a UDF to generate a random ID for every record, you do not want to have the same ID, generated once, reused for all records.

Check out the code of [RandomString](./flink-udf-examples/src/main/java/io/confluent/flink/examples/udf/scalar/RandomString.java) to see how you tell Flink
that your UDF is non-deterministic.

Register the function:

```sql
CREATE FUNCTION `random_string`
    AS 'io.confluent.flink.examples.udf.scalar.RandomString'
    USING JAR 'confluent-artifact://<artifact-id>'
```

Use the function in a query
```sql
SELECT
    random_string(10) AS my_random,
    order_id
FROM  `examples`.`marketplace`.`orders`
```

---
---

### 2. User Defined Table Functions (UDTFs)

#### 2.1 Parse a STRING containing JSON to extract specific fields

User Defined Table Function (UDTF) source code:
[JsonAddressToRow](./flink-udf-examples/src/main/java/io/confluent/flink/examples/udf/table/JsonAddressToRow.java)


This User Defined Table Function (UDTF) unnests a STRING field containing a JSON representation of the address in this form:

```json
{
  "street" : "91839 Satterfield Wall",
  "postcode": "05420",
  "city" : "Wunschtown"
}
```

...returning a `ROW` containing three separate fields `street`, `postcode`, and `city`.

The function also demonstrates a simple error handling when a malformed JSON is encountered.
The behavior depends on the second parameter (`failOnError`), passed to the function:
- `failOnError` = `TRUE`: the UDTF fails when a malformed JSON is encountered.
- `failOnError` = `FALSE`: the UDTF logs the parsing failure, and returns nothing.

Note that, if a valid JSON is encountered, but the `street`, `postcode`, or `city` fields are not present, this is not handled
as a parsing failure but `NULL` is returned for the missing fields.

##### Register the function

Register the function:

```sql
CREATE FUNCTION `unnest_json_address`
  AS 'io.confluent.flink.examples.udf.table.JsonAddressToRow'
  USING JAR 'confluent-artifact://<artifact-id>'
```

Describe the function:

```sql
DESCRIBE FUNCTION EXTENDED `unnest_json_address`
```

Note that it returns a `ROW` containing the three extracted fields.

##### Prepare the test data

Before testing the function, we need to prepare the test data.

We create a new table `customers_json` where we send records from `examples.marketplace.customers` generating a field `full_address` with the JSON representation of the address:

```sql
CREATE TABLE customers_json (
  PRIMARY KEY(`customer_id`) NOT ENFORCED
)
WITH (
  'value.format' = 'json-registry',
  'scan.startup.mode' = 'latest-offset'
)
AS SELECT
  customer_id,
  `name`,
  CONCAT('{ "street" : "', address, '", "postcode" : "', postcode, '", "city" : "', city, '"}') AS full_address,
  email
FROM `examples`.`marketplace`.`customers`
```

##### Testing the function

Keep the previous statement running, and sending data to `customers_json`.

Write the following query as separate SQL statement:

```sql
SELECT
    `customer_id`,
    `name`,
    `email`,
    -- the fields below are returned by the UDTF
    `street`,
    `postcode`,
    `city`
FROM customers_json
LEFT JOIN LATERAL TABLE(unnest_json_address(`full_address`, TRUE)) ON TRUE
```

##### Test error handling

Note that we are passing `TRUE` as second parameter to `unnest_json_address` (`failOnError`).
This means the function will fail if a malformed JSON is encountered in the `full_address` field.

Let's test it.
Using a new SQL statement, let's inject an invalid record **while the previous query is running**:

```sql
INSERT INTO customers_json VALUES (1234, 'my name', '{ this-is-malformed }', 'some@email.com')
```

When the malformed record reaches the query (it may take a few seconds), the query will fail, with an exception like:
*"UDF invocation error: exception raised in the user function code"*.

Stop the query, and re-run it changing `failOnError` to `FALSE`. 
In this case, when a malformed JSON is encountered, the problem is logged and the statement proceeds to the next record without returning any ROW. 

```sql
SELECT
    `customer_id`,
    `name`,
    `email`,
    -- fields returned by the UDTF
    `street`,
    `postcode`,
    `city`
FROM customers_json
LEFT JOIN LATERAL TABLE(unnest_json_address(`full_address`, FALSE)) ON TRUE
```

Let's inject another malformed record:
```sql
INSERT INTO customers_json VALUES (4321, 'my name', '{ this-is-malformed }', 'some@email.com')
```

To see the record being emitted, use the UI to filter the output by `customer_id` = `4321`.


> ℹ️ Because we are using `LEFT JOIN LATERAL TABLE...` the input record is not lost, but `street`, `postcode`, and `city` are null.
> Using `CROSS JOIN LATERAL TABLE` instead of `LEFT JOIN LATERAL TABLE`, no record would have been emitted
> for the malformed record.

Go to the logging for the running statement: under the running query, click on the *Statement Name*. A pane opens on the right. Select *Logs*. You can also click *view all* and filter by *Source* selecting *Function* and *Log level* = *Warn*


#### 2.2 Parse a STRING containing JSON to normalize nested elements

User Defined Table Function (UDTF) source code:
[NormalizeJsonArray](./flink-udf-examples/src/main/java/io/confluent/flink/examples/udf/table/NormalizeJsonArray.java)


This UDTF demonstrates how you can normalize a JSON payload emitting multiple records for each nested element.

In this example, the UDTF expands a field containing a simple JSON array, which is roughly what the Flink system function `UNNEST`
does. The implementation can be easily expanded with a more complex logic, for example looking for specific elements or fields to extract.

##### Register the function

Register the function:

```sql
CREATE FUNCTION `normalize_json_emails`
  AS 'io.confluent.flink.examples.udf.table.NormalizeJsonArray'
  USING JAR 'confluent-artifact://<artifact-id>'
```

##### Prepare the test data

To test this function, we first create a *faker* table to generate customers with multiple emails
(note that faker only generates fixed-size arrays, so we also generate the number of emails to retain):
```sql
CREATE TABLE `customer_emails` (
  `customer_id` INT NOT NULL,
  `name` VARCHAR(2147483647) NOT NULL,
  `emails` ARRAY<STRING>,
  `desired_email_count` INT,
  PRIMARY KEY (`customer_id`) NOT ENFORCED
)
WITH (
  'connector' = 'faker',
  'rows-per-second' = '10',
  'fields.customer_id.expression' = '#{Number.numberBetween ''3000'',''3250''}',
  'fields.name.expression' = '#{Name.fullName}',
  'fields.emails.expression' = '#{Internet.emailAddress}',
  'fields.emails.length' = '3',
  'fields.desired_email_count.expression' = '#{Number.numberBetween ''0'',''3''}'
);
```

Then create a table where we send a single `emails` field containing the email addresses as a JSON array.
A variable number of emails is retained, between zero and three:
```sql
CREATE TABLE customer_emails_json (
    PRIMARY KEY (`customer_id`) NOT ENFORCED
)
AS SELECT
    customer_id,
    name,
    CASE
        WHEN desired_email_count = 0 THEN '[]'
        ELSE JSON_QUERY(CAST(JSON_ARRAY(ARRAY_SLICE(emails, 1, desired_email_count)) AS STRING),'$[0]')
        END AS emails
FROM customer_emails;
```

> ℹ️ The two-step process, faker + CTAS, is required to generate records with a variable number of emails. 
> Faker can generate random arrays but only with fixed number of entries.

##### Test the function, normalizing customers + email

```sql
SELECT
    customer_id,
    name,
    -- fields returned by the UDTF
    email_index,
    email
FROM customer_emails_json
LEFT JOIN LATERAL TABLE(normalize_json_emails(`emails`, TRUE)) ON TRUE
```

This query generates one row for each customer's email.
If a customer has no email, a single record is emitted with `NULL` values for `email_index` and `email`.

> ℹ️ the UDF also implements simple error handling, similar to the previous example.
> Feel free to change the second parameter to `FALSE` and send malformed JSON.
