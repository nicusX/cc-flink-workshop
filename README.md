# Flink SQL Workshop: Advanced Stream Processing with Confluent Cloud

Welcome to the hands-on portion of our Flink SQL workshop. This repository contains all the lab exercises and SQL scripts required to master streaming analytics.

---

## 🛠 Prerequisites
* Access to a Confluent Cloud Flink SQL environment.
* Basic understanding of SQL syntax.
* A MongoDB account, a free MongoDB cluster with sample data (required for [Lab 2, Lookup joins](./lab2.md#5---lookup-joins) only).
  See [MongoDB setup](mongodb-setup.md) instructions.

---

## 📚 Workshop Labs

Navigate through the following labs to complete the practical exercises:

| Lab # | Topic | Description |
| :--- | :--- | :--- |
| **[Lab 1: Foundations](lab1.md)** | **Executing Flink SQL statements** | Executing Flink SQL statements. Interpreting `EXPLAIN` plans.|
| **[Lab 2: Joins](lab2.md)** | **Join Operators** | Implementing Temporal Table Joins, Interval Joins, Regular Joins with TTL, and Lookup Joins to external databases. |
| **[Lab 3: Aggregations & Windows](lab3.md)** | **Aggregations and Windowing** | Using GROUP BY, OVER windows, Tumble windows, and Session windows. |
| **[Lab 4: Hints & Views](lab4.md)** | **SQL Hints, Views, and Table Options** | Using `HINTS` for statement improvement, creating `VIEW`s, Common Table Expressions (CTE), and setting table options. |
| **[Lab 5: Changelogs and Operators](lab5.md)** | **Changelog Modes & State-intensive Operators** | Understanding changelog modes, operators and their state impact. |
| **[Lab 6: Statement Operations](lab6.md)** | **Error Handling & Statement Operations** | Custom error handling with DLQ, Carry-over Offset for statement replacement. |
