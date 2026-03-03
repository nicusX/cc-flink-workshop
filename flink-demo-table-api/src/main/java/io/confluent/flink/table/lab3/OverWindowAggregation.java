package io.confluent.flink.table.lab3;

// Original SQL statement from Lab 3 - Section 2
        /*
        SELECT
            `account_number`,
            transaction_type,
            amount,
            SUM(amount) OVER w as total_value,
            CASE WHEN SUM(amount) OVER w > 500 THEN 'YES' ELSE 'NO' END AS FLAG
        FROM `transactions_faker`
        WHERE transaction_type = 'withdrawal'
        WINDOW w AS (
            PARTITION BY account_number,transaction_type
            ORDER BY `timestamp` ASC
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
        */

import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.plugin.ConfluentTools;
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

public class OverWindowAggregation {

    public static void main(String[] args) throws Exception {

        // Use ConfluentSettings as the entrypoint for configuration
        ConfluentSettings.Builder settings = ConfluentSettings.newBuilderFromResource("/cloud.properties");
        settings.setOption("sql.local-time-zone", "UTC");
        settings.setContextName("table-api-demo");

        // 1. Set up the Table Environment and point to your Kafka cluster
        TableEnvironment env = TableEnvironment.create(settings.build());
        env.useCatalog("");
        env.useDatabase("");

        // 2. Reference the table
        Table transactions = env.from("transactions_faker");

        // 3. Build the query using Table API
        Table result = transactions
                // Equivalent to: WHERE transaction_type = 'withdrawal'
                .filter($("transaction_type").isEqual("withdrawal"))

                // Define the window specification for OVER clause
                // PARTITION BY account_number, transaction_type
                // ORDER BY timestamp ASC
                // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                .window(
                        Over.partitionBy($("account_number"), $("transaction_type"))
                            .orderBy($("timestamp"))
                            .preceding(UNBOUNDED_ROW)
                            .following(CURRENT_ROW)
                            .as("w")
                )

                // Equivalent to: SELECT account_number, transaction_type, amount,
                //                SUM(amount) OVER w as total_value,
                //                CASE WHEN SUM(amount) OVER w > 500 THEN 'YES' ELSE 'NO' END AS FLAG
                .select(
                        $("account_number"),
                        $("transaction_type"),
                        $("amount"),
                        $("amount").sum().over($("w")).as("total_value"),
                        ifThenElse($("amount").sum().over($("w")).isGreater(500), lit("YES"), lit("NO"))
                            .as("FLAG")
                );

        // 4. Print or execute the result
        System.out.println("Print a capped changelog...");
        ConfluentTools.printChangelog(result, 20);
    }
}
