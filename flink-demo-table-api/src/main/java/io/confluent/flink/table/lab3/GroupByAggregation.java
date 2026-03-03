package io.confluent.flink.table.lab3;

// Original SQL statement from Lab 3
        /*
        SELECT
            account_number,
            transaction_type,
            SUM(amount)
        FROM
            `transactions_faker`
        WHERE
            transaction_type = 'withdrawal'
        GROUP BY
            account_number,transaction_type
        HAVING SUM(amount) > 500
        */

import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.plugin.ConfluentTools;
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.$;

public class GroupByAggregation {

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

                // Equivalent to: GROUP BY account_number, transaction_type
                .groupBy($("account_number"), $("transaction_type"))

                // Equivalent to: SELECT account_number, transaction_type, SUM(amount) AS total_amount
                .select(
                        $("account_number"),
                        $("transaction_type"),
                        $("amount").sum().as("total_amount")
                )

                // Equivalent to: HAVING SUM(amount) > 500
                .filter($("total_amount").isGreater(500));

        // 4. Print or execute the result
        System.out.println("Print a capped changelog...");
        ConfluentTools.printChangelog(result, 20);

        System.out.println("Print a table of the capped and applied changelog...");
        ConfluentTools.printMaterialized(result, 20);
    }
}
