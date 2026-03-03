package io.confluent.flink.table.lab3;

// Original SQL statement from Lab 3 - Section 3
        /*
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
        */

import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.plugin.ConfluentTools;
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

public class GroupByTumbleWindow {

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
                // Equivalent to: WHERE transaction_type ='payment' AND status = 'Failed'
                .filter($("transaction_type").isEqual("payment")
                        .and($("status").isEqual("Failed")))

                // Equivalent to: TUMBLE(TABLE `transactions_faker`, DESCRIPTOR (`timestamp`), INTERVAL '1' MINUTE)
                .window(Tumble.over(lit(1).minutes())
                        .on($("timestamp"))
                        .as("w"))

                // Equivalent to: GROUP BY window_start, window_end, merchant
                .groupBy($("w"), $("merchant"))

                // Equivalent to: SELECT window_start, window_end, merchant, COUNT(*) AS total_tx_failed
                .select(
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("merchant"),
                        $("merchant").count().as("total_tx_failed")
                );

        // 4. Print or execute the result
        System.out.println("Print a capped changelog...");
        ConfluentTools.printChangelog(result, 20);
    }
}
