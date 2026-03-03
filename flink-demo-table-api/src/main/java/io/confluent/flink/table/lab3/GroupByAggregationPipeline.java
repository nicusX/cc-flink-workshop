package io.confluent.flink.table.lab3;

// Original SQL statement from Lab 3
        /*
        SELECT
          account_number,
          transaction_type,
          SUM(amount)
        FROM `transactions_faker`
        WHERE transaction_type = 'withdrawal'
        GROUP BY account_number,transaction_type
        HAVING SUM(amount) > 500
        */

import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.plugin.ConfluentTableDescriptor;
import io.confluent.flink.plugin.ConfluentTools;
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

import java.util.concurrent.ExecutionException;

/**
 * A complete data pipeline example that demonstrates:
 * 1. Creating an output table with a defined schema
 * 2. Building a data pipeline from source to sink
 * 3. Collecting and printing results
 */
public class GroupByAggregationPipeline {

    // Fill this with an environment you have write access to
    static final String TARGET_CATALOG = "";
    static final String TARGET_DATABASE = "";

    // Output table name
    static final String OUTPUT_TABLE = "withdrawal_aggregations";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Use ConfluentSettings as the entrypoint for configuration
        ConfluentSettings.Builder settings = ConfluentSettings.newBuilderFromResource("/cloud.properties");
        settings.setOption("sql.local-time-zone", "UTC");
        settings.setContextName("table-api-demo");

        // 1. Set up the Table Environment and point to your Kafka cluster
        TableEnvironment env = TableEnvironment.create(settings.build());
        env.useCatalog(TARGET_CATALOG);
        env.useDatabase(TARGET_DATABASE);

        System.out.println("Creating output table: " + OUTPUT_TABLE);

        // 2. Create the output table with defined schema
        // Use an upsert table to accommodate update changes from GROUP BY aggregation
        env.createTable(
                OUTPUT_TABLE,
                ConfluentTableDescriptor.forManaged()
                        .schema(
                                Schema.newBuilder()
                                        .column("account_number", DataTypes.STRING().notNull())
                                        .column("transaction_type", DataTypes.STRING().notNull())
                                        .column("total_amount", DataTypes.DECIMAL(10, 2).notNull())
                                        .primaryKey("account_number", "transaction_type")
                                        .build())
                        .build());

        System.out.println("Table created successfully!");

        // 3. Build the aggregation query
        Table transactions = env.from("transactions_faker");

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

        System.out.println("Executing table pipeline...");

        // 4. Create and execute the pipeline
        TablePipeline pipeline = result.insertInto(OUTPUT_TABLE);

        // Execute the pipeline asynchronously (suitable for unbounded streams)
        pipeline.execute();

        System.out.println("Pipeline executed. Reading results from " + OUTPUT_TABLE + "...");

        // 5. Read and display the results
        Table outputTable = env.from(OUTPUT_TABLE);

        System.out.println("Print a capped changelog...");
        ConfluentTools.printChangelog(outputTable, 20);
    }
}
