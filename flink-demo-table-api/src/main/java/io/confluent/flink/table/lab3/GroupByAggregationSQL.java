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
import io.confluent.flink.plugin.ConfluentTools;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.util.Properties;

public class GroupByAggregationSQL {

    public static void main(String[] args) throws Exception {

        // Use ConfluentSettings as the entrypoint for configuration
        ConfluentSettings.Builder settings = ConfluentSettings.newBuilderFromResource("/cloud.properties");
        settings.setOption("sql.local-time-zone", "UTC");
        settings.setContextName("table-api-demo");

        // Read target catalog and database from app.properties
        Properties appProps = new Properties();
        String catalog = "";
        String database = "";
        try {
            appProps.load(GroupByAggregationSQL.class.getResourceAsStream("/app.properties"));
            catalog = appProps.getProperty("target.catalog", "");
            database = appProps.getProperty("target.database", "");
        } catch (IOException e) {
            System.err.println("Warning: Could not load app.properties");
        }

        // 1. Set up the Table Environment and point to your Kafka cluster
        TableEnvironment env = TableEnvironment.create(settings.build());

        if (!catalog.isEmpty()) {
            env.useCatalog(catalog);
        }
        if (!database.isEmpty()) {
            env.useDatabase(database);
        }

        // 2. Execute SQL query
        String sql = "SELECT " +
                "  account_number," +
                "  transaction_type," +
                "  SUM(amount) AS total_amount " +
                "FROM `transactions_faker` " +
                "WHERE transaction_type = 'withdrawal' " +
                "GROUP BY account_number, transaction_type " +
                "HAVING SUM(amount) > 500";

        TableResult result = env.executeSql(sql);

        // 3. Print or execute the result
        System.out.println("Print a capped changelog...");
        ConfluentTools.printChangelog(result, 20);
    }
}
