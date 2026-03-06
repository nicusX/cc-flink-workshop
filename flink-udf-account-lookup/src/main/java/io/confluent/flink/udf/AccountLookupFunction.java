package io.confluent.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink SQL UDF that returns account details for a given account number.
 *
 * <p>In production this function would query a real database or internal API. In the workshop demo
 * it returns deterministic synthetic data so the agent always has something meaningful to work
 * with, regardless of which account number arrives on the stream.
 */
public class AccountLookupFunction extends ScalarFunction {

    /**
     * Returns a JSON string with account details for the given account number.
     *
     * @param accountNumber the account identifier, e.g. "ACC1000001"
     * @return JSON with balance, status, last_transaction_amount, last_transaction_date, alert_flag
     */
    public String eval(String accountNumber) {
        if (accountNumber == null || accountNumber.isBlank()) {
            return "{\"error\": \"account_number is required\"}";
        }

        // Derive a stable numeric seed from the account suffix so the same account
        // always returns the same data within a demo session.
        int seed = extractSeed(accountNumber);

        double balance = 1000.0 + (seed * 347.83) % 9000.0;
        String status = (seed % 5 == 0) ? "FROZEN" : "ACTIVE";
        double lastTxAmount = 10.0 + (seed * 127.5) % 990.0;
        String lastTxDate = lastTransactionDate(seed);
        boolean alertFlag = seed % 7 == 0;

        return String.format(
                "{"
                        + "\"account_number\": \"%s\", "
                        + "\"balance\": %.2f, "
                        + "\"status\": \"%s\", "
                        + "\"last_transaction_amount\": %.2f, "
                        + "\"last_transaction_date\": \"%s\", "
                        + "\"alert_flag\": %b"
                        + "}",
                accountNumber, balance, status, lastTxAmount, lastTxDate, alertFlag);
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private int extractSeed(String accountNumber) {
        // Parse the numeric suffix (e.g. "ACC1000003" -> 1000003)
        String digits = accountNumber.replaceAll("[^0-9]", "");
        if (digits.isEmpty()) {
            return accountNumber.hashCode() & 0x7FFFFFFF;
        }
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            return accountNumber.hashCode() & 0x7FFFFFFF;
        }
    }

    private String lastTransactionDate(int seed) {
        // Produce a plausible recent date string without external dependencies.
        int day = 1 + (seed % 28);
        int month = 1 + (seed % 12);
        return String.format("2025-%02d-%02d", month, day);
    }
}
