package uk.gov.hmcts.reporting.spike;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;

/**
 * Check & Challenge reporting spike for local Spark and Microsoft Fabric.
 *
 * Each AC-labelled method represents a demonstrable validation step, not an
 * assertion that it corresponds to a particular Jira acceptance criterion.
 * The fixed October 2026 window and 300-row expectation are spike fixtures.
 */
public final class CheckChallengeReportJob {

    private static final String PERMISSIONS_TABLE = "spike_ug_schema_v2.cpp_permission";
    private static final String USERS_TABLE = "spike_ug_schema_v2.cpp_user";
    private static final String EXPECTED_TABLE = "spike_reporting_v3.check_challenge_export";
    private static final String REPORT_FROM = "2026-10-01";
    private static final String REPORT_TO_EXCLUSIVE = "2026-11-01";
    private static final long EXPECTED_GRANTS = 300L;
    private static final int EXPECTED_COLUMNS = 15;
    // Default Fabric destination. Override for local Docker using
    // -Dreport.output.path=/data/output/check_challenge_report
    // or REPORT_OUTPUT_PATH=/data/output/check_challenge_report.
    // Confirm the workspace/lakehouse IDs and job identity permissions in Fabric.
    private static final String FABRIC_OUTPUT_PATH =
            "abfss://aaf7ab08-177e-412f-aeb0-578b63fd1b22"
                    + "@onelake.dfs.fabric.microsoft.com/"
                    + "cde51d26-212c-410b-8e2f-ac4cd507c0ca"
                    + "/Files/reports/check_challenge_report";

    // Browser URL for the Lakehouse containing the exported report.
    // This opens the Lakehouse; the Files folder may need to be selected manually.
    private static final String FABRIC_WORKSPACE_ID =
            "aaf7ab08-177e-412f-aeb0-578b63fd1b22";
    private static final String FABRIC_LAKEHOUSE_ID =
            "cde51d26-212c-410b-8e2f-ac4cd507c0ca";
    private static final String FABRIC_LAKEHOUSE_URL =
            "https://app.fabric.microsoft.com/groups/"
                    + FABRIC_WORKSPACE_ID + "/lakehouses/" + FABRIC_LAKEHOUSE_ID;
    private static final String REPORT_FOLDER_IN_LAKEHOUSE =
            "Files/reports/check_challenge_report";
    private static final DateTimeFormatter REPORT_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
                    .withZone(ZoneOffset.UTC);

    private CheckChallengeReportJob() { }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CheckChallengeReportSpike")
                .getOrCreate();
        try {
            spark.conf().set("spark.sql.session.timeZone", "UTC");
            System.out.println("SPIKE2_START");

            SourceData sources = ac01ReadSourceData(spark);
            Dataset<Row> filtered = ac02FilterQualifyingGrants(sources.permissions);
            ac03ValidateUniqueUserIds(sources.users);
            Dataset<Row> joined = ac04JoinAndValidateUsers(filtered, sources.users);
            Dataset<Row> report = ac05BuildReport(joined);
            ac06ValidateReport(report);
            ac07InspectAndClassifyReasons(report);
            Dataset<Row> expected = ac08ReadExpectedReport(spark);
            ac09ReconcileAgainstReference(report, expected);
            ac10ExportReconciledCsv(report, resolveOutputPath(args));
        } finally {
            spark.stop();
        }
    }

    /** AC 01: Source tables are accessible and row counts are observable. */
    private static SourceData ac01ReadSourceData(SparkSession spark) {
        heading("AC01 - Read DPAP source tables");
        Dataset<Row> permissions = spark.table(PERMISSIONS_TABLE);
        Dataset<Row> users = spark.table(USERS_TABLE);
        System.out.println("PERMISSION_ROWS=" + permissions.count());
        System.out.println("USER_ROWS=" + users.count());
        permissions.printSchema();
        users.printSchema();
        System.out.println("SPIKE2_READ_COMPLETE");
        return new SourceData(permissions, users);
    }

    /** AC 02: October 2026 CaseAccess/View grants, with unique permission IDs. */
    private static Dataset<Row> ac02FilterQualifyingGrants(Dataset<Row> permissions) {
        heading("AC02 - Filter qualifying permissions");
        Dataset<Row> filtered = permissions
                .filter(col("object").equalTo("CaseAccess"))
                .filter(col("action").equalTo("View"))
                .filter(col("start_date").geq(lit(REPORT_FROM).cast("date")))
                .filter(col("start_date").lt(lit(REPORT_TO_EXCLUSIVE).cast("date")));
        long count = filtered.count();
        long uniqueIds = filtered.select("id").distinct().count();
        System.out.println("FILTERED_GRANTS=" + count);
        System.out.println("DISTINCT_PERMISSION_IDS=" + uniqueIds);
        require(count == EXPECTED_GRANTS && uniqueIds == EXPECTED_GRANTS,
                "Unexpected grant counts: " + count + "/" + uniqueIds);
        System.out.println("SPIKE2_FILTER_COMPLETE");
        return filtered;
    }

    /** AC 03: User IDs must be unique before the enrichment join. */
    private static void ac03ValidateUniqueUserIds(Dataset<Row> users) {
        heading("AC03 - Validate user lookup uniqueness");
        long total = users.count();
        long unique = users.select("id").distinct().count();
        System.out.println("USER_DISTINCT_IDS=" + unique);
        require(total == unique, "Duplicate user IDs detected");
        System.out.println("AC03_USER_IDS_VALIDATED");
    }

    /** AC 04: Left-join grants to users without losing or multiplying grants. */
    private static Dataset<Row> ac04JoinAndValidateUsers(
            Dataset<Row> filtered, Dataset<Row> users) {
        heading("AC04 - Enrich permissions with users");
        Dataset<Row> joined = filtered.alias("p")
                .join(users.alias("u"), col("p.source").equalTo(col("u.id")), "left");
        long sourceCount = filtered.count();
        long joinedCount = joined.count();
        long matched = joined.filter(col("u.id").isNotNull()).count();
        long unmatched = joined.filter(col("u.id").isNull()).count();
        System.out.println("JOINED_ROWS=" + joinedCount);
        System.out.println("MATCHED_USERS=" + matched);
        System.out.println("UNMATCHED_USERS=" + unmatched);
        require(joinedCount == sourceCount,
                "Join changed the number of permission rows: " + joinedCount);
        require(matched + unmatched == joinedCount,
                "Matched/unmatched counts do not reconcile");
        joined.select(
                col("p.id").alias("permission_id"),
                col("p.source").alias("user_id"),
                col("u.first_name"), col("u.last_name"),
                col("u.email"), col("u.account_status")
        ).show(10, false);
        System.out.println("SPIKE2_JOIN_COMPLETE");
        return joined;
    }

    /** AC 05: Construct the 15-column Check & Challenge report. */
    private static Dataset<Row> ac05BuildReport(Dataset<Row> joined) {
        heading("AC05 - Build Check & Challenge report");
        return joined.select(
                lit(REPORT_FROM).cast("date").alias("report_from_utc_date"),
                lit(REPORT_TO_EXCLUSIVE).cast("date")
                        .alias("report_to_utc_date_exclusive"),
                col("p.id").alias("permission_id"),
                col("p.start_date").alias("permission_start_date"),
                col("p.end_date").alias("permission_end_date"),
                col("p.source").alias("user_id"),
                col("u.first_name").alias("first_name"),
                col("u.last_name").alias("last_name"),
                col("u.email").alias("email"),
                col("u.account_status").alias("current_account_status"),
                when(col("u.id").isNotNull(), lit("MATCHED"))
                        .otherwise(lit("UNMATCHED")).alias("user_match_status"),
                col("p.target").alias("case_or_hearing_id"),
                col("p.description").alias("recorded_reason"),
                when(col("p.description").isNull()
                                .or(length(trim(col("p.description"))).equalTo(0)),
                        lit("MISSING_REASON"))
                        .otherwise(lit("REASON_PRESENT")).alias("reason_status"),
                col("p.active").alias("stored_active_flag")
        );
    }

    /** AC 06: Validate report shape, count, and permission uniqueness. */
    private static void ac06ValidateReport(Dataset<Row> report) {
        heading("AC06 - Validate report structure");
        long rows = report.count();
        long uniqueIds = report.select("permission_id").distinct().count();
        int columns = report.columns().length;
        System.out.println("REPORT_ROWS=" + rows);
        System.out.println("REPORT_DISTINCT_IDS=" + uniqueIds);
        System.out.println("REPORT_COLUMNS=" + columns);
        require(rows == EXPECTED_GRANTS && uniqueIds == EXPECTED_GRANTS
                        && columns == EXPECTED_COLUMNS,
                "Report validation failed: rows=" + rows
                        + ", distinctIds=" + uniqueIds + ", columns=" + columns);
        System.out.println("AC06_REPORT_VALIDATED");
    }

    /** AC 07: Expose report sample and classify missing reasons. */
    private static void ac07InspectAndClassifyReasons(Dataset<Row> report) {
        heading("AC07 - Inspect report and reason classifications");
        report.printSchema();
        report.show(10, false);
        long missing = report.filter(col("reason_status").equalTo("MISSING_REASON"))
                .count();
        System.out.println("MISSING_REASON_ROWS=" + missing);
        System.out.println("SPIKE2_REPORT_TRANSFORM_COMPLETE");
    }

    /** AC 08: Load independently produced reference output. */
    private static Dataset<Row> ac08ReadExpectedReport(SparkSession spark) {
        heading("AC08 - Load reference report");
        Dataset<Row> expected = spark.table(EXPECTED_TABLE);
        System.out.println("EXPECTED_ROWS=" + expected.count());
        expected.printSchema();
        return expected;
    }

    /** AC 09: Compare all 15 columns in both directions, including duplicates. */
    private static void ac09ReconcileAgainstReference(
            Dataset<Row> report, Dataset<Row> expected) {
        heading("AC09 - Reconcile against reference report");
        String[] columns = report.columns();
        Dataset<Row> actualNormalised = normaliseForComparison(report).selectExpr(columns);
        Dataset<Row> expectedNormalised = normaliseForComparison(expected).selectExpr(columns);
        Dataset<Row> unexpected = actualNormalised.exceptAll(expectedNormalised);
        Dataset<Row> missing = expectedNormalised.exceptAll(actualNormalised);
        long unexpectedCount = unexpected.count();
        long missingCount = missing.count();
        System.out.println("RECON_UNEXPECTED_ROWS=" + unexpectedCount);
        System.out.println("RECON_MISSING_ROWS=" + missingCount);
        if (unexpectedCount > 0 || missingCount > 0) {
            System.out.println("UNEXPECTED_ROWS_SAMPLE:");
            unexpected.show(10, false);
            System.out.println("MISSING_ROWS_SAMPLE:");
            missing.show(10, false);
            throw new IllegalStateException("Reconciliation failed: unexpected="
                    + unexpectedCount + ", missing=" + missingCount);
        }
        System.out.println("SPIKE2_RECONCILIATION_COMPLETE");
    }

    private static Dataset<Row> normaliseForComparison(Dataset<Row> data) {
        for (String column : new String[] {
                "report_from_utc_date", "report_to_utc_date_exclusive",
                "permission_start_date", "permission_end_date"}) {
            data = data.withColumn(column,
                    date_format(col(column).cast("date"), "yyyy-MM-dd"));
        }
        return data.withColumn("stored_active_flag",
                col("stored_active_flag").cast("boolean"));
    }

    /** AC 10: Export to a unique, timestamped directory after reconciliation. */
    private static void ac10ExportReconciledCsv(Dataset<Row> report, String baseOutputPath) {
        heading("AC10 - Export reconciled report to CSV");

        // One output directory per run; UTC timestamp is readable and the
        // short UUID prevents collisions between concurrent executions.
        String timestamp = REPORT_TIMESTAMP.format(Instant.now());
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String outputPath = baseOutputPath.replaceAll("/+$", "")
                + "_" + timestamp + "_" + runId;

        System.out.println("REPORT_TIMESTAMP_UTC=" + timestamp);
        System.out.println("CSV_EXPORT_START=" + outputPath);
        report.coalesce(1)
                .write()
                .mode("errorifexists")
                .option("header", "true")
                .option("dateFormat", "yyyy-MM-dd")
                .csv(outputPath);

        System.out.println("REPORT_CSV_EXPORTED=" + outputPath);
        System.out.println("SPIKE2_EXPORT_COMPLETE");
        System.out.println("==========================================");
        System.out.println("CHECK & CHALLENGE REPORT GENERATED");
        System.out.println("REPORT_OUTPUT_PATH=" + outputPath);
        if (outputPath.startsWith("abfss://")) {
            System.out.println("FABRIC_LAKEHOUSE_URL=" + FABRIC_LAKEHOUSE_URL);
            System.out.println("REPORT_FOLDER=" + REPORT_FOLDER_IN_LAKEHOUSE
                    + "_" + timestamp + "_" + runId);
            System.out.println("NOTE=Open the Lakehouse URL and browse to REPORT_FOLDER");
        } else {
            System.out.println("LOCAL_REPORT_FOLDER=" + outputPath);
        }
        System.out.println("CSV_FILE_PATTERN=part-*.csv");
        System.out.println("==========================================");
    }

    /**
     * Output destination priority: first command-line argument, JVM property,
     * environment variable, then the Fabric OneLake default.
     * Spark CSV output is a directory containing part-*.csv and _SUCCESS.
     */
    private static String resolveOutputPath(String[] args) {
        String path = args != null && args.length > 0 ? args[0] : null;
        if (path == null || path.trim().isEmpty()) {
            path = System.getProperty("report.output.path");
        }
        if (path == null || path.trim().isEmpty()) {
            path = System.getenv("REPORT_OUTPUT_PATH");
        }
        if (path == null || path.trim().isEmpty()) {
            path = FABRIC_OUTPUT_PATH;
        }
        path = path.trim();
        require(!path.isEmpty(), "CSV output path cannot be empty");
        return path;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void heading(String label) {
        System.out.println("========== " + label + " ==========");
    }

    private static final class SourceData {
        private final Dataset<Row> permissions;
        private final Dataset<Row> users;

        private SourceData(Dataset<Row> permissions, Dataset<Row> users) {
            this.permissions = permissions;
            this.users = users;
        }
    }
}
