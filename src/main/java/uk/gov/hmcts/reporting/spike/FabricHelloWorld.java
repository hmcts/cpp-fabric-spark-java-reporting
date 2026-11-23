
package uk.gov.hmcts.reporting.spike;

import java.time.Instant;

import org.apache.spark.sql.SparkSession;

public final class FabricHelloWorld {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("FabricJVMHelloWorld")
                .getOrCreate();

        try {
            String recipient =
                    args.length > 0 ? args[0] : "Fabric";

            System.out.println("SPIKE_START");
            System.out.println("Hello " + recipient + " from Java in Fabric");
            System.out.println(
                    "Java version: " +
                            System.getProperty("java.version")
            );
            System.out.println("Timestamp UTC: " + Instant.now());
            System.out.println("SPIKE_COMPLETE");

        } finally {
            spark.stop();
        }
    }
}
