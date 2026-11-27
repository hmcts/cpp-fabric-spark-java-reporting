# HMCTS Access Reporting — Fabric JVM Spike 1

## 1. Objective

Validate that a compiled Java JAR can execute directly in Microsoft Fabric using a Spark Job Definition.

The spike demonstrates Java execution, runtime argument passing, driver logging and repeatable deployment.

Fabric Runtime 2.0 (Spark 4.1, Delta 4.2)

## 2. Implementation

- Language: Java
- Build tool: Maven
- Framework: Apache Spark (Java API)
- Deployment: Microsoft Fabric Spark Job Definition
- Main class: `uk.gov.hmcts.reporting.spike.FabricHelloWorld`
- JAR: `target/fabric-jvm-hello.jar`

The Java application initialises a SparkSession and prints a greeting, Java runtime version, UTC timestamp and completion markers.

## 3. Prerequisites

- Compatible JDK installed
- Maven installed
- Access to the Fabric workspace
- Permission to configure and execute Spark Job Definitions
- Compatible Fabric Spark runtime
- Default Lakehouse/environment configured as required

## 4. Build Instructions

From the project root:

```bash
java -version
mvn -version
mvn clean package
```

Expected result: `BUILD SUCCESS`

Verify the packaged Java class:

```bash
jar tf target/fabric-jvm-hello.jar
```

Expected class:

`uk/gov/hmcts/reporting/spike/FabricHelloWorld.class`

Verify the main method:

```bash
javap -classpath target/fabric-jvm-hello.jar uk.gov.hmcts.reporting.spike.FabricHelloWorld
```

## 5. Fabric Deployment

1. Open the target Microsoft Fabric workspace.
2. Create or open the Spark Job Definition `access_reporting`.
3. Select Spark (Scala/Java).
4. Upload `target/fabric-jvm-hello.jar` as the main definition file.
5. Set the main class to `uk.gov.hmcts.reporting.spike.FabricHelloWorld`.
6. Configure the approved environment and default Lakehouse.
7. Set the command-line argument to `HMCTS`.
8. Save and execute the job.

## 6. Execution Evidence

### Run 1 — HMCTS

- Run ID: application_1795772089664_0001
- Execution date/time UTC: 11/27/26 9:33:00 AM
- Status: Success
- Duration: 1 min 48 sec

Expected output:

```text
SPIKE_START
Hello HMCTS from Java in Fabric
Java version: ...
Timestamp UTC: ...
SPIKE_COMPLETE
```

### Run 2 — ReportingTeam

Change the command-line argument to `ReportingTeam`, save and execute again without rebuilding the JAR.

- Run ID: 6d8eaed7-b1aa-44a8-8006-13bdcfe2167c
- Execution date/time UTC: 11/23/26 2:00:55 PM
- Status: Success
- Duration: 3 min 33 sec

Expected greeting:

`Hello ReportingTeam from Java in Fabric`

## 7. Acceptance Criteria

- [x] AC1 — Java application compiled, packaged and locally validated
- [x] AC2 — JAR executed directly as a Fabric Spark Job Definition
- [x] AC3 — Driver logs show greeting, Java version, UTC timestamp and completion markers
- [x] AC4 — Second successful execution with a different argument
- [x] AC5 — Source, build steps, deployment configuration and execution evidence recorded for reproducibility

## 8. Issues Encountered

During initial investigation, the following issues were observed:

- SparkContext initialisation failure
- Java main-class lookup failure
- Fabric Spark monitoring application lookup error


