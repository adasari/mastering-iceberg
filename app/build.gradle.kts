/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.4/userguide/building_java_projects.html in the Gradle documentation.
 */

plugins {
    java
    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {

    implementation("org.apache.iceberg:iceberg-core:1.6.0")           // Iceberg Core
    implementation("org.apache.iceberg:iceberg-api:1.6.0")
    implementation("org.apache.iceberg:iceberg-data:1.6.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.6.0")
    implementation("org.apache.iceberg:iceberg-aws:1.6.0")

//    implementation("org.apache.iceberg:iceberg-hadoop:1.6.0")
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-client-api:3.3.6")
    implementation("org.apache.hadoop:hadoop-aws:3.3.6")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6")

    // Parquet Writer dependencies
    implementation("org.apache.parquet:parquet-avro:1.12.3")
    implementation("org.apache.parquet:parquet-common:1.12.3")
    implementation("org.apache.parquet:parquet-hadoop:1.12.3")

    // aws
    implementation("software.amazon.awssdk:s3:2.20.13")
    implementation("software.amazon.awssdk:sts:2.20.13")

    implementation("org.xerial:sqlite-jdbc:3.41.2.2")

    // Use JUnit test framework.
    testImplementation("junit:junit:4.13.2")

    // This dependency is used by the application.
    implementation("com.google.guava:guava:32.1.1-jre")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

application {
    // Define the main class for the application.
    mainClass.set("com.example.App")
}
