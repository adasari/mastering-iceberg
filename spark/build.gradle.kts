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

    implementation("org.apache.spark:spark-core_2.13:3.4.1")
    implementation("org.apache.spark:spark-avro_2.13:3.4.1")
    implementation("org.apache.spark:spark-sql_2.13:3.4.1")

    implementation("org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.6.0")

    // aws
    implementation("software.amazon.awssdk:s3:2.20.13")
    implementation("software.amazon.awssdk:sts:2.20.13")

    implementation("org.xerial:sqlite-jdbc:3.41.2.2")

    testImplementation("junit:junit:4.13.2")
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
