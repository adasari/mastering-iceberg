package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CreateTable {

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "/Users/ADASARI/work/workspace/java/mastering-iceberg");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // spark.sql.catalog.<catalog-name>
                .set("spark.sql.catalog.test_catalog","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.test_catalog.type","hadoop")
                .set("spark.sql.catalog.test_catalog.warehouse", "file:///Users/ADASARI/work/workspace/java/mastering-iceberg/test_catalog")
                ;
        SparkSession spark = SparkSession.builder().appName("Iceberg Spark App").config(sparkConf).getOrCreate();
        // <catalog-name>.<namespace>.<table-name>
        spark.sql("CREATE OR REPLACE TABLE test_catalog.test_namespace.employee (id INT, name STRING, city STRING, join_date DATE) " +
                "USING iceberg PARTITIONED BY (city)");

        spark.sql("CREATE OR REPLACE TABLE test_catalog.test_namespace.employee_partitioned_month (id INT, name STRING, city STRING, join_date DATE) " +
                "USING iceberg PARTITIONED BY (months(join_date))");
    }
}
