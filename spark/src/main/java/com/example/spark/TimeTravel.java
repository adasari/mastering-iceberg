package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class TimeTravel {

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

        // test_catalog.test_namespace.employee
        spark.sql("select * from test_catalog.test_namespace.employee").show();
        spark.sql("select * from test_catalog.test_namespace.employee.history").show();
    }
}
