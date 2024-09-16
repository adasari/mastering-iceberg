package com.example.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ListTables {

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
        Configuration conf = new Configuration();
        String warehousePath = "file:///Users/ADASARI/work/workspace/java/mastering-iceberg/test_catalog";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        List<TableIdentifier> tables = catalog.listTables(Namespace.of("test_namespace"));
        System.out.println(tables);
    }
}
