package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class InsertTable {

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
        spark.sql("CREATE OR REPLACE TABLE test_catalog.test_namespace.employee (id INT, name STRING, city STRING) " +
                "USING iceberg PARTITIONED BY (city)");

        spark.sql("CREATE OR REPLACE TABLE test_catalog.test_namespace.employee_partitioned_month (id INT, name STRING, city STRING, join_date DATE) " +
                "USING iceberg PARTITIONED BY (months(join_date))");

        StructType structType = new StructType();
        structType = structType.add("id", DataTypes.IntegerType, false);
        structType = structType.add("name", DataTypes.StringType, false);
        structType = structType.add("city", DataTypes.StringType, false);
//        structType = structType.add("join_date", DataTypes.DateType, true);

        List<Row> records = new ArrayList<Row>();
        records.add(RowFactory.create(1,"A","Sunnyvale"));
        records.add(RowFactory.create(2,"B","Mountain View"));
        records.add(RowFactory.create(3,"C","Sunnyvale"));
        records.add(RowFactory.create(4,"D","Mountain View"));

        Dataset<Row> dataset = spark.createDataFrame(records, structType);
        try {
            dataset.writeTo(" test_catalog.test_namespace.employee").append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }
}
