package com.example.spark.local;

import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * test_catalog/test_namespace/alter_table_test_new/
 * ├── data
 * │   ├── city=Mountain+View
 * │   │   ├── 00000-4-63f73f14-5358-4957-bdde-3278eb841b0d-0-00001.parquet
 * │   │   └── country=CA
 * │   │       └── 00000-9-7553b45a-6dff-4794-a9e4-d394b0ee1646-0-00001.parquet
 * │   ├── city=Sunnyvale
 * │   │   ├── 00000-4-63f73f14-5358-4957-bdde-3278eb841b0d-0-00002.parquet
 * │   │   └── country=CA
 * │   │       └── 00000-9-7553b45a-6dff-4794-a9e4-d394b0ee1646-0-00002.parquet
 * │   └── country=CA
 * │       └── 00000-14-4853c73a-8e1c-4d64-991b-063abbde0f14-0-00001.parquet
 * └── metadata
 *     ├── 10f7ba4f-24fc-4345-8bf1-cfc85a39d2c0-m0.avro
 *     ├── 273190f9-40e5-4c79-ae9e-36d57643b12d-m0.avro
 *     ├── f1662421-018d-425a-95ec-82a5fc53c89b-m0.avro
 *     ├── snap-1241609459196940426-1-f1662421-018d-425a-95ec-82a5fc53c89b.avro
 *     ├── snap-3939391822974485798-1-273190f9-40e5-4c79-ae9e-36d57643b12d.avro
 *     ├── snap-815926136251002927-1-10f7ba4f-24fc-4345-8bf1-cfc85a39d2c0.avro
 *     ├── v1.metadata.json
 *     ├── v2.metadata.json
 *     ├── v3.metadata.json
 *     ├── v4.metadata.json
 *     ├── v5.metadata.json
 *     ├── v6.metadata.json
 *     ├── v7.metadata.json
 *     └── version-hint.text
 */
public class AlterTable {

    public static void main(String[] args) throws NoSuchTableException {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // spark.sql.catalog.<catalog-name>
                .set("spark.sql.catalog.test_catalog","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.test_catalog.type","hadoop")
                .set("spark.sql.catalog.test_catalog.warehouse", "file:///Users/ADASARI/work/workspace/java/mastering-iceberg/test_catalog");

        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();

        spark.sql("CREATE OR REPLACE TABLE test_catalog.test_namespace.alter_table_test_new (id INT, name STRING, city STRING, country STRING) " +
                "USING iceberg PARTITIONED BY (city)");

        StructType structType = new StructType();
        structType = structType.add("id", DataTypes.IntegerType, false);
        structType = structType.add("name", DataTypes.StringType, false);
        structType = structType.add("city", DataTypes.StringType, false);
        structType = structType.add("country", DataTypes.StringType, false);

        List<Row> records = new ArrayList<Row>();
        records.add(RowFactory.create(1,"A","Sunnyvale", "CA"));
        records.add(RowFactory.create(2,"B","Mountain View", "CA"));
        records.add(RowFactory.create(3,"C","Sunnyvale", "CA"));
        records.add(RowFactory.create(4,"D","Mountain View", "CA"));

        Dataset<Row> dataset = spark.createDataFrame(records, structType);
        try {
            dataset.writeTo(" test_catalog.test_namespace.alter_table_test_new").append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new RuntimeException(e);
        }

        // add partition
        spark.sql("ALTER TABLE test_catalog.test_namespace.alter_table_test_new ADD PARTITION FIELD country");

        List<Row> countryRecords = new ArrayList<Row>();
        countryRecords.add(RowFactory.create(11,"A","Sunnyvale", "CA"));
        countryRecords.add(RowFactory.create(12,"B","Mountain View", "CA"));
        countryRecords.add(RowFactory.create(13,"C","Sunnyvale", "CA"));
        countryRecords.add(RowFactory.create(14,"D","Mountain View", "CA"));

        Dataset<Row> countryDataset = spark.createDataFrame(countryRecords, structType);
        try {
            countryDataset.writeTo(" test_catalog.test_namespace.alter_table_test_new").append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new RuntimeException(e);
        }

        // drop partition
        spark.sql("ALTER TABLE test_catalog.test_namespace.alter_table_test_new DROP PARTITION FIELD city");

        List<Row> dropCityRecords = new ArrayList<Row>();
        dropCityRecords.add(RowFactory.create(11,"A","Sunnyvale", "CA"));
        dropCityRecords.add(RowFactory.create(12,"B","Mountain View", "CA"));
        dropCityRecords.add(RowFactory.create(13,"C","Sunnyvale", "CA"));
        dropCityRecords.add(RowFactory.create(14,"D","Mountain View", "CA"));

        Dataset<Row> dropCityDataset = spark.createDataFrame(dropCityRecords, structType);
        try {
            dropCityDataset.writeTo(" test_catalog.test_namespace.alter_table_test_new").append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new RuntimeException(e);
        }

        // rename column
//        spark.sql("ALTER TABLE test_catalog.test_namespace.alter_table_test_new RENAME COLUMN city to new_city");
//
//        List<Row> renameCityRecords = new ArrayList<Row>();
//        renameCityRecords.add(RowFactory.create(11,"A","Sunnyvale", "CA"));
//        renameCityRecords.add(RowFactory.create(12,"B","Mountain View", "CA"));
//        renameCityRecords.add(RowFactory.create(13,"C","Sunnyvale", "CA"));
//        renameCityRecords.add(RowFactory.create(14,"D","Mountain View", "CA"));
//
//        Dataset<Row> renameCityDatasets = spark.createDataFrame(dropCityRecords, structType);
//        try {
//            renameCityDatasets.writeTo(" test_catalog.test_namespace.alter_table_test_new").append();
//        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
//            throw new RuntimeException(e);
//        }
    }
}
