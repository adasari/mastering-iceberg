/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.example.local;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *  Data file : Local filesystem.
 *  Metadata tables: Jdbc catalog (sqlite db)
 *  Metadata files: Local filesystem
 */

public class Write {

    public static void main(String[] args) throws IOException {
        // Define the Avro schema for the Parquet file
        String schema = "{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": ["
                + "{\"name\": \"id\", \"type\": \"int\"},"
                + "{\"name\": \"name\", \"type\": \"string\"}"
                + "]}";

        Schema avroSchema = new Schema.Parser().parse(schema);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        // JDBC Catalog configuration
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        catalogProperties.put(CatalogProperties.URI, "jdbc:sqlite:file:/Users/ADASARI/work/learning/iceberg/catalog/catalog-db-local.db");
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "file:///Users/ADASARI/work/learning/iceberg/catalog-warehouse");

        Configuration hadoopConfig = new Configuration();
        // Create a Catalog instance
        Catalog catalog = CatalogUtil.buildIcebergCatalog("test_local_warehouse", catalogProperties, hadoopConfig);

        // Define the table identifier (e.g., "namespace"."table_name")
        TableIdentifier tableIdentifier = TableIdentifier.of("local", "test_table");

        if (catalog.tableExists(tableIdentifier)) {
            System.out.println("Table already exists.");
        } else {
            catalog.createTable(tableIdentifier, icebergSchema, PartitionSpec.unpartitioned());
        }

        // Load the table from the REST catalog
        Table table = catalog.loadTable(tableIdentifier);

        File localFile = new File("local-file.parquet").getAbsoluteFile();
        FileIO fileIO = new HadoopFileIO(new Configuration());
        OutputFile file = fileIO.newOutputFile("file:///" + localFile.getAbsolutePath());

        DataWriter<GenericRecord> dataWriter = null;
        try {
            dataWriter = Parquet.writeData(file)
                    .schema(icebergSchema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();
            GenericRecord record = GenericRecord.create(icebergSchema);
            record.setField("id", 100);
            record.setField("name", "John");
            dataWriter.write(record);
        } catch (Exception ex) {
            ex.printStackTrace();
            if (dataWriter != null) {
                try {
                    dataWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return;
        }

        dataWriter.close();
        DataFile dataFile = dataWriter.toDataFile();
        table.newAppend().appendFile(dataFile).commit();

        System.out.println("Table location: "+ table.location());
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                System.out.println("ID: " + record.getField("id") + ", Name: " + record.getField("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
