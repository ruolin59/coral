/**
 * Copyright 2019-2025 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.integration;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.testng.annotations.Test;

import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.spark.CoralSpark;
import com.linkedin.coral.trino.rel2trino.HiveToTrinoConverter;

import static org.testng.Assert.*;


/**
 * Sample integration test demonstrating Coral interoperability with Spark, Trino, Iceberg, and Hive Tables/Views.
 */
public class CoralInteropIntegrationTest extends CoralIntegrationTestBase {

  @Test
  public void testCreateHiveViewOnIcebergTable() throws Exception {
    // Create an Iceberg table using fully qualified name
    executeSql("CREATE TABLE IF NOT EXISTS iceberg_catalog.default.test_iceberg_table "
        + "(id BIGINT, name STRING, age INT, salary DOUBLE, hire_date TIMESTAMP) " + "USING iceberg");

    // Insert test data into the Iceberg table
    executeSql("INSERT INTO iceberg_catalog.default.test_iceberg_table "
        + "SELECT 1L, 'Alice', 30, 75000.0, current_timestamp() UNION ALL "
        + "SELECT 2L, 'Bob', 25, 65000.0, current_timestamp() UNION ALL "
        + "SELECT 3L, 'Charlie', 35, 85000.0, current_timestamp()");

    // Create a Hive view on top of the Iceberg table
    // The view filters employees with age > 25 and selects specific columns including timestamp
    executeSql("USE iceberg_catalog");
    executeSql("CREATE OR REPLACE VIEW spark_catalog.default.iceberg_table_view AS "
        + "SELECT id, name, age, hire_date FROM default.test_iceberg_table WHERE age > 25");
    executeSql("USE spark_catalog");

    // Query the Hive view
    Dataset<Row> viewResult = spark.sql("SELECT * FROM spark_catalog.default.iceberg_table_view");
    long viewCount = viewResult.count();

    // Verify the view returns the expected number of rows (2 employees with age > 25)
    assertEquals(viewCount, 2, "View should return 2 rows with age > 25");

    // Verify we can filter on the view
    Dataset<Row> filteredView = spark.sql("SELECT name FROM spark_catalog.default.iceberg_table_view WHERE age >= 30");
    assertEquals(filteredView.count(), 2, "Should have 2 employees with age >= 30");

    // Test Coral Spark translation
    String db = "default";
    String table = "iceberg_table_view";

    HiveMetastoreClient hiveMetastoreClient = createCoralHiveMetastoreClient();

    // Test Spark translation and validation
    CoralSpark coralSparkTranslation = getCoralSparkTranslation(db, table, hiveMetastoreClient);
    assertTrue(validateSparkSql(spark, coralSparkTranslation));

    // Test Trino translation and validation
    // Ideally we run this against a trino server in unit test, like we did for Spark.
    // But trino testcontainers require a local docker daemon to spin up which may not be available in all environments.
    HiveToTrinoConverter hiveToTrinoConverter = HiveToTrinoConverter.create(hiveMetastoreClient);
    String trinoSql = hiveToTrinoConverter.toTrinoSql(db, table);
    assertNotNull(trinoSql, "Trino SQL translation should not be null");
    assertTrue(validateTrinoSql(trinoSql), "Trino SQL validation should succeed");

    RelNode relNode = getRelNode(db, table, hiveMetastoreClient);
    assertNotNull(relNode, "RelNode conversion should not be null");
    RelDataType timestampField = relNode.getRowType().getFieldList().stream()
        .filter(field -> field.getName().equals("hire_date")).map(field -> field.getType()).findFirst().orElse(null);

    assertNotNull(timestampField, "hire_date field should exist in RelNode");
    assertEquals(timestampField.getSqlTypeName(), SqlTypeName.TIMESTAMP, "hire_date field should be of TIMESTAMP type");
    assertEquals(timestampField.getPrecision(), -1,
        "TIMESTAMP field should have precision 6 (microsecond precision) when bug is fixed");

    // Drop the view after test
    executeSql("DROP VIEW IF EXISTS spark_catalog.default.iceberg_table_view");
    executeSql("DROP TABLE IF EXISTS iceberg_catalog.default.test_iceberg_table");
  }

  @Test
  public void testArrayWithComplexNestedStructs() throws Exception {
    // This test verifies that both table and view schemas match the original avro.schema.literal
    // when array items are defined as unions in the base table's avro.schema.literal

    // Define the Avro schema with array items as a union type: items = [{"type":"record",...}]
    // This is the key to reproducing the bug - the array items is a union with single element
    String originalAvroSchemaLiteral =
        "{\"type\":\"record\",\"name\":\"test_complex_array_table\",\"namespace\":\"com.example.test\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"entity_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"items\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"ItemConfig\",\"namespace\":\"com.example.data\",\"doc\":\"Configuration information for items.\",\"fields\":["
            + "{\"name\":\"fooConfiguration\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"FooConfiguration\",\"doc\":\"Foo configuration details.\",\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\",\"doc\":\"The name of the configuration\"},"
            + "{\"name\":\"urlValue\",\"type\":\"string\",\"doc\":\"The URL value for the configuration\"},"
            + "{\"name\":\"source\",\"type\":\"string\",\"doc\":\"The source of the configuration\"}"
            + "]}],\"default\":null,\"doc\":\"Foo configuration details.\"},"
            + "{\"name\":\"barConfiguration\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BarConfiguration\",\"doc\":\"Bar configuration details.\",\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\",\"doc\":\"The name of the configuration\"},"
            + "{\"name\":\"domain\",\"type\":\"string\",\"doc\":\"The domain value\"}"
            + "]}],\"default\":null,\"doc\":\"Bar configuration details.\"}"
            + "]}]}],\"default\":null}" + "]}";

    // Create an Iceberg table with the Avro schema
    executeSql("CREATE TABLE IF NOT EXISTS iceberg_catalog.default.test_complex_array_table " + "(id BIGINT, "
        + " entity_name STRING, " + " items ARRAY<STRUCT<"
        + "   fooConfiguration: STRUCT<name: STRING, urlValue: STRING, source: STRING>, "
        + "   barConfiguration: STRUCT<name: STRING, domain: STRING>" + " >>) " + "USING iceberg "
        + "TBLPROPERTIES ('avro.schema.literal'='" + originalAvroSchemaLiteral + "')");

    // Insert test data
    executeSql("INSERT INTO iceberg_catalog.default.test_complex_array_table " + "SELECT 1L, 'EntityA', " + "  ARRAY("
        + "    named_struct("
        + "      'fooConfiguration', named_struct('name', 'Foo-Config-1', 'urlValue', 'https://example.com/foo', 'source', 'https://source.example.com'), "
        + "      'barConfiguration', CAST(NULL AS STRUCT<name: STRING, domain: STRING>)" + "    )" + "  )");

    // Verify data exists
    executeSql("USE iceberg_catalog");
    Dataset<Row> tblResult = spark.sql("SELECT * FROM default.test_complex_array_table");
    assertEquals(tblResult.count(), 1, "Table should return 1 row");

    // Create a view on top of the table
    executeSql("CREATE OR REPLACE VIEW spark_catalog.default.complex_array_view AS "
        + "SELECT * FROM default.test_complex_array_table");
    executeSql("USE spark_catalog");
    Dataset<Row> viewResult = spark.sql("SELECT * FROM default.complex_array_view");
    assertEquals(viewResult.count(), 1, "View should return 1 row");

    HiveMetastoreClient baseHmsClient = createCoralHiveMetastoreClient();

    // Wrap the HMS client so that base table is forced to return `org.apache.hadoop.hive.serde2.avro.AvroSerDe` for serlializationLib
    HiveMetastoreClient hiveMetastoreClient = new HiveMetastoreClient() {
      @Override
      public List<String> getAllDatabases() {
        return baseHmsClient.getAllDatabases();
      }

      @Override
      public org.apache.hadoop.hive.metastore.api.Database getDatabase(String dbName) {
        return baseHmsClient.getDatabase(dbName);
      }

      @Override
      public List<String> getAllTables(String dbName) {
        return baseHmsClient.getAllTables(dbName);
      }

      @Override
      public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName) {
        org.apache.hadoop.hive.metastore.api.Table table = baseHmsClient.getTable(dbName, tableName);

        // In prod, Iceberg tables often do not have AvroSerDe library set on storage descriptor, but have avro.schema.literal
        if (table != null && table.getParameters() != null
            && table.getParameters().containsKey("avro.schema.literal")) {
          // Set AvroSerDe on the storage descriptor to null
          if (table.getSd() != null) {
            table.getSd().getSerdeInfo().setSerializationLib(null);
            // Add the avro.schema.literal to SerDe parameters
            table.getSd().getSerdeInfo().getParameters().put("avro.schema.literal",
            originalAvroSchemaLiteral);
          }
        }

        return table;
      }
    };

    System.out.println("\n=== Testing Coral Schema Conversion ===");

    // Parse the original Avro schema
    Schema originalAvroSchema = new Schema.Parser().parse(originalAvroSchemaLiteral);

    // Get view's Avro schema from Coral conversion
    System.out.println("\n--- VIEW SCHEMA (from Coral ViewToAvroSchemaConverter) ---");
    Schema viewAvroSchema = com.linkedin.coral.schema.avro.ViewToAvroSchemaConverter.create(hiveMetastoreClient)
        .toAvroSchema("default", "complex_array_view", false, true);
    System.out.println(viewAvroSchema.toString(true));

    System.out.println("\n--- COMPARING fooConfiguration FIELD ---");

    // Navigate to fooConfiguration in original schema
    // Path: root -> items (union) -> array -> items (union) -> record -> fooConfiguration
    Schema originalFooSchema = unwrapUnion(
        unwrapUnion(unwrapUnion(originalAvroSchema.getField("items").schema()).getElementType())
            .getField("fooConfiguration").schema());

    // Navigate to fooConfiguration in view schema
    Schema viewFooSchema = unwrapUnion(
        unwrapUnion(unwrapUnion(viewAvroSchema.getField("items").schema()).getElementType())
            .getField("fooconfiguration").schema());

    // Compare the fooConfiguration schemas
    System.out.println("\nOriginal fooConfiguration schema:");
    System.out.println(originalFooSchema.toString(true));
    System.out.println("\nView fooConfiguration schema:");
    System.out.println(viewFooSchema.toString(true));

    // Compare field by field; all 3 assertEquals fail due to the bug related to nullability
    assertEquals(viewFooSchema.getField("name").schema(), originalFooSchema.getField("name").schema(),
        "name field should match");
    assertEquals(viewFooSchema.getField("urlvalue").schema(), originalFooSchema.getField("urlValue").schema(),
        "urlValue field should match");
    assertEquals(viewFooSchema.getField("source").schema(), originalFooSchema.getField("source").schema(),
        "source field should match");

    System.out.println("\n✓ fooConfiguration fields match!");

    // Clean up
    executeSql("DROP VIEW IF EXISTS spark_catalog.default.complex_array_view");
    executeSql("DROP TABLE IF EXISTS iceberg_catalog.default.test_complex_array_table");
  }

  private RelNode getRelNode(String db, String view, HiveMetastoreClient hiveMetastoreClient) {
    return new HiveToRelConverter(hiveMetastoreClient).convertView(db, view);
  }

  /**
   * Unwrap a union type to get the non-null type (assuming union of [null, Type]).
   *
   * @param schema The schema (potentially a union)
   * @return The unwrapped schema
   */
  private Schema unwrapUnion(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema type : schema.getTypes()) {
        if (type.getType() != Schema.Type.NULL) {
          return type;
        }
      }
    }
    return schema;
  }
}
