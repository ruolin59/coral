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
        "{\"type\":\"record\",\"name\":\"test_sso_configs_table\",\"namespace\":\"com.linkedin.test\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"company_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sso_configs\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"SsoConfiguration\",\"namespace\":\"com.linkedin.enterprise.account\",\"doc\":\"The information about SSO configurations.\",\"fields\":["
            + "{\"name\":\"samlSsoConfiguration\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SamlSsoConfiguration\",\"doc\":\"The information about SAML 2.0 SSO configurations.\",\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\",\"doc\":\"The name of the SSO configuration\"},"
            + "{\"name\":\"credentialUrn\",\"type\":\"string\",\"doc\":\"An urn represents credentials that validate SAML responses\"},"
            + "{\"name\":\"issuer\",\"type\":\"string\",\"doc\":\"The issuer of the SAML assertion\"}"
            + "]}],\"default\":null,\"doc\":\"The information about SAML 2.0 SSO configurations.\"},"
            + "{\"name\":\"googleSsoConfiguration\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"GoogleSsoConfiguration\",\"doc\":\"The information about Google OpenID Connect SSO configurations.\",\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\",\"doc\":\"The name of the SSO configuration\"},"
            + "{\"name\":\"hostedDomain\",\"type\":\"string\",\"doc\":\"The hosted domain of the company\"}"
            + "]}],\"default\":null,\"doc\":\"The information about Google OpenID Connect SSO configurations.\"}"
            + "]}]}],\"default\":null}" + "]}";

    // Create an Iceberg table with the Avro schema
    executeSql("CREATE TABLE IF NOT EXISTS iceberg_catalog.default.test_sso_configs_table " + "(id BIGINT, "
        + " company_name STRING, " + " sso_configs ARRAY<STRUCT<"
        + "   samlSsoConfiguration: STRUCT<name: STRING, credentialUrn: STRING, issuer: STRING>, "
        + "   googleSsoConfiguration: STRUCT<name: STRING, hostedDomain: STRING>" + " >>) " + "USING iceberg "
        + "TBLPROPERTIES ('avro.schema.literal'='" + originalAvroSchemaLiteral + "')");

    // Insert test data
    executeSql("INSERT INTO iceberg_catalog.default.test_sso_configs_table " + "SELECT 1L, 'CompanyA', " + "  ARRAY("
        + "    named_struct("
        + "      'samlSsoConfiguration', named_struct('name', 'SAML-Config-1', 'credentialUrn', 'urn:li:credential:123', 'issuer', 'https://sso.companya.com'), "
        + "      'googleSsoConfiguration', CAST(NULL AS STRUCT<name: STRING, hostedDomain: STRING>)" + "    )" + "  )");

    // Verify data exists
    executeSql("USE iceberg_catalog");
    Dataset<Row> tblResult = spark.sql("SELECT * FROM default.test_sso_configs_table");
    assertEquals(tblResult.count(), 1, "Table should return 1 row");

    // Create a view on top of the table
    executeSql("CREATE OR REPLACE VIEW spark_catalog.default.sso_configs_view AS "
        + "SELECT * FROM default.test_sso_configs_table");
    executeSql("USE spark_catalog");
    Dataset<Row> viewResult = spark.sql("SELECT * FROM default.sso_configs_view");
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

        // If the table has avro.schema.literal property, ensure it has AvroSerDe configured
        if (table != null && table.getParameters() != null
            && table.getParameters().containsKey("avro.schema.literal")) {
          // Set AvroSerDe on the storage descriptor so Coral can process it
          if (table.getSd() != null) {
            table.getSd().getSerdeInfo().setSerializationLib(null);
            // Also add the avro.schema.literal to SerDe parameters
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
        .toAvroSchema("default", "sso_configs_view", false, true);
    System.out.println(viewAvroSchema.toString(true));

    System.out.println("\n--- COMPARING samlSsoConfiguration FIELD ---");

    // Navigate to samlSsoConfiguration in original schema
    // Path: root -> sso_configs (union) -> array -> items (union) -> record -> samlSsoConfiguration
    Schema originalSamlSchema = unwrapUnion(
        unwrapUnion(unwrapUnion(originalAvroSchema.getField("sso_configs").schema()).getElementType())
            .getField("samlSsoConfiguration").schema());

    // Navigate to samlSsoConfiguration in view schema
    Schema viewSamlSchema = unwrapUnion(
        unwrapUnion(unwrapUnion(viewAvroSchema.getField("sso_configs").schema()).getElementType())
            .getField("samlssoconfiguration").schema());

    // Compare the samlSsoConfiguration schemas
    System.out.println("\nOriginal samlSsoConfiguration schema:");
    System.out.println(originalSamlSchema.toString(true));
    System.out.println("\nView samlSsoConfiguration schema:");
    System.out.println(viewSamlSchema.toString(true));

    // Compare field by field
    assertEquals(viewSamlSchema.getField("name").schema(), originalSamlSchema.getField("name").schema(),
        "name field should match");
    assertEquals(viewSamlSchema.getField("credentialurn").schema(), originalSamlSchema.getField("credentialUrn").schema(),
        "credentialUrn field should match");
    assertEquals(viewSamlSchema.getField("issuer").schema(), originalSamlSchema.getField("issuer").schema(),
        "issuer field should match");

    System.out.println("\n✓ samlSsoConfiguration fields match!");

    // Clean up
    executeSql("DROP VIEW IF EXISTS spark_catalog.default.sso_configs_view");
    executeSql("DROP TABLE IF EXISTS iceberg_catalog.default.test_sso_configs_table");
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
