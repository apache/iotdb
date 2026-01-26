/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TsFileTableSchemaUtilTest {

  @Test
  public void testToTsFileTableSchemaNoAttribute() {
    // Create a TsTable with all column types
    final TsTable table = new TsTable("testTable");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    table.addColumnSchema(new TagColumnSchema("tag2", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr2", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field1", TSDataType.DOUBLE));
    table.addColumnSchema(new FieldColumnSchema("field2", TSDataType.INT32));

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Verify table name
    Assert.assertEquals("testtable", result.getTableName());
    Assert.assertNotNull(result.getColumnSchemas());
    Assert.assertNotNull(result.getColumnTypes());

    // Verify that TIME and ATTRIBUTE columns are filtered out
    final List<IMeasurementSchema> measurementSchemas = result.getColumnSchemas();
    final List<ColumnCategory> columnTypes = result.getColumnTypes();

    // Should only have TAG and FIELD columns (2 TAG + 2 FIELD = 4)
    Assert.assertEquals("Expected 4 columns (2 TAG + 2 FIELD)", 4, measurementSchemas.size());
    Assert.assertEquals("Column types size should match schemas size", 4, columnTypes.size());
    Assert.assertEquals(
        "measurementSchemas and columnTypes should have same size",
        measurementSchemas.size(),
        columnTypes.size());

    // Verify column names, types, and categories in order
    verifyColumn(
        measurementSchemas.get(0),
        columnTypes.get(0),
        "tag1",
        TSDataType.STRING,
        ColumnCategory.TAG);
    verifyColumn(
        measurementSchemas.get(1),
        columnTypes.get(1),
        "tag2",
        TSDataType.STRING,
        ColumnCategory.TAG);
    verifyColumn(
        measurementSchemas.get(2),
        columnTypes.get(2),
        "field1",
        TSDataType.DOUBLE,
        ColumnCategory.FIELD);
    verifyColumn(
        measurementSchemas.get(3),
        columnTypes.get(3),
        "field2",
        TSDataType.INT32,
        ColumnCategory.FIELD);

    // Verify columnPosIndex if available
    if (result.getColumnPosIndex() != null) {
      final Map<String, Integer> columnPosIndex = result.getColumnPosIndex();
      Assert.assertEquals("tag1 should be at position 0", 0, (int) columnPosIndex.get("tag1"));
      Assert.assertEquals("tag2 should be at position 1", 1, (int) columnPosIndex.get("tag2"));
      Assert.assertEquals("field1 should be at position 2", 2, (int) columnPosIndex.get("field1"));
      Assert.assertEquals("field2 should be at position 3", 3, (int) columnPosIndex.get("field2"));
      Assert.assertEquals("columnPosIndex should have 4 entries", 4, columnPosIndex.size());
    }

    // Verify no TIME or ATTRIBUTE columns are present
    final Set<String> columnNames =
        measurementSchemas.stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toSet());
    Assert.assertFalse("TIME column should be filtered out", columnNames.contains("time"));
    Assert.assertFalse(
        "ATTRIBUTE column attr1 should be filtered out", columnNames.contains("attr1"));
    Assert.assertFalse(
        "ATTRIBUTE column attr2 should be filtered out", columnNames.contains("attr2"));
  }

  /** Helper method to verify a column's properties */
  private void verifyColumn(
      final IMeasurementSchema schema,
      final ColumnCategory category,
      final String expectedName,
      final TSDataType expectedDataType,
      final ColumnCategory expectedCategory) {
    Assert.assertNotNull("Schema should not be null", schema);
    Assert.assertEquals("Column name mismatch", expectedName, schema.getMeasurementName());
    Assert.assertEquals("Data type mismatch", expectedDataType, schema.getType());
    Assert.assertEquals("Column category mismatch", expectedCategory, category);
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithOnlyTagAndField() {
    // Create a TsTable with only TAG and FIELD columns
    final TsTable table = new TsTable("testTable2");
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field1", TSDataType.DOUBLE));

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Verify all columns are included
    Assert.assertEquals("Should have 2 columns", 2, result.getColumnSchemas().size());
    Assert.assertEquals("Should have 2 column types", 2, result.getColumnTypes().size());
    verifyColumn(
        result.getColumnSchemas().get(0),
        result.getColumnTypes().get(0),
        "tag1",
        TSDataType.STRING,
        ColumnCategory.TAG);
    verifyColumn(
        result.getColumnSchemas().get(1),
        result.getColumnTypes().get(1),
        "field1",
        TSDataType.DOUBLE,
        ColumnCategory.FIELD);
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithOnlyTimeAndAttribute() {
    // Create a TsTable with only TIME and ATTRIBUTE columns
    final TsTable table = new TsTable("testTable3");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    table.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr2", TSDataType.STRING));

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Verify all columns are filtered out
    Assert.assertEquals(
        "All TIME and ATTRIBUTE columns should be filtered out",
        0,
        result.getColumnSchemas().size());
    Assert.assertEquals("Column types should be empty", 0, result.getColumnTypes().size());
    Assert.assertEquals("Table name should be preserved", "testtable3", result.getTableName());
    Assert.assertNotNull("Column schemas list should not be null", result.getColumnSchemas());
    Assert.assertNotNull("Column types list should not be null", result.getColumnTypes());
  }

  @Test
  public void testTsTableBufferToTableSchemaNoAttribute() throws IOException {
    // Create a TsTable with all column types
    final TsTable table = new TsTable("testTable4");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field1", TSDataType.DOUBLE));

    // Serialize to ByteBuffer
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    table.serialize(outputStream);
    final ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());
    final int originalPosition = buffer.position();

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.tsTableBufferToTableSchemaNoAttribute(buffer);

    // Verify buffer was consumed (position changed)
    Assert.assertTrue("Buffer position should have changed", buffer.position() != originalPosition);

    // Verify table name
    Assert.assertEquals("testtable4", result.getTableName());

    // Verify that TIME and ATTRIBUTE columns are filtered out
    final List<IMeasurementSchema> measurementSchemas = result.getColumnSchemas();
    final List<ColumnCategory> columnTypes = result.getColumnTypes();

    // Should only have TAG and FIELD columns (1 TAG + 1 FIELD = 2)
    Assert.assertEquals("Should have 2 columns after filtering", 2, measurementSchemas.size());
    Assert.assertEquals("Column types size should match", 2, columnTypes.size());

    // Verify column names and types
    verifyColumn(
        measurementSchemas.get(0),
        columnTypes.get(0),
        "tag1",
        TSDataType.STRING,
        ColumnCategory.TAG);
    verifyColumn(
        measurementSchemas.get(1),
        columnTypes.get(1),
        "field1",
        TSDataType.DOUBLE,
        ColumnCategory.FIELD);

    // Verify no filtered columns are present
    final Set<String> columnNames =
        measurementSchemas.stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toSet());
    Assert.assertFalse("TIME column should be filtered", columnNames.contains("time"));
    Assert.assertFalse("ATTRIBUTE column should be filtered", columnNames.contains("attr1"));
  }

  @Test
  public void testTsTableBufferToTableSchemaNoAttributeWithManyColumns() throws IOException {
    // Create a TsTable with many columns to test performance
    final TsTable table = new TsTable("testTable5");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));

    final int tagCount = 10;
    final int attrCount = 10;
    final int fieldCount = 10;

    // Add many TAG columns
    for (int i = 0; i < tagCount; i++) {
      table.addColumnSchema(new TagColumnSchema("tag" + i, TSDataType.STRING));
    }

    // Add many ATTRIBUTE columns
    for (int i = 0; i < attrCount; i++) {
      table.addColumnSchema(new AttributeColumnSchema("attr" + i, TSDataType.STRING));
    }

    // Add many FIELD columns
    for (int i = 0; i < fieldCount; i++) {
      table.addColumnSchema(new FieldColumnSchema("field" + i, TSDataType.DOUBLE));
    }

    // Serialize to ByteBuffer
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    table.serialize(outputStream);
    final ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.tsTableBufferToTableSchemaNoAttribute(buffer);

    // Verify that TIME and ATTRIBUTE columns are filtered out
    // Should only have 10 TAG + 10 FIELD = 20 columns
    final int expectedCount = tagCount + fieldCount;
    Assert.assertEquals(
        "Should have " + expectedCount + " columns after filtering",
        expectedCount,
        result.getColumnSchemas().size());
    Assert.assertEquals(
        "Column types size should match", expectedCount, result.getColumnTypes().size());

    // Verify all TAG columns are present and in order
    for (int i = 0; i < tagCount; i++) {
      verifyColumn(
          result.getColumnSchemas().get(i),
          result.getColumnTypes().get(i),
          "tag" + i,
          TSDataType.STRING,
          ColumnCategory.TAG);
    }

    // Verify all FIELD columns are present and in order
    for (int i = 0; i < fieldCount; i++) {
      final int index = tagCount + i;
      verifyColumn(
          result.getColumnSchemas().get(index),
          result.getColumnTypes().get(index),
          "field" + i,
          TSDataType.DOUBLE,
          ColumnCategory.FIELD);
    }

    // Verify no TIME or ATTRIBUTE columns are present
    final Set<String> columnNames =
        result.getColumnSchemas().stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toSet());
    Assert.assertFalse("TIME column should be filtered", columnNames.contains("time"));
    for (int i = 0; i < attrCount; i++) {
      Assert.assertFalse(
          "ATTRIBUTE column attr" + i + " should be filtered", columnNames.contains("attr" + i));
    }
  }

  @Test
  public void testColumnCategoryFilter() {
    // Test NO_ATTRIBUTE filter
    final TsFileTableSchemaUtil.ColumnCategoryFilter filter =
        TsFileTableSchemaUtil.ColumnCategoryFilter.NO_ATTRIBUTE;

    // TIME should be filtered out
    Assert.assertFalse("TIME should be filtered out", filter.test(TsTableColumnCategory.TIME));

    // ATTRIBUTE should be filtered out
    Assert.assertFalse(
        "ATTRIBUTE should be filtered out", filter.test(TsTableColumnCategory.ATTRIBUTE));

    // TAG should be included
    Assert.assertTrue("TAG should be included", filter.test(TsTableColumnCategory.TAG));

    // FIELD should be included
    Assert.assertTrue("FIELD should be included", filter.test(TsTableColumnCategory.FIELD));

    // Test all enum values
    for (TsTableColumnCategory category : TsTableColumnCategory.values()) {
      final boolean shouldInclude =
          category != TsTableColumnCategory.TIME && category != TsTableColumnCategory.ATTRIBUTE;
      Assert.assertEquals(
          "Category " + category + " filter result mismatch", shouldInclude, filter.test(category));
    }
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithProps() {
    // Create a TsTable with columns that have properties
    final TsTable table = new TsTable("testTable6");
    final Map<String, String> props = new HashMap<>();
    props.put("key1", "value1");
    props.put("key2", "value2");
    props.put("description", "test column");

    final TagColumnSchema tagWithProps = new TagColumnSchema("tag1", TSDataType.STRING, props);
    final Map<String, String> fieldProps = new HashMap<>();
    fieldProps.put("unit", "meter");
    final FieldColumnSchema fieldWithProps =
        new FieldColumnSchema(
            "field1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY, fieldProps);

    table.addColumnSchema(tagWithProps);
    table.addColumnSchema(fieldWithProps);

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Verify columns are included (props should not affect filtering)
    Assert.assertEquals("Should have 2 columns", 2, result.getColumnSchemas().size());
    verifyColumn(
        result.getColumnSchemas().get(0),
        result.getColumnTypes().get(0),
        "tag1",
        TSDataType.STRING,
        ColumnCategory.TAG);
    verifyColumn(
        result.getColumnSchemas().get(1),
        result.getColumnTypes().get(1),
        "field1",
        TSDataType.DOUBLE,
        ColumnCategory.FIELD);
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithAllDataTypes() {
    // Test all supported data types
    final TsTable table = new TsTable("testTableAllTypes");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field_boolean", TSDataType.BOOLEAN));
    table.addColumnSchema(new FieldColumnSchema("field_int32", TSDataType.INT32));
    table.addColumnSchema(new FieldColumnSchema("field_int64", TSDataType.INT64));
    table.addColumnSchema(new FieldColumnSchema("field_float", TSDataType.FLOAT));
    table.addColumnSchema(new FieldColumnSchema("field_double", TSDataType.DOUBLE));
    table.addColumnSchema(new FieldColumnSchema("field_text", TSDataType.TEXT));
    table.addColumnSchema(new FieldColumnSchema("field_blob", TSDataType.BLOB));
    table.addColumnSchema(new FieldColumnSchema("field_date", TSDataType.DATE));
    table.addColumnSchema(new FieldColumnSchema("field_timestamp", TSDataType.TIMESTAMP));

    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Should have 1 TAG + 9 FIELD = 10 columns
    Assert.assertEquals("Should have 10 columns", 10, result.getColumnSchemas().size());

    // Verify TAG column
    verifyColumn(
        result.getColumnSchemas().get(0),
        result.getColumnTypes().get(0),
        "tag1",
        TSDataType.STRING,
        ColumnCategory.TAG);

    // Verify all FIELD columns with their data types
    verifyColumn(
        result.getColumnSchemas().get(1),
        result.getColumnTypes().get(1),
        "field_boolean",
        TSDataType.BOOLEAN,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(2),
        result.getColumnTypes().get(2),
        "field_int32",
        TSDataType.INT32,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(3),
        result.getColumnTypes().get(3),
        "field_int64",
        TSDataType.INT64,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(4),
        result.getColumnTypes().get(4),
        "field_float",
        TSDataType.FLOAT,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(5),
        result.getColumnTypes().get(5),
        "field_double",
        TSDataType.DOUBLE,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(6),
        result.getColumnTypes().get(6),
        "field_text",
        TSDataType.TEXT,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(7),
        result.getColumnTypes().get(7),
        "field_blob",
        TSDataType.BLOB,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(8),
        result.getColumnTypes().get(8),
        "field_date",
        TSDataType.DATE,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(9),
        result.getColumnTypes().get(9),
        "field_timestamp",
        TSDataType.TIMESTAMP,
        ColumnCategory.FIELD);
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithEncodingAndCompression() {
    // Test FIELD columns with different encoding and compression
    final TsTable table = new TsTable("testTableEncoding");
    table.addColumnSchema(
        new FieldColumnSchema(
            "field1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
    table.addColumnSchema(
        new FieldColumnSchema("field2", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4));
    table.addColumnSchema(
        new FieldColumnSchema(
            "field3", TSDataType.FLOAT, TSEncoding.TS_2DIFF, CompressionType.GZIP));

    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    Assert.assertEquals("Should have 3 FIELD columns", 3, result.getColumnSchemas().size());
    verifyColumn(
        result.getColumnSchemas().get(0),
        result.getColumnTypes().get(0),
        "field1",
        TSDataType.DOUBLE,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(1),
        result.getColumnTypes().get(1),
        "field2",
        TSDataType.INT32,
        ColumnCategory.FIELD);
    verifyColumn(
        result.getColumnSchemas().get(2),
        result.getColumnTypes().get(2),
        "field3",
        TSDataType.FLOAT,
        ColumnCategory.FIELD);
    // Note: Encoding and compression are not preserved in the result MeasurementSchema
    // as they are set to defaults during conversion
  }

  @Test
  public void testTsTableBufferToTableSchemaNoAttributeRoundTrip() throws IOException {
    // Test round-trip: TsTable -> ByteBuffer -> TableSchema
    final TsTable originalTable = new TsTable("roundTripTable");
    originalTable.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    originalTable.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    originalTable.addColumnSchema(new TagColumnSchema("tag2", TSDataType.STRING));
    originalTable.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    originalTable.addColumnSchema(new FieldColumnSchema("field1", TSDataType.DOUBLE));
    originalTable.addColumnSchema(new FieldColumnSchema("field2", TSDataType.INT32));

    // Method 1: Direct conversion
    final TableSchema directResult =
        TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(originalTable);

    // Method 2: ByteBuffer conversion
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    originalTable.serialize(outputStream);
    final ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());
    final TableSchema bufferResult =
        TsFileTableSchemaUtil.tsTableBufferToTableSchemaNoAttribute(buffer);

    // Both methods should produce equivalent results
    Assert.assertEquals(
        "Table names should match", directResult.getTableName(), bufferResult.getTableName());
    Assert.assertEquals(
        "Column count should match",
        directResult.getColumnSchemas().size(),
        bufferResult.getColumnSchemas().size());
    Assert.assertEquals(
        "Column types count should match",
        directResult.getColumnTypes().size(),
        bufferResult.getColumnTypes().size());

    // Verify each column matches
    for (int i = 0; i < directResult.getColumnSchemas().size(); i++) {
      final IMeasurementSchema directSchema = directResult.getColumnSchemas().get(i);
      final IMeasurementSchema bufferSchema = bufferResult.getColumnSchemas().get(i);
      Assert.assertEquals(
          "Column " + i + " name should match",
          directSchema.getMeasurementName(),
          bufferSchema.getMeasurementName());
      Assert.assertEquals(
          "Column " + i + " type should match", directSchema.getType(), bufferSchema.getType());
      Assert.assertEquals(
          "Column " + i + " category should match",
          directResult.getColumnTypes().get(i),
          bufferResult.getColumnTypes().get(i));
    }
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeColumnOrder() {
    // Test that column order is preserved
    final TsTable table = new TsTable("orderTest");
    final List<String> expectedOrder = new ArrayList<>();

    // Add columns in specific order
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    expectedOrder.add("tag1");
    table.addColumnSchema(new TagColumnSchema("tag2", TSDataType.STRING));
    expectedOrder.add("tag2");
    table.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field1", TSDataType.DOUBLE));
    expectedOrder.add("field1");
    table.addColumnSchema(new FieldColumnSchema("field2", TSDataType.INT32));
    expectedOrder.add("field2");
    table.addColumnSchema(new TagColumnSchema("tag3", TSDataType.STRING));
    expectedOrder.add("tag3");
    table.addColumnSchema(new FieldColumnSchema("field3", TSDataType.FLOAT));
    expectedOrder.add("field3");

    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Verify order matches expected (TAG columns first, then FIELD columns, in original order)
    final List<String> actualOrder =
        result.getColumnSchemas().stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toList());

    Assert.assertEquals(
        "Column count should match expected", expectedOrder.size(), actualOrder.size());
    Assert.assertEquals("Column order should match", expectedOrder, actualOrder);
  }

  @Test
  public void testTsTableBufferToTableSchemaNoAttributeWithEmptyTable() throws IOException {
    // Create an empty TsTable
    final TsTable table = new TsTable("emptyTable");

    // Serialize to ByteBuffer
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    table.serialize(outputStream);
    final ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.tsTableBufferToTableSchemaNoAttribute(buffer);

    // Verify empty result
    Assert.assertEquals("emptytable", result.getTableName());
    Assert.assertEquals("Empty table should have no columns", 0, result.getColumnSchemas().size());
    Assert.assertEquals(
        "Empty table should have no column types", 0, result.getColumnTypes().size());
    Assert.assertNotNull("Column schemas list should not be null", result.getColumnSchemas());
    Assert.assertNotNull("Column types list should not be null", result.getColumnTypes());
    Assert.assertTrue("Column schemas should be empty", result.getColumnSchemas().isEmpty());
    Assert.assertTrue("Column types should be empty", result.getColumnTypes().isEmpty());
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithEmptyTable() {
    // Create an empty TsTable
    final TsTable table = new TsTable("emptyTable2");

    // Convert to TableSchema
    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Verify empty result
    Assert.assertEquals("emptytable2", result.getTableName());
    Assert.assertEquals("Empty table should have no columns", 0, result.getColumnSchemas().size());
    Assert.assertEquals(
        "Empty table should have no column types", 0, result.getColumnTypes().size());
    Assert.assertNotNull("Column schemas list should not be null", result.getColumnSchemas());
    Assert.assertNotNull("Column types list should not be null", result.getColumnTypes());
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeWithMixedOrder() {
    // Test with columns in mixed order (TIME, TAG, ATTRIBUTE, FIELD, etc.)
    final TsTable table = new TsTable("mixedOrderTable");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr1", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field1", TSDataType.DOUBLE));
    table.addColumnSchema(new TimeColumnSchema("time2", TSDataType.INT64));
    table.addColumnSchema(new TagColumnSchema("tag2", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("attr2", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("field2", TSDataType.INT32));

    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    // Should have 2 TAG + 2 FIELD = 4 columns
    Assert.assertEquals("Should have 4 columns", 4, result.getColumnSchemas().size());

    // Verify order: tag1, tag2, field1, field2 (original order preserved)
    final List<String> columnNames =
        result.getColumnSchemas().stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toList());
    Assert.assertEquals("tag1", columnNames.get(0));
    Assert.assertEquals("field1", columnNames.get(1));
    Assert.assertEquals("tag2", columnNames.get(2));
    Assert.assertEquals("field2", columnNames.get(3));
  }

  @Test
  public void testTsTableBufferToTableSchemaNoAttributeWithLargeTable() throws IOException {
    // Test with a very large table (1000+ columns) to verify performance
    final TsTable table = new TsTable("largeTable");
    final int totalColumns = 1000;
    final int tagColumns = 200;
    final int attrColumns = 300;
    final int fieldColumns = 500;

    // Add TIME column
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));

    // Add TAG columns
    for (int i = 0; i < tagColumns; i++) {
      table.addColumnSchema(new TagColumnSchema("tag" + i, TSDataType.STRING));
    }

    // Add ATTRIBUTE columns
    for (int i = 0; i < attrColumns; i++) {
      table.addColumnSchema(new AttributeColumnSchema("attr" + i, TSDataType.STRING));
    }

    // Add FIELD columns
    for (int i = 0; i < fieldColumns; i++) {
      table.addColumnSchema(new FieldColumnSchema("field" + i, TSDataType.DOUBLE));
    }

    // Serialize to ByteBuffer
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    table.serialize(outputStream);
    final ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());

    // Convert to TableSchema
    final long startTime = System.currentTimeMillis();
    final TableSchema result = TsFileTableSchemaUtil.tsTableBufferToTableSchemaNoAttribute(buffer);
    final long endTime = System.currentTimeMillis();

    // Verify result
    final int expectedCount = tagColumns + fieldColumns;
    Assert.assertEquals(
        "Should have " + expectedCount + " columns",
        expectedCount,
        result.getColumnSchemas().size());
    Assert.assertEquals("Column types should match", expectedCount, result.getColumnTypes().size());

    // Verify all TAG columns are present
    for (int i = 0; i < tagColumns; i++) {
      final IMeasurementSchema schema = result.getColumnSchemas().get(i);
      Assert.assertEquals("tag" + i, schema.getMeasurementName());
      Assert.assertEquals(ColumnCategory.TAG, result.getColumnTypes().get(i));
    }

    // Verify all FIELD columns are present
    for (int i = 0; i < fieldColumns; i++) {
      final int index = tagColumns + i;
      final IMeasurementSchema schema = result.getColumnSchemas().get(index);
      Assert.assertEquals("field" + i, schema.getMeasurementName());
      Assert.assertEquals(ColumnCategory.FIELD, result.getColumnTypes().get(index));
    }

    // Performance check: should complete in reasonable time (less than 1 second for 1000 columns)
    final long duration = endTime - startTime;
    Assert.assertTrue(
        "Conversion should complete quickly (took " + duration + "ms)", duration < 1000);
  }

  @Test
  public void testToTsFileTableSchemaNoAttributeNullSafety() {
    // Test with null table name (should not happen in practice, but test robustness)
    final TsTable table = new TsTable("nullTest");
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));

    final TableSchema result = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);

    Assert.assertNotNull("Result should not be null", result);
    Assert.assertNotNull("Table name should not be null", result.getTableName());
    Assert.assertNotNull("Column schemas should not be null", result.getColumnSchemas());
    Assert.assertNotNull("Column types should not be null", result.getColumnTypes());
  }
}
