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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TsFileTableSchemaRegistrationUtilsTest {

  private static final String DATABASE = "schema_registration_test";

  @After
  public void tearDown() {
    DataNodeTableCache.getInstance().invalid(DATABASE);
  }

  @Test
  public void testRegistersMissingSchemaWithoutAddingTimeColumn() {
    final String tableName = "t1";
    final TsTable table = new TsTable(tableName);
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    registerTable(tableName, table);

    final Schema schema = new Schema();
    final TsFileIOWriter writer = mockWriter(schema, tableName);
    TsFileTableSchemaRegistrationUtils.registerTableSchemaIfAbsent(writer, DATABASE, tableName);

    final TableSchema registered = schema.getTableSchemaMap().get(tableName);
    assertNotNull(registered);
    assertEquals(1, registered.getColumnSchemas().size());
    assertEquals(ColumnCategory.TAG, registered.getColumnTypes().get(0));
    assertFalse(registered.getColumnTypes().contains(ColumnCategory.TIME));
  }

  @Test
  public void testTimeOnlyTableStillGetsEmptySchemaEntry() {
    final String tableName = "time_only_table";
    final TsTable table = new TsTable(tableName);
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    registerTable(tableName, table);

    final Schema schema = new Schema();
    final TsFileIOWriter writer = mockWriter(schema, tableName);
    TsFileTableSchemaRegistrationUtils.registerTableSchemaIfAbsent(writer, DATABASE, tableName);

    final TableSchema registered = schema.getTableSchemaMap().get(tableName);
    assertNotNull(registered);
    assertTrue(registered.getColumnTypes().isEmpty());
  }

  @Test
  public void testDroppedTableIsNotTouched() {
    final String tableName = "dropped_table";
    final Schema schema = new Schema();
    final TsFileIOWriter writer = mockWriter(schema, tableName);

    TsFileTableSchemaRegistrationUtils.registerTableSchemaIfAbsent(writer, DATABASE, tableName);

    // The storage path does not describe a table it cannot read. The file stays recognizable
    // through the table-model file property instead.
    assertTrue(schema.getTableSchemaMap().isEmpty());
  }

  @Test
  public void testExistingSchemaIsNotRewritten() {
    final String tableName = "existing_table";
    final Schema schema = new Schema();
    final TableSchema existingSchema = new TableSchema(tableName);
    schema.registerTableSchema(existingSchema);
    final TsFileIOWriter writer = mockWriter(schema, tableName);

    TsFileTableSchemaRegistrationUtils.registerTableSchemaIfAbsent(writer, DATABASE, tableName);

    assertSame(existingSchema, schema.getTableSchemaMap().get(tableName));
    Mockito.verify(writer, Mockito.never())
        .addTsFileProperty(Mockito.anyString(), Mockito.any(byte[].class));
  }

  @Test
  public void testDatabaseNameIsOnlyUsedAsCacheKey() {
    // The model of the region is known by the caller (the owning DataRegion), so this method must
    // not guess it from the name: a database whose name looks like a tree-model one, but that does
    // own the table, has to be described as well.
    final String database = "root.tree_named_db";
    final String tableName = "table_on_root_named_db";
    final TsTable table = new TsTable(tableName);
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
    DataNodeTableCache.getInstance().preUpdateTable(database, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(database, tableName, null, null);
    try {
      final Schema schema = new Schema();

      TsFileTableSchemaRegistrationUtils.registerTableSchemaIfAbsent(
          mockWriter(schema, tableName), database, tableName);

      assertNotNull(schema.getTableSchemaMap().get(tableName));
    } finally {
      DataNodeTableCache.getInstance().invalid(database);
    }
  }

  @Test
  public void testMarkTableModelWritesSingleProperty() {
    final TsFileIOWriter writer = mockWriter(new Schema(), "t1");

    TsFileTableSchemaRegistrationUtils.markTableModel(writer);

    Mockito.verify(writer)
        .addTsFileProperty(
            Mockito.eq(TsFileTableSchemaRegistrationUtils.TABLE_MODEL_PROPERTY),
            AdditionalMatchers.aryEq("true".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testTableModelPropertyIsPersistedWithoutTableSchema() throws IOException {
    final Path tempDir = Files.createTempDirectory("table-model-marker");
    final File tsFile = tempDir.resolve("marker.tsfile").toFile();
    try {
      try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
        TsFileTableSchemaRegistrationUtils.markTableModel(writer);
        writer.endFile();
      }

      try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
        final TsFileMetadata metadata = reader.readFileMetadata();
        // The single property is enough to recognize the file as a table-model TsFile, even when
        // the TableSchema map is empty.
        assertTrue(TsFileTableSchemaRegistrationUtils.isTableModel(metadata));
        assertArrayEquals(
            "true".getBytes(StandardCharsets.UTF_8),
            metadata
                .getTsFileProperties()
                .get(TsFileTableSchemaRegistrationUtils.TABLE_MODEL_PROPERTY));
        assertTrue(reader.getTableSchemaMap().isEmpty());
      }
    } finally {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(tempDir);
    }
  }

  private static void registerTable(final String tableName, final TsTable table) {
    DataNodeTableCache.getInstance().preUpdateTable(DATABASE, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(DATABASE, tableName, null, null);
  }

  private static TsFileIOWriter mockWriter(final Schema schema, final String tableName) {
    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    Mockito.when(writer.getSchema()).thenReturn(schema);
    final IDeviceID device = new StringArrayDeviceID(tableName, "tag");
    Mockito.when(writer.getDeviceChunkMetadataMap())
        .thenReturn(Collections.singletonMap(device, Collections.emptyList()));
    return writer;
  }
}
