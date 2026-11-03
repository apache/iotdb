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
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
    TsFileTableSchemaRegistrationUtils.ensureTableSchemasForExistingChunks(
        mockWriter(schema, tableName), DATABASE);

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
    TsFileTableSchemaRegistrationUtils.ensureTableSchemasForExistingChunks(
        mockWriter(schema, tableName), DATABASE);

    final TableSchema registered = schema.getTableSchemaMap().get(tableName);
    assertNotNull(registered);
    assertTrue(registered.getColumnTypes().isEmpty());
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
