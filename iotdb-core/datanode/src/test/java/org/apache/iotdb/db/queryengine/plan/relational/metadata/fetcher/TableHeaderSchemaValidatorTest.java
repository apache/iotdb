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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.exception.table.ColumnInDeletionException;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TableHeaderSchemaValidatorTest {
  private static final String DATABASE = "pre_delete_write_test";
  private static final String TABLE = "table1";
  private final ITableCache cache = DataNodeTableCache.getInstance();
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final MPPQueryContext context = new MPPQueryContext(new QueryId("pre_delete_write_test"));
  private ClusterConfigTaskExecutor executor;
  private TableHeaderSchemaValidator validator;
  private boolean autoCreateSchema;
  private boolean partialInsert;

  @Before
  public void setUp() {
    autoCreateSchema = config.isAutoCreateSchemaEnabled();
    partialInsert = config.isEnablePartialInsert();
    executor = Mockito.mock(ClusterConfigTaskExecutor.class);
    validator = new TableHeaderSchemaValidator(executor);
    cache.invalid(DATABASE);
    final TsTable table = new TsTable(TABLE);
    table.addColumnSchema(
        new FieldColumnSchema("live", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4));
    table.addColumnSchema(
        new FieldColumnSchema("field", TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4));
    table.addColumnSchema(new AttributeColumnSchema("attribute", TSDataType.STRING));
    cache.preUpdateTable(DATABASE, table, null);
    cache.commitUpdateTable(DATABASE, TABLE, null);
    // A DROP COLUMN invalidates the DataNode cache before removing ConfigNode metadata.
    cache.invalid(DATABASE, TABLE, "field");
    cache.invalid(DATABASE, TABLE, "attribute");
    Mockito.when(executor.getPreDeletedColumns(DATABASE, TABLE))
        .thenReturn(new java.util.HashSet<>(Arrays.asList("field", "attribute")));
  }

  @After
  public void tearDown() {
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    cache.invalid(DATABASE);
    config.setAutoCreateSchemaEnabled(autoCreateSchema);
    config.setEnablePartialInsert(partialInsert);
  }

  @Test
  public void testInsertReportsDeletionBeforeUnknownCategory() {
    for (final boolean autoCreate : new boolean[] {true, false}) {
      config.setAutoCreateSchemaEnabled(autoCreate);
      final InsertNodeMeasurementInfo measurements = measurements("field", null);
      final SemanticException error =
          assertThrows(
              SemanticException.class,
              () ->
                  validator.validateInsertNodeMeasurements(
                      DATABASE, measurements, context, true, null, null));
      assertDeletion(error, "field");
    }
  }

  @Test
  public void testInsertRejectsPreDeletedAttribute() {
    final SemanticException error =
        assertThrows(
            SemanticException.class,
            () ->
                validator.validateInsertNodeMeasurements(
                    DATABASE,
                    measurements("attribute", TsTableColumnCategory.ATTRIBUTE),
                    context,
                    true,
                    null,
                    null));
    assertDeletion(error, "attribute");
  }

  @Test
  public void testTsFileLoadRejectsPreDeletedFieldAndAttribute() {
    for (final boolean autoCreate : new boolean[] {true, false}) {
      config.setAutoCreateSchemaEnabled(autoCreate);
      for (final String column : Arrays.asList("field", "attribute")) {
        final TableSchema tableSchema =
            new TableSchema(
                TABLE,
                Collections.singletonList(
                    new ColumnSchema(
                        column,
                        TypeFactory.getType(
                            column.equals("field") ? TSDataType.INT32 : TSDataType.STRING),
                        false,
                        column.equals("field")
                            ? TsTableColumnCategory.FIELD
                            : TsTableColumnCategory.ATTRIBUTE)));
        final SemanticException error =
            assertThrows(
                SemanticException.class,
                () ->
                    validator.validateTableHeaderSchema4TsFile(
                        DATABASE, tableSchema, context, true, false, new AtomicBoolean()));
        assertDeletion(error, column);
      }
    }
  }

  @Test
  public void testExistingColumnsDoNotFetchDeletionStatus() throws Exception {
    validator.validateInsertNodeMeasurements(
        DATABASE, measurements("live", TsTableColumnCategory.FIELD), context, true, null, null);
    validator.validateTableHeaderSchema4TsFile(
        DATABASE,
        new TableSchema(
            TABLE,
            Collections.singletonList(
                new ColumnSchema(
                    "live",
                    TypeFactory.getType(TSDataType.INT32),
                    false,
                    TsTableColumnCategory.FIELD))),
        context,
        true,
        false,
        new AtomicBoolean());
    Mockito.verify(executor, Mockito.never())
        .getPreDeletedColumns(Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testMissingColumnStillReportsUnknownCategory() {
    final SemanticException error =
        assertThrows(
            SemanticException.class,
            () ->
                validator.validateInsertNodeMeasurements(
                    DATABASE, measurements("missing", null), context, true, null, null));
    assertEquals(TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode(), error.getErrorCode());
  }

  @Test
  public void testMissingColumnsFetchDeletionStatusOnce() {
    config.setAutoCreateSchemaEnabled(false);
    config.setEnablePartialInsert(true);
    final InsertNodeMeasurementInfo measurements = Mockito.mock(InsertNodeMeasurementInfo.class);
    Mockito.when(measurements.getTableName()).thenReturn(TABLE);
    Mockito.when(measurements.getMeasurementCount()).thenReturn(2);
    Mockito.when(measurements.getColumnCategories())
        .thenReturn(
            new TsTableColumnCategory[] {TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD});
    Mockito.when(measurements.getMeasurementName(0)).thenReturn("missing1");
    Mockito.when(measurements.getMeasurementName(1)).thenReturn("missing2");
    validator.validateInsertNodeMeasurements(DATABASE, measurements, context, true, null, null);
    Mockito.verify(executor).getPreDeletedColumns(DATABASE, TABLE);
  }

  private InsertNodeMeasurementInfo measurements(
      final String name, final TsTableColumnCategory category) {
    final InsertNodeMeasurementInfo measurements = Mockito.mock(InsertNodeMeasurementInfo.class);
    Mockito.when(measurements.getTableName()).thenReturn(TABLE);
    Mockito.when(measurements.getMeasurementCount()).thenReturn(1);
    Mockito.when(measurements.getColumnCategories())
        .thenReturn(new TsTableColumnCategory[] {category});
    Mockito.when(measurements.getMeasurementName(0)).thenReturn(name);
    Mockito.when(measurements.getType(0)).thenReturn(TSDataType.INT32);
    return measurements;
  }

  private static void assertDeletion(final SemanticException error, final String column) {
    assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), error.getErrorCode());
    assertTrue(error.getCause() instanceof ColumnInDeletionException);
    assertEquals(
        new ColumnInDeletionException(DATABASE, TABLE, column).getMessage(),
        error.getCause().getMessage());
  }
}
