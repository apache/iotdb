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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertNumericRow;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertNumericRowStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowReq;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class PreparedNumericRowInfoTest {
  private static final String DATABASE = "prepared_numeric_fastpath_test";
  private TsTable table;
  private MPPQueryContext context;
  private Metadata metadata;

  @Before
  public void setUp() {
    DataNodeTableCache.getInstance().invalid(DATABASE);
    table = new TsTable("t");
    table.addColumnSchema(new TagColumnSchema("tag", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("v", TSDataType.DOUBLE));
    install(table);
    context = new MPPQueryContext(new QueryId("prepared_test"));
    context.setSession(new SessionInfo(1, "root", ZoneId.of("UTC")));
    metadata = mock(Metadata.class);
    doAnswer(
            invocation -> {
              TableHeaderSchemaValidator.getInstance()
                  .validateInsertNodeMeasurements(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2),
                      invocation.getArgument(3),
                      invocation.getArgument(4),
                      invocation.getArgument(5));
              return null;
            })
        .when(metadata)
        .validateInsertNodeMeasurements(anyString(), any(), any(), anyBoolean(), any(), any());
  }

  @After
  public void tearDown() {
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    DataNodeTableCache.getInstance().invalid(DATABASE);
  }

  // An unchanged table skips column validation but still checks access and device metadata.
  @Test
  public void testUnchangedVersionRetainsOtherChecks() {
    PreparedNumericRowInfo info = new PreparedNumericRowInfo(DATABASE, request("tag", "v"));
    InsertNumericRowStatement statement = bind(info, 1);
    InsertNumericRowStatement next = bind(info, 1);
    assertNotNull(statement.getPreparedTableVersion());
    assertNotSame(statement.getMeasurementSchemas(), next.getMeasurementSchemas());
    assertSame(statement.getMeasurementSchemas()[1], next.getMeasurementSchemas()[1]);
    AccessControl accessControl = mock(AccessControl.class);
    InsertNumericRow insert = new InsertNumericRow(statement, context);
    SchemaValidator.validate(metadata, insert, context, accessControl);
    verify(metadata, never())
        .validateInsertNodeMeasurements(anyString(), any(), any(), anyBoolean(), any(), any());
    verify(metadata).validateDeviceSchema(insert, context);
    verify(accessControl).checkCanInsertIntoTable(eq("root"), any(), eq(context));
    assertTrue(context.getAcquiredLocks().contains(SchemaLockType.VALIDATE_VS_DELETION_TABLE));
    assertEquals(TSDataType.DOUBLE, statement.getMeasurementSchemas()[1].getType());
    statement.getMeasurementSchemas()[1] = null;
    assertNotNull(bind(info, 1).getMeasurementSchemas()[1]);
  }

  // Adding a TAG after bind forces normal validation to pad the new device-id segment.
  @Test
  public void testVersionChangeBetweenBindAndAnalysis() {
    PreparedNumericRowInfo info = new PreparedNumericRowInfo(DATABASE, request("tag", "v"));
    InsertNumericRowStatement statement = bind(info, 1);
    table.addColumnSchema(new TagColumnSchema("tag2", TSDataType.STRING));
    validateFallback(statement);
    assertArrayEquals(new String[] {"tag", "tag2", "v"}, statement.getMeasurements());
    assertNull(statement.getFirstValueOfIndex(1));
    assertEquals(2.5D, statement.getFirstValueOfIndex(2));
    assertArrayEquals(new String[] {"tag", "v"}, bind(info, 1).getMeasurements());
  }

  // A replacement table with the same name/revision must not reuse the old instance's schema.
  @Test
  public void testRecreatedTableUsesFullValidation() {
    InsertNumericRowStatement statement =
        bind(new PreparedNumericRowInfo(DATABASE, request("tag", "v")), 1);
    TsTable replacement = new TsTable("t");
    replacement.addColumnSchema(new TagColumnSchema("tag", TSDataType.STRING));
    replacement.addColumnSchema(new FieldColumnSchema("v", TSDataType.DOUBLE));
    assertEquals(table.getInstanceVersion().right, replacement.getInstanceVersion().right);
    install(replacement);
    validateFallback(statement);
    assertNotNull(statement.getMeasurementSchemas()[1]);
  }

  // Missing columns/table, views and type conversions remain on normal creation/validation.
  @Test
  public void testIneligibleMetadataHasNoCachedSchema() {
    assertNull(
        bind(new PreparedNumericRowInfo(DATABASE, request("tag", "missing")), 1)
            .getPreparedTableVersion());
    TSPrepareNumericRowReq req = request("tag", "v");
    req.setDataTypes(Arrays.asList(TSDataType.STRING.ordinal(), TSDataType.INT64.ordinal()));
    assertNull(
        new PreparedNumericRowInfo(DATABASE, req)
            .bind(1, tagBuffer(1), ByteBuffer.allocate(8))
            .getPreparedTableVersion());
    table.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.sg.**");
    assertNull(
        bind(new PreparedNumericRowInfo(DATABASE, request("tag", "v")), 1)
            .getPreparedTableVersion());
    DataNodeTableCache.getInstance().invalid(DATABASE);
    assertNull(
        bind(new PreparedNumericRowInfo(DATABASE, request("tag", "v")), 1).getMeasurementSchemas());
  }

  // Reversed TAG input must take the normal path so payload values follow schema order.
  @Test
  public void testTagReorderingFallsBack() {
    table.addColumnSchema(new TagColumnSchema("tag2", TSDataType.STRING));
    InsertNumericRowStatement statement =
        bind(new PreparedNumericRowInfo(DATABASE, request("tag2", "tag", "v")), 2);
    assertNull(statement.getPreparedTableVersion());
    validateFallback(statement);
    assertArrayEquals(new String[] {"tag", "tag2", "v"}, statement.getMeasurements());
    assertEquals("b", statement.getNumericValues().getTag(0));
    assertEquals("a", statement.getNumericValues().getTag(1));
  }

  private void validateFallback(InsertNumericRowStatement statement) {
    new InsertNumericRow(statement, context).validateTableSchema(metadata, context);
    verify(metadata)
        .validateInsertNodeMeasurements(eq(DATABASE), any(), eq(context), eq(true), any(), any());
    assertNull(statement.getPreparedTableVersion());
  }

  private static void install(TsTable table) {
    DataNodeTableCache.getInstance().preUpdateTable(DATABASE, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(DATABASE, "t", null);
  }

  private static TSPrepareNumericRowReq request(String... names) {
    TSPrepareNumericRowReq req = new TSPrepareNumericRowReq();
    req.setTableName("t");
    req.setMeasurements(Arrays.asList(names));
    for (int i = 0; i < names.length; i++) {
      boolean tag = i < names.length - 1;
      req.addToDataTypes((tag ? TSDataType.STRING : TSDataType.DOUBLE).ordinal());
      req.addToColumnCategories((byte) (tag ? ColumnCategory.TAG : ColumnCategory.FIELD).ordinal());
    }
    return req;
  }

  private static ByteBuffer tagBuffer(int count) {
    ByteBuffer tags = ByteBuffer.allocate(count * 5);
    for (int i = 0; i < count; i++) {
      tags.putInt(1).put((byte) ('a' + i));
    }
    tags.flip();
    return tags;
  }

  private static InsertNumericRowStatement bind(PreparedNumericRowInfo info, int tags) {
    ByteBuffer values = ByteBuffer.allocate(8);
    values.putDouble(2.5D).flip();
    return info.bind(1, tagBuffer(tags), values);
  }
}
