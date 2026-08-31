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
package com.timecho.iotdb.service;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderReq;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableDeviceLeaderResolverTest {

  private static final String VIEW_DATABASE = "view_db";
  private static final String SOURCE_DATABASE = "source_db";

  @Test
  public void resolvesNormalTableWithoutChangingDeviceLayout() {
    final TsTable table = new TsTable("source_table");
    table.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
    final ITableCache tableCache = mock(ITableCache.class);
    when(tableCache.getTable(SOURCE_DATABASE, "source_table", true)).thenReturn(table);

    final TableDeviceLeaderResolver.ResolvedDevice resolved =
        TableDeviceLeaderResolver.resolve(
            request(SOURCE_DATABASE, "SOURCE_TABLE", "device-1"), tableCache);

    Assert.assertEquals(SOURCE_DATABASE, resolved.getDatabase());
    Assert.assertArrayEquals(
        new Object[] {"source_table", "device-1"}, resolved.getDeviceId().getSegments());
  }

  @Test
  public void resolvesIdentityWritableView() {
    final TsTable source = sourceTable("source_table", "region", "device_id");
    final WritableView view = new WritableView("view", SOURCE_DATABASE, "source_table", false);
    view.addColumnSchema(new TagColumnSchema("region", TSDataType.STRING));
    view.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
    final ITableCache tableCache = tableCache(view, source);

    final TableDeviceLeaderResolver.ResolvedDevice resolved =
        TableDeviceLeaderResolver.resolve(
            request(VIEW_DATABASE, "VIEW", "north", "device-1"), tableCache);

    Assert.assertEquals(SOURCE_DATABASE, resolved.getDatabase());
    Assert.assertArrayEquals(
        new Object[] {"source_table", "north", "device-1"}, resolved.getDeviceId().getSegments());
  }

  @Test
  public void preservesAliasedAndReorderedViewTagValues() {
    final TsTable source = sourceTable("source_table", "region", "plant", "device_id");
    final WritableView view = new WritableView("view", SOURCE_DATABASE, "source_table", false);
    view.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    view.addColumnSchema(new TagColumnSchema("area", TSDataType.STRING));
    view.putViewColumnSourceColumnMapping("device", "device_id");
    view.putViewColumnSourceColumnMapping("area", "region");
    final ITableCache tableCache = tableCache(view, source);

    final TableDeviceLeaderResolver.ResolvedDevice resolved =
        TableDeviceLeaderResolver.resolve(
            request(VIEW_DATABASE, "view", "device-1", "north"), tableCache);

    Assert.assertArrayEquals(
        new Object[] {"source_table", "device-1", "north"}, resolved.getDeviceId().getSegments());
  }

  @Test
  public void resolvesViewUsingColumnSourceNameWhenMappingIsMissing() {
    final TsTable source = sourceTable("source_table", "source_region", "device_id");
    final WritableView view = new WritableView("view", SOURCE_DATABASE, "source_table", false);
    final TagColumnSchema area = new TagColumnSchema("area", TSDataType.STRING);
    ViewColumnSchemaUtils.setSourceName(area, "source_region");
    view.addColumnSchema(area);
    view.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
    view.setViewColumnToSourceColumnMap(null);
    final ITableCache tableCache = tableCache(view, source);

    final TableDeviceLeaderResolver.ResolvedDevice resolved =
        TableDeviceLeaderResolver.resolve(
            request(VIEW_DATABASE, "view", "north", "device-1"), tableCache);

    Assert.assertArrayEquals(
        new Object[] {"source_table", "north", "device-1"}, resolved.getDeviceId().getSegments());
  }

  @Test
  public void resolvesNamedViewTagsInSourceOrder() {
    final TsTable source = sourceTable("source_table", "region", "plant", "device_id");
    final WritableView view = new WritableView("view", SOURCE_DATABASE, "source_table", false);
    view.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    view.addColumnSchema(new TagColumnSchema("area", TSDataType.STRING));
    view.putViewColumnSourceColumnMapping("device", "device_id");
    view.putViewColumnSourceColumnMapping("area", "region");
    final ITableCache tableCache = tableCache(view, source);

    final TTableDeviceLeaderReq request = request(VIEW_DATABASE, "view", "device-1", "north");
    request.setTagColumnNames(java.util.Arrays.asList("device", "area"));

    final TTableDeviceLeaderReq normalized =
        TableDeviceLeaderTagResolver.resolve(request, tableCache);
    Assert.assertEquals(SOURCE_DATABASE, normalized.getDbName());
    Assert.assertEquals(
        java.util.Arrays.asList("source_table", "north", "device-1"), normalized.getDeviceId());
    Assert.assertEquals(java.util.Arrays.asList(true, true, false, true), normalized.getIsSetTag());
  }

  @Test
  public void resolvesRenamedWritableView() {
    final TsTable source = sourceTable("source_table", "device_id");
    final WritableView view = new WritableView("old_view", SOURCE_DATABASE, "source_table", false);
    view.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
    view.renameTable("renamed_view");
    final ITableCache tableCache = mock(ITableCache.class);
    when(tableCache.getTable(VIEW_DATABASE, "renamed_view", true)).thenReturn(view);
    when(tableCache.getTable(SOURCE_DATABASE, "source_table", true)).thenReturn(source);

    final TableDeviceLeaderResolver.ResolvedDevice resolved =
        TableDeviceLeaderResolver.resolve(
            request(VIEW_DATABASE, "renamed_view", "device-1"), tableCache);

    Assert.assertArrayEquals(
        new Object[] {"source_table", "device-1"}, resolved.getDeviceId().getSegments());
  }

  @Test(expected = TableDeviceLeaderResolver.InvalidRequestException.class)
  public void rejectsMalformedRequest() {
    final ITableCache tableCache = mock(ITableCache.class);
    TableDeviceLeaderResolver.resolve(
        new TTableDeviceLeaderReq(
            VIEW_DATABASE, Collections.singletonList("view"), Collections.singletonList(false), 1L),
        tableCache);
  }

  private static ITableCache tableCache(final WritableView view, final TsTable source) {
    final ITableCache tableCache = mock(ITableCache.class);
    when(tableCache.getTable(VIEW_DATABASE, "view", true)).thenReturn(view);
    when(tableCache.getTable(SOURCE_DATABASE, "source_table", true)).thenReturn(source);
    return tableCache;
  }

  private static TsTable sourceTable(final String tableName, final String... tagNames) {
    final TsTable source = new TsTable(tableName);
    for (final String tagName : tagNames) {
      source.addColumnSchema(new TagColumnSchema(tagName, TSDataType.STRING));
    }
    source.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
    return source;
  }

  private static TTableDeviceLeaderReq request(
      final String database, final String tableName, final String... values) {
    final java.util.List<String> deviceId = new java.util.ArrayList<>();
    final java.util.List<Boolean> isSetTag = new java.util.ArrayList<>();
    isSetTag.add(true);
    deviceId.add(tableName);
    for (final String value : values) {
      isSetTag.add(true);
      deviceId.add(value);
    }
    return new TTableDeviceLeaderReq(database, deviceId, isSetTag, 1L);
  }
}
