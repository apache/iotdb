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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.schema.table.NonCommittableTsTable;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.WritableViewInsertRewriteSupport;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.WritableViewSchema;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

public class DataNodeTableCacheTest {

  @Test
  public void testInvalidColumnPreservesWritableViewType() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_test_db";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final WritableView view = new WritableView(viewName, database, "source_table", true);
      view.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      view.putViewColumnSourceColumnMapping("value", "source_value");

      cache.preUpdateTable(database, view, null);
      cache.commitUpdateTable(database, viewName, null);
      cache.invalid(database, viewName, "value");

      final TsTable cachedTable = cache.getTable(database, viewName);
      Assert.assertTrue(cachedTable instanceof WritableView);
      Assert.assertNull(cachedTable.getColumnSchema("value"));
      Assert.assertEquals("source_table", ((WritableView) cachedTable).getSourceTableName());
      Assert.assertFalse(
          ((WritableView) cachedTable).getViewColumnToSourceColumnMap().containsKey("value"));
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testWritableViewRewriteSupportUsesColumnSourceNameWhenMapMissing() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_column_source_name_db";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable source = new TsTable(sourceName);
      source.addColumnSchema(new TagColumnSchema("source_device", TSDataType.STRING));
      source.addColumnSchema(new AttributeColumnSchema("source_attr", TSDataType.STRING));
      source.addColumnSchema(new FieldColumnSchema("source_value", TSDataType.INT32));
      cache.preUpdateTable(database, source, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView view = new WritableView(viewName, database, sourceName, false);
      view.addColumnSchema(
          withSourceName(new TagColumnSchema("device", TSDataType.STRING), "source_device"));
      view.addColumnSchema(
          withSourceName(new AttributeColumnSchema("attr", TSDataType.STRING), "source_attr"));
      view.addColumnSchema(
          withSourceName(
              new FieldColumnSchema(
                  "value", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED),
              "source_value"));
      view.setViewColumnToSourceColumnMap(null);
      cache.preUpdateTable(database, view, null);
      cache.commitUpdateTable(database, viewName, null);

      final Optional<WritableViewInsertRewriteSupport> rewriteSupport =
          new TableMetadataImpl()
              .getWritableViewInsertRewriteSupport(
                  new SessionInfo(0, "test", ZoneId.systemDefault(), database, SqlDialect.TABLE),
                  new QualifiedObjectName(database, viewName));

      Assert.assertTrue(rewriteSupport.isPresent());
      Assert.assertTrue(rewriteSupport.get().requiresSourceColumnRewrite());
      Assert.assertEquals(
          "source_device", rewriteSupport.get().resolveExistingSourceColumnName("device"));
      Assert.assertEquals(
          "source_attr", rewriteSupport.get().resolveExistingSourceColumnName("attr"));
      Assert.assertEquals(
          "source_value", rewriteSupport.get().resolveExistingSourceColumnName("value"));
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testWritableViewMetadataCacheRefreshesAfterSourceTableInvalidation() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_refresh_source_drop_db";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable source = new TsTable(sourceName);
      source.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      cache.preUpdateTable(database, source, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView view = new WritableView(viewName, database, sourceName, false);
      view.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      cache.preUpdateTable(database, view, null);
      cache.commitUpdateTable(database, viewName, null);

      final TableMetadataImpl metadata = new TableMetadataImpl();
      final SessionInfo session =
          new SessionInfo(0, "test", ZoneId.systemDefault(), database, SqlDialect.TABLE);
      final QualifiedObjectName viewObjectName = new QualifiedObjectName(database, viewName);

      final WritableViewSchema schemaBeforeDrop =
          getWritableViewSchema(metadata.getTableSchema(session, viewObjectName));
      Assert.assertTrue(schemaBeforeDrop.canUseIdentitySourceFastPath());

      cache.invalid(database, sourceName);

      final WritableViewSchema schemaAfterDrop =
          getWritableViewSchema(metadata.getTableSchema(session, viewObjectName));
      Assert.assertFalse(schemaAfterDrop.canUseIdentitySourceFastPath());
      Assert.assertFalse(schemaAfterDrop.getSourceTableSchema().isPresent());
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testGetTableUsesCommittedTableDuringActivePreUpdate() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_active_pre_update_db";
    final String tableName = "table1";

    cache.invalid(database);
    try {
      final TsTable oldTable = new TsTable(tableName);
      oldTable.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      cache.preUpdateTable(database, oldTable, null);
      Assert.assertTrue(cache.commitUpdateTable(database, tableName, null));

      final TsTable newTable = new TsTable(tableName);
      newTable.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT64));
      cache.preUpdateTable(database, newTable, null);

      final TsTable tableDuringPreUpdate = cache.getTable(database, tableName, false);
      Assert.assertEquals(
          TSDataType.INT32, tableDuringPreUpdate.getColumnSchema("value").getDataType());

      Assert.assertTrue(cache.commitUpdateTable(database, tableName, null));
      final TsTable tableAfterCommit = cache.getTable(database, tableName, false);
      Assert.assertEquals(
          TSDataType.INT64, tableAfterCommit.getColumnSchema("value").getDataType());
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testCommitUpdateTableRejectsNonCommittablePlaceholder() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_non_committable_commit_db";
    final String tableName = "table1";

    cache.invalid(database);
    try {
      cache.preUpdateTable(database, new NonCommittableTsTable(tableName), null);

      Assert.assertFalse(cache.commitUpdateTable(database, tableName, null));
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testWritableViewMetadataCacheClearsStaleEntriesAfterVersionChange() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_writable_view_metadata_eviction_db";
    final String sourceName = "source_table";
    final String firstViewName = "writable_view_1";
    final String secondViewName = "writable_view_2";

    cache.invalid(database);
    try {
      final TsTable source = new TsTable(sourceName);
      source.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      cache.preUpdateTable(database, source, null);
      Assert.assertTrue(cache.commitUpdateTable(database, sourceName, null));

      final WritableView firstView = new WritableView(firstViewName, database, sourceName, false);
      firstView.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      cache.preUpdateTable(database, firstView, null);
      Assert.assertTrue(cache.commitUpdateTable(database, firstViewName, null));

      final TableMetadataImpl metadata = new TableMetadataImpl();
      final SessionInfo session =
          new SessionInfo(0, "test", ZoneId.systemDefault(), database, SqlDialect.TABLE);
      final QualifiedObjectName firstViewObjectName =
          new QualifiedObjectName(database, firstViewName);
      Assert.assertTrue(metadata.getTableSchema(session, firstViewObjectName).isPresent());
      Assert.assertTrue(getWritableViewMetadataCacheMap(metadata).containsKey(firstViewObjectName));

      cache.invalid(database, firstViewName);
      final WritableView secondView = new WritableView(secondViewName, database, sourceName, false);
      secondView.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
      cache.preUpdateTable(database, secondView, null);
      Assert.assertTrue(cache.commitUpdateTable(database, secondViewName, null));

      final QualifiedObjectName secondViewObjectName =
          new QualifiedObjectName(database, secondViewName);
      Assert.assertTrue(metadata.getTableSchema(session, secondViewObjectName).isPresent());

      final Map<QualifiedObjectName, ?> metadataCache = getWritableViewMetadataCacheMap(metadata);
      Assert.assertFalse(metadataCache.containsKey(firstViewObjectName));
      Assert.assertTrue(metadataCache.containsKey(secondViewObjectName));
      Assert.assertEquals(1, metadataCache.size());
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testWritableViewIdentityFastPathIgnoresColumnOrder() {
    final DataNodeTableCache cache = DataNodeTableCache.getInstance();
    final String database = "cache_identity_column_order_db";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable source = new TsTable(sourceName);
      source.addColumnSchema(new TagColumnSchema("tag_a", TSDataType.STRING));
      source.addColumnSchema(new TagColumnSchema("tag_b", TSDataType.STRING));
      source.addColumnSchema(new AttributeColumnSchema("attr", TSDataType.STRING));
      source.addColumnSchema(new FieldColumnSchema("s1", TSDataType.INT32));
      source.addColumnSchema(new FieldColumnSchema("s2", TSDataType.INT64));
      cache.preUpdateTable(database, source, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView view = new WritableView(viewName, database, sourceName, false);
      view.addColumnSchema(new FieldColumnSchema("s2", TSDataType.INT64));
      view.addColumnSchema(new TagColumnSchema("tag_b", TSDataType.STRING));
      view.addColumnSchema(new FieldColumnSchema("s1", TSDataType.INT32));
      view.addColumnSchema(new AttributeColumnSchema("attr", TSDataType.STRING));
      view.addColumnSchema(new TagColumnSchema("tag_a", TSDataType.STRING));
      cache.preUpdateTable(database, view, null);
      cache.commitUpdateTable(database, viewName, null);

      final WritableViewSchema schema =
          getWritableViewSchema(
              new TableMetadataImpl()
                  .getTableSchema(
                      new SessionInfo(
                          0, "test", ZoneId.systemDefault(), database, SqlDialect.TABLE),
                      new QualifiedObjectName(database, viewName)));

      Assert.assertTrue(schema.canUseIdentitySourceFastPath());
      Assert.assertFalse(schema.getSourceTableSchema().isPresent());
      Assert.assertEquals(Integer.valueOf(0), schema.getSourceTagColumnIndexMap().get("tag_a"));
      Assert.assertEquals(Integer.valueOf(1), schema.getSourceTagColumnIndexMap().get("tag_b"));
    } finally {
      cache.invalid(database);
    }
  }

  private static <T extends TsTableColumnSchema> T withSourceName(
      final T columnSchema, final String sourceName) {
    ViewColumnSchemaUtils.setSourceName(columnSchema, sourceName);
    return columnSchema;
  }

  private static WritableViewSchema getWritableViewSchema(final Optional<TableSchema> tableSchema) {
    Assert.assertTrue(tableSchema.isPresent());
    Assert.assertTrue(tableSchema.get() instanceof WritableViewSchema);
    return (WritableViewSchema) tableSchema.get();
  }

  @SuppressWarnings("unchecked")
  private static Map<QualifiedObjectName, ?> getWritableViewMetadataCacheMap(
      final TableMetadataImpl metadata) {
    try {
      final Field field = TableMetadataImpl.class.getDeclaredField("writableViewMetadataCacheMap");
      field.setAccessible(true);
      return (Map<QualifiedObjectName, ?>) field.get(metadata);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
