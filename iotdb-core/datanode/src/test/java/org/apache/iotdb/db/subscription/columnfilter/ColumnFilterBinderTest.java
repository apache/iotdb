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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ColumnFilterBinderTest {

  @Test
  public void testStreamingFilterKeepsNewTagsAtRuntime() {
    final TsTable table = createTableSchema();
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(
            createTableTopicConfig("column_name = \"temperature\""), (db, name) -> table);
    final ColumnFilterMatcher compiled = matcher.forTable("db", "sensors");

    final Tablet prunedTablet =
        TabletColumnPruner.pruneTableModelTablet(createRuntimeTabletWithNewTag(), "db", matcher);

    Assert.assertNotNull(prunedTablet);
    Assert.assertEquals(3, prunedTablet.getSchemas().size());
    Assert.assertEquals("device", prunedTablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("new_tag", prunedTablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals("temperature", prunedTablet.getSchemas().get(2).getMeasurementName());
    Assert.assertSame(compiled, matcher.forTable("db", "sensors"));
  }

  @Test
  public void testAttributeMatchBindsTagsButNotAttribute() {
    final BoundColumnFilter boundFilter =
        new ColumnFilterBinder()
            .bind(
                createTableTopicConfig("category = \"ATTRIBUTE\""),
                Collections.singletonMap(
                    "db", Collections.singletonMap("sensors", createTableSchema())));
    final ColumnFilterMatcher matcher = ColumnFilterMatcher.fromBoundColumnFilter(boundFilter);

    final Tablet prunedTablet =
        TabletColumnPruner.pruneTableModelTablet(createRuntimeTabletWithNewTag(), "db", matcher);

    Assert.assertNotNull(prunedTablet);
    Assert.assertEquals(2, prunedTablet.getSchemas().size());
    Assert.assertEquals("device", prunedTablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("new_tag", prunedTablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, prunedTablet.getColumnTypes().get(0));
  }

  @Test
  public void testStreamingFilterRefreshesChangedSchemaAndNewTables() {
    final Map<String, TsTable> tables = new HashMap<>();
    final TsTable initialTable = createTableSchema();
    tables.put("sensors", initialTable);
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(
            createTableTopicConfig("category = \"FIELD\" AND datatype = \"DOUBLE\""),
            (db, name) -> tables.get(name));
    final ColumnFilterMatcher initialPlan = matcher.forTable("db", "sensors");
    Assert.assertTrue(initialPlan.match("db", "sensors", "temperature"));
    Assert.assertFalse(initialPlan.match("db", "sensors", "new_field"));
    Assert.assertSame(initialPlan, matcher.forTable("db", "sensors"));

    final TsTable updatedTable = new TsTable(initialTable);
    updatedTable.addColumnSchema(new FieldColumnSchema("new_field", TSDataType.DOUBLE));
    updatedTable.addColumnSchema(new TagColumnSchema("new_tag", TSDataType.STRING));
    tables.put("sensors", updatedTable);
    final ColumnFilterMatcher updatedPlan = matcher.forTable("db", "sensors");
    Assert.assertNotSame(initialPlan, updatedPlan);
    Assert.assertTrue(updatedPlan.match("db", "sensors", "new_field"));
    Assert.assertTrue(updatedPlan.match("db", "sensors", "new_tag"));
    Assert.assertFalse(updatedPlan.match("db", "sensors", "status"));

    Assert.assertFalse(matcher.match("db", "events", "temperature"));
    tables.put("events", createTableSchema("events", "event_time"));
    Assert.assertTrue(matcher.match("db", "events", "temperature"));
    tables.remove("sensors");
    Assert.assertFalse(matcher.match("db", "sensors", "temperature"));
  }

  @Test
  public void testStreamingFilterUsesLatestDatatypeAndViewAlias() {
    final AtomicReference<TsTable> schema = new AtomicReference<>(createTreeViewSchema());
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(
            createTableTopicConfig("column_name = \"temp_alias\" AND datatype = \"INT64\""),
            (db, name) -> schema.get());
    Assert.assertFalse(matcher.match("db", "sensors_view", "temperature"));
    final TsTable changed = createTreeViewSchema();
    final FieldColumnSchema field = new FieldColumnSchema("temp_alias", TSDataType.INT64);
    TreeViewSchema.setOriginalName(field, "temperature");
    changed.removeColumnSchema("temp_alias");
    changed.addColumnSchema(field);
    schema.set(changed);
    Assert.assertTrue(matcher.match("db", "sensors_view", "temperature"));
    Assert.assertFalse(matcher.match("db", "sensors_view", "temp_alias"));
    Assert.assertTrue(matcher.match("db", "sensors_view", "device_alias"));
  }

  @Test
  public void testStreamingFilterKeepsTimeSelectionPerTable() {
    final Map<String, TsTable> tables = new HashMap<>();
    tables.put("sensors", createTableSchema());
    tables.put("events", createTableSchema("events", "event_time"));
    final TopicConfig config = createTableTopicConfig("column_name = \"time\"");
    config.getAttribute().put(TopicConstant.DATABASE_KEY, "db");
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(config, (db, name) -> tables.get(name));
    Assert.assertTrue(matcher.isTimeSelected("db", "sensors"));
    Assert.assertFalse(matcher.isTimeSelected("db", "events"));
    Assert.assertFalse(matcher.isTimeSelected("other", "sensors"));
    Assert.assertTrue(matcher.isTimeSelected());
    Assert.assertEquals(
        Map.of("sensors", true, "events", false), matcher.getTimeSelectedByTable("db").get("db"));
    Assert.assertTrue(matcher.getTimeSelectedByTable("other").isEmpty());
  }

  @Test
  public void testBindTimeSelectionPerTable() {
    final Map<String, TsTable> tables = new HashMap<>();
    tables.put("sensors", createTableSchema("sensors", "time"));
    tables.put("events", createTableSchema("events", "event_time"));
    final BoundColumnFilter boundFilter =
        new ColumnFilterBinder()
            .bind(
                createTableTopicConfig("column_name = \"time\""),
                Collections.singletonMap("db", tables));
    final ColumnFilterMatcher matcher = ColumnFilterMatcher.fromBoundColumnFilter(boundFilter);

    Assert.assertTrue(boundFilter.isTimeSelected());
    Assert.assertTrue(matcher.isTimeSelected("db", "sensors"));
    Assert.assertFalse(matcher.isTimeSelected("db", "events"));
    Assert.assertFalse(matcher.match("db", "new_sensors", "time"));
    Assert.assertEquals(
        Boolean.TRUE, matcher.getTimeSelectedByTable("db").get("db").get("sensors"));
    Assert.assertEquals(
        Boolean.FALSE, matcher.getTimeSelectedByTable("db").get("db").get("events"));
    Assert.assertTrue(matcher.getTimeSelectedByTable("db2").isEmpty());
  }

  @Test
  public void testBindTreeViewUsesViewColumnsAndSelectsSourceColumns() {
    final BoundColumnFilter boundFilter =
        new ColumnFilterBinder()
            .bind(
                createTableTopicConfig("column_name = \"temp_alias\""),
                Collections.singletonMap(
                    "db", Collections.singletonMap("sensors_view", createTreeViewSchema())));
    final ColumnFilterMatcher matcher = ColumnFilterMatcher.fromBoundColumnFilter(boundFilter);

    Assert.assertTrue(matcher.match("db", "sensors_view", "temperature"));
    Assert.assertFalse(matcher.match("db", "sensors_view", "temp_alias"));
    Assert.assertTrue(matcher.match("db", "sensors_view", "device_alias"));
    Assert.assertFalse(matcher.match("db", "sensors_view", "device"));

    final Tablet prunedTablet =
        TabletColumnPruner.pruneTableModelTablet(createRuntimeViewTablet(), "db", matcher);

    Assert.assertNotNull(prunedTablet);
    Assert.assertEquals(2, prunedTablet.getSchemas().size());
    Assert.assertEquals("device_alias", prunedTablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("temperature", prunedTablet.getSchemas().get(1).getMeasurementName());
  }

  @Test
  public void testProjectTreeTabletToTreeViewTabletBeforePruning() {
    final TsTable viewSchema = createTreeViewSchema();
    final Tablet projectedTablet =
        new TreeViewTabletProjector("db", viewSchema).project(createTreeTabletForView());

    Assert.assertNotNull(projectedTablet);
    Assert.assertEquals("sensors_view", projectedTablet.getTableName());
    Assert.assertEquals(3, projectedTablet.getSchemas().size());
    Assert.assertEquals("device_alias", projectedTablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("temperature", projectedTablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals("status", projectedTablet.getSchemas().get(2).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, projectedTablet.getColumnTypes().get(0));
    Assert.assertEquals(ColumnCategory.FIELD, projectedTablet.getColumnTypes().get(1));
    Assert.assertEquals(ColumnCategory.FIELD, projectedTablet.getColumnTypes().get(2));

    final BoundColumnFilter boundFilter =
        new ColumnFilterBinder()
            .bind(
                createTableTopicConfig("column_name = \"temp_alias\""),
                Collections.singletonMap(
                    "db", Collections.singletonMap("sensors_view", viewSchema)));
    final Tablet prunedTablet =
        TabletColumnPruner.pruneTableModelTablet(
            projectedTablet, "db", ColumnFilterMatcher.fromBoundColumnFilter(boundFilter));

    Assert.assertNotNull(prunedTablet);
    Assert.assertEquals(2, prunedTablet.getSchemas().size());
    Assert.assertEquals("device_alias", prunedTablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("temperature", prunedTablet.getSchemas().get(1).getMeasurementName());
  }

  @Test
  public void testProjectTreeTabletSkipsDeviceOutsideTreeViewPattern() {
    final Tablet projectedTablet =
        new TreeViewTabletProjector("db", createTreeViewSchema())
            .project(createTreeTablet("root.other.d1"));

    Assert.assertNull(projectedTablet);
  }

  @Test
  public void testProjectTreeTabletMarksMissingTreeViewTagAsNull() {
    final Tablet projectedTablet =
        new TreeViewTabletProjector("db", createTreeViewSchemaWithExtraTag())
            .project(createTreeTabletForView());

    Assert.assertNotNull(projectedTablet);
    Assert.assertEquals(4, projectedTablet.getSchemas().size());
    Assert.assertEquals("device_alias", projectedTablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("sensor_alias", projectedTablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, projectedTablet.getColumnTypes().get(1));
    Assert.assertNotNull(projectedTablet.getValues()[1]);

    final BitMap[] bitMaps = projectedTablet.getBitMaps();
    Assert.assertNotNull(bitMaps);
    Assert.assertNull(bitMaps[0]);
    Assert.assertNotNull(bitMaps[1]);
    Assert.assertTrue(bitMaps[1].isMarked(0));
  }

  @Test
  public void testRuntimeExpressionTimeSelectionUsesTimestampDatatype() {
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(createTableTopicConfig("datatype = \"TIMESTAMP\""));

    Assert.assertTrue(matcher.isTimeSelected());
    Assert.assertTrue(matcher.isTimeSelected("db", "sensors"));
  }

  @Test
  public void testRuntimeExpressionGlobalTimeSelectionCanBeFalse() {
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(
            createTableTopicConfig("column_name = \"temperature\""));

    Assert.assertFalse(matcher.isTimeSelected());
    Assert.assertFalse(matcher.isTimeSelected("db", "sensors"));
  }

  private static TopicConfig createTableTopicConfig(final String columnFilter) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("__system.sql-dialect", "table");
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
    return new TopicConfig(attributes);
  }

  private static TsTable createTableSchema() {
    return createTableSchema("sensors", "time");
  }

  private static TsTable createTableSchema(final String tableName, final String timeColumnName) {
    final TsTable table = new TsTable(tableName);
    table.addColumnSchema(new TimeColumnSchema(timeColumnName, TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("site", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.DOUBLE));
    table.addColumnSchema(new FieldColumnSchema("status", TSDataType.STRING));
    return table;
  }

  private static TsTable createTreeViewSchema() {
    final TsTable table = new TsTable("sensors_view");
    table.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.test.**");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    final TagColumnSchema tagColumnSchema = new TagColumnSchema("device_alias", TSDataType.STRING);
    table.addColumnSchema(tagColumnSchema);
    table.addColumnSchema(new AttributeColumnSchema("site", TSDataType.STRING));
    final FieldColumnSchema temperature = new FieldColumnSchema("temp_alias", TSDataType.DOUBLE);
    TreeViewSchema.setOriginalName(temperature, "temperature");
    table.addColumnSchema(temperature);
    final FieldColumnSchema status = new FieldColumnSchema("status_alias", TSDataType.STRING);
    TreeViewSchema.setOriginalName(status, "status");
    table.addColumnSchema(status);
    return table;
  }

  private static TsTable createTreeViewSchemaWithExtraTag() {
    final TsTable table = new TsTable("sensors_view");
    table.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.test.**");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("device_alias", TSDataType.STRING));
    table.addColumnSchema(new TagColumnSchema("sensor_alias", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("site", TSDataType.STRING));
    final FieldColumnSchema temperature = new FieldColumnSchema("temp_alias", TSDataType.DOUBLE);
    TreeViewSchema.setOriginalName(temperature, "temperature");
    table.addColumnSchema(temperature);
    final FieldColumnSchema status = new FieldColumnSchema("status_alias", TSDataType.STRING);
    TreeViewSchema.setOriginalName(status, "status");
    table.addColumnSchema(status);
    return table;
  }

  private static Tablet createRuntimeTabletWithNewTag() {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "new_tag", "site", "temperature", "status"),
            Arrays.asList(
                TSDataType.STRING,
                TSDataType.STRING,
                TSDataType.STRING,
                TSDataType.DOUBLE,
                TSDataType.STRING),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.ATTRIBUTE,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, "new");
    tablet.addValue(0, 2, "north");
    tablet.addValue(0, 3, 36.5);
    tablet.addValue(0, 4, "ok");
    tablet.setRowSize(1);
    return tablet;
  }

  private static Tablet createTreeTabletForView() {
    return createTreeTablet("root.test.d1");
  }

  private static Tablet createTreeTablet(final String deviceId) {
    final Tablet tablet =
        new Tablet(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            Arrays.asList("temperature", "status"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.STRING),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, 36.5);
    tablet.addValue(0, 1, "ok");
    tablet.setRowSize(1);
    return tablet;
  }

  private static Tablet createRuntimeViewTablet() {
    final Tablet tablet =
        new Tablet(
            "sensors_view",
            Arrays.asList("device_alias", "site", "temperature", "status"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.STRING),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.ATTRIBUTE,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, "north");
    tablet.addValue(0, 2, 36.5);
    tablet.addValue(0, 3, "ok");
    tablet.setRowSize(1);
    return tablet;
  }
}
