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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class ShowCreateViewTaskTest {

  @Test
  public void testShowCreateWritableViewQuotesSourceTableName() {
    final WritableView view = new WritableView("target_view", "test", "select", true);
    final Map<String, String> columnMap = new LinkedHashMap<>();
    columnMap.put("time", "time");
    columnMap.put("area", "region_id");
    columnMap.put("label", "model");
    view.setViewColumnToSourceColumnMap(columnMap);
    final TimeColumnSchema timeColumnSchema = new TimeColumnSchema("time", TSDataType.TIMESTAMP);
    timeColumnSchema.getProps().put(TsTable.COMMENT_KEY, "time_comment");
    view.addColumnSchema(timeColumnSchema);
    final TagColumnSchema areaColumnSchema = new TagColumnSchema("area", TSDataType.STRING);
    areaColumnSchema.getProps().put(TsTable.COMMENT_KEY, "area_comment");
    view.addColumnSchema(areaColumnSchema);
    view.addColumnSchema(new AttributeColumnSchema("label", TSDataType.STRING));
    view.addProp(TsTable.COMMENT_KEY, "view_comment");

    final String sql = ShowCreateViewTask.getShowCreateWritableViewSQL(view);

    Assert.assertTrue(sql.contains("FROM \"select\""));
    Assert.assertTrue(sql.contains("\"region_id\" AS \"area\""));
    Assert.assertTrue(sql.contains("\"model\" AS \"label\""));
    Assert.assertTrue(sql.contains("\"time\" AS \"time\" COMMENT 'time_comment'"));
    Assert.assertTrue(sql.contains("\"region_id\" AS \"area\" COMMENT 'area_comment'"));
    Assert.assertTrue(sql.contains("COMMENT 'view_comment'"));
  }

  @Test
  public void testShowCreateWritableViewUsesLocalOriginalNameForUnmappedRenamedColumn() {
    final WritableView view = new WritableView("target_view", "test", "source_table", false);
    final Map<String, String> columnMap = new LinkedHashMap<>();
    columnMap.put("time", "time");
    view.setViewColumnToSourceColumnMap(columnMap);
    view.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    view.addColumnSchema(new FieldColumnSchema("pressure", TSDataType.DOUBLE));
    view.renameColumnSchema("pressure", "pressure_alias");

    final String sql = ShowCreateViewTask.getShowCreateWritableViewSQL(view);

    Assert.assertTrue(sql.contains("\"pressure\" AS \"pressure_alias\""));
    Assert.assertFalse(sql.contains("\"source_table\" AS \"pressure_alias\""));
  }
}
