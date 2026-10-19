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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class ShowCreateViewTaskTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> paths() {
    return Arrays.asList(
        new Object[][] {
          {"root.demo_tick.ab.**", "root.\"demo_tick\".\"ab\".**"},
          {"root.demo_tick.`a``b`.**", "root.\"demo_tick\".\"`a``b`\".**"},
          {"root.`demo``tick`.`a````b`.**", "root.\"`demo``tick`\".\"`a````b`\".**"},
          {"root.demo_tick.`a b`.**", "root.\"demo_tick\".\"`a b`\".**"},
          {"root.demo_tick.`a.b`.**", "root.\"demo_tick\".\"`a.b`\".**"},
          {"root.demo_tick.`a\"b`.**", "root.\"demo_tick\".\"`a\"\"b`\".**"},
          {"root.demo_tick.`a``\"b`.**", "root.\"demo_tick\".\"`a``\"\"b`\".**"},
          {"root.demo_tick.`*`.**", "root.\"demo_tick\".\"`*`\".**"},
          {"root.demo_tick.`1`.**", "root.\"demo_tick\".\"`1`\".**"}
        });
  }

  private final String treePath;
  private final String sqlPath;

  public ShowCreateViewTaskTest(final String treePath, final String sqlPath) {
    this.treePath = treePath;
    this.sqlPath = sqlPath;
  }

  @Test
  public void testShowCreateViewPreservesPrefixPath() throws Exception {
    final PartialPath prefixPath = new PartialPath(treePath);
    final TsTable table = new TsTable("v");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    final FieldColumnSchema field = new FieldColumnSchema("value", TSDataType.INT32);
    TreeViewSchema.setOriginalName(field, "s1");
    table.addColumnSchema(field);
    assertNull(TreeViewSchema.setPathPattern(table, prefixPath));

    final String sql = ShowCreateViewTask.getShowCreateViewSQL(table);
    assertEquals(
        "CREATE VIEW \"v\" (\"time\" TIMESTAMP TIME,\"device\" STRING TAG,"
            + "\"value\" INT32 FIELD FROM \"s1\") WITH (ttl='INF') AS "
            + sqlPath,
        sql);
    final CreateView recreatedView =
        (CreateView) new SqlParser().createStatement(sql, ZoneId.systemDefault(), null);
    assertEquals(prefixPath, recreatedView.getPrefixPath());
  }
}
