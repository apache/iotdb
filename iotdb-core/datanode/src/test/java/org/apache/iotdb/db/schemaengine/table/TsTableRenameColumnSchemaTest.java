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

import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class TsTableRenameColumnSchemaTest {

  @Test
  public void testRenameColumnSchemaMovesMapKeyAndKeepsIdempotency() {
    final TsTable table = new TsTable("test_table");
    table.addColumnSchema(new FieldColumnSchema("old_field", TSDataType.DOUBLE));

    table.renameColumnSchema("old_field", "new_field");
    table.renameColumnSchema("old_field", "new_field");

    Assert.assertNull(table.getColumnSchema("old_field"));
    Assert.assertNotNull(table.getColumnSchema("new_field"));
    Assert.assertEquals("new_field", table.getColumnSchema("new_field").getColumnName());
    Assert.assertEquals(
        "old_field", TreeViewSchema.getOriginalName(table.getColumnSchema("new_field")));
  }

  @Test
  public void testRenameTagColumnKeepsTagOrdinal() {
    final TsTable table = new TsTable("test_table");
    table.addColumnSchema(new TagColumnSchema("tag_1", TSDataType.STRING));
    table.addColumnSchema(new TagColumnSchema("tag_2", TSDataType.STRING));

    table.renameColumnSchema("tag_1", "renamed_tag");

    Assert.assertEquals(-1, table.getTagColumnOrdinal("tag_1"));
    Assert.assertEquals(0, table.getTagColumnOrdinal("renamed_tag"));
    Assert.assertEquals(1, table.getTagColumnOrdinal("tag_2"));
  }
}
