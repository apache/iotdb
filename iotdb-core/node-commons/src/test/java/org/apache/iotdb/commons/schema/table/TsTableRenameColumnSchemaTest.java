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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
        "old_field", ViewColumnSchemaUtils.getMappedSourceName(table.getColumnSchema("new_field")));
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

  @Test
  public void testWritableViewRenameColumnSchemaUpdatesOriginalMapping() {
    final WritableView writableView =
        new WritableView("view_table", "source_db", "source_table", true);
    writableView.addColumnSchema(new FieldColumnSchema("view_col", TSDataType.DOUBLE));
    final Map<String, String> columnMapping = new HashMap<>();
    columnMapping.put("view_col", "source_col");
    writableView.setViewColumnToSourceColumnMap(columnMapping);

    writableView.renameColumnSchema("view_col", "renamed_view_col");

    Assert.assertNull(writableView.getColumnSchema("view_col"));
    Assert.assertNotNull(writableView.getColumnSchema("renamed_view_col"));
    Assert.assertEquals("source_col", writableView.getOriginalColumnName("renamed_view_col"));
    Assert.assertEquals("source_col", writableView.getMappedSourceColumnName("renamed_view_col"));
    Assert.assertFalse(writableView.getViewColumnToSourceColumnMap().containsKey("view_col"));
  }

  @Test
  public void testWritableViewLocalColumnRenameDoesNotCreateSourceMapping() {
    final WritableView writableView =
        new WritableView("view_table", "source_db", "source_table", false);
    final Map<String, String> columnMapping = new HashMap<>();
    columnMapping.put("mapped_view_col", "source_col");
    writableView.setViewColumnToSourceColumnMap(columnMapping);
    writableView.addColumnSchema(new FieldColumnSchema("local_col", TSDataType.DOUBLE));

    Assert.assertNull(writableView.getMappedSourceColumnName("local_col"));

    writableView.renameColumnSchema("local_col", "renamed_local_col");

    Assert.assertEquals("local_col", writableView.getOriginalColumnName("renamed_local_col"));
    Assert.assertNull(writableView.getMappedSourceColumnName("renamed_local_col"));
    Assert.assertFalse(writableView.getViewColumnToSourceColumnMap().containsKey("local_col"));
    Assert.assertFalse(
        writableView.getViewColumnToSourceColumnMap().containsKey("renamed_local_col"));
  }

  @Test
  public void testWritableViewExplicitSourceMappingCanBeAddedAndRemoved() {
    final WritableView writableView =
        new WritableView("view_table", "source_db", "source_table", true);
    writableView.addColumnSchema(new FieldColumnSchema("pressure", TSDataType.INT64));

    Assert.assertNull(writableView.getMappedSourceColumnName("pressure"));

    writableView.putViewColumnSourceColumnMapping("pressure", "pressure");
    Assert.assertEquals("pressure", writableView.getMappedSourceColumnName("pressure"));

    writableView.removeViewColumnSourceColumnMapping("pressure");
    Assert.assertNull(writableView.getMappedSourceColumnName("pressure"));
  }

  @Test
  public void testWritableViewRemoveColumnSchemaRemovesSourceMapping() {
    final WritableView writableView =
        new WritableView("view_table", "source_db", "source_table", true);
    writableView.addColumnSchema(new FieldColumnSchema("view_col", TSDataType.DOUBLE));
    writableView.putViewColumnSourceColumnMapping("view_col", "source_col");

    writableView.removeColumnSchema("view_col");

    Assert.assertNull(writableView.getColumnSchema("view_col"));
    Assert.assertNull(writableView.getMappedSourceColumnName("view_col"));
    Assert.assertFalse(writableView.getViewColumnToSourceColumnMap().containsKey("view_col"));
  }

  @Test
  public void testWritableViewSerializeDeserializeKeepsOriginalMapping() throws IOException {
    final WritableView writableView =
        new WritableView("view_table", "source_db", "source_table", true);
    writableView.addColumnSchema(new FieldColumnSchema("view_col", TSDataType.DOUBLE));
    final Map<String, String> columnMapping = new HashMap<>();
    columnMapping.put("view_col", "source_col");
    writableView.setViewColumnToSourceColumnMap(columnMapping);

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    writableView.serialize(outputStream);

    final WritableView restored =
        (WritableView) TsTable.deserialize(ByteBuffer.wrap(outputStream.toByteArray()));

    Assert.assertEquals("source_col", restored.getOriginalColumnName("view_col"));
    Assert.assertEquals(
        writableView.getViewColumnToSourceColumnMap(), restored.getViewColumnToSourceColumnMap());
  }
}
