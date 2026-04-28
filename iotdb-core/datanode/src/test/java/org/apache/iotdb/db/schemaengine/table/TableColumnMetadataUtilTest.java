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

import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TableColumnMetadataUtilTest {

  @Test
  public void testPreAlteredColumnUsesPendingTypeAndStatus() {
    final FieldColumnSchema columnSchema = new FieldColumnSchema("s1", TSDataType.INT32);
    final Map<String, Byte> preAlteredColumns = new HashMap<>();
    preAlteredColumns.put("s1", TSDataType.INT64.serialize());

    assertEquals(
        TableColumnMetadataUtil.PRE_ALTER_STATUS,
        TableColumnMetadataUtil.getColumnStatus("s1", Collections.emptySet(), preAlteredColumns));
    assertEquals(
        TSDataType.INT64.name(),
        TableColumnMetadataUtil.getColumnDataTypeName(columnSchema, preAlteredColumns));
  }

  @Test
  public void testPreDeleteStatusTakesPriority() {
    final Set<String> preDeletedColumns = Collections.singleton("s1");
    final Map<String, Byte> preAlteredColumns = new HashMap<>();
    preAlteredColumns.put("s1", TSDataType.FLOAT.serialize());

    assertEquals(
        TableColumnMetadataUtil.PRE_DELETE_STATUS,
        TableColumnMetadataUtil.getColumnStatus("s1", preDeletedColumns, preAlteredColumns));
  }
}
