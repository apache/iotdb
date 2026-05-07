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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;

import org.apache.tsfile.enums.TSDataType;

import java.util.Map;
import java.util.Set;

public final class TableColumnMetadataUtil {

  public static final String USING_STATUS = "USING";
  public static final String PRE_DELETE_STATUS = "PRE_DELETE";
  public static final String PRE_ALTER_STATUS = "PRE_ALTER";

  private TableColumnMetadataUtil() {
    // Utility class
  }

  public static String getColumnStatus(
      final String columnName,
      final Set<String> preDeletedColumns,
      final Map<String, ?> preAlteredColumns) {
    if (preDeletedColumns != null && preDeletedColumns.contains(columnName)) {
      return PRE_DELETE_STATUS;
    }
    if (preAlteredColumns != null && preAlteredColumns.containsKey(columnName)) {
      return PRE_ALTER_STATUS;
    }
    return USING_STATUS;
  }

  public static String getColumnDataTypeName(
      final TsTableColumnSchema columnSchema, final Map<String, Byte> preAlteredColumns) {
    if (preAlteredColumns != null) {
      final Byte serializedType = preAlteredColumns.get(columnSchema.getColumnName());
      if (serializedType != null) {
        return TSDataType.deserialize(serializedType).name();
      }
    }
    return columnSchema.getDataType().name();
  }
}
