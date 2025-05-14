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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Objects;

public class TreeViewSchema {
  public static final String ORIGINAL_NAME = "__original_name";
  public static final String TREE_PATH_PATTERN = "__tree_path_pattern";
  public static final String RESTRICT = "__restrict";

  public static boolean isTreeViewTable(final TsTable table) {
    return table.getPropValue(TREE_PATH_PATTERN).isPresent();
  }

  public static PartialPath getPrefixPattern(final TsTable table) {
    return table
        .getPropValue(TreeViewSchema.TREE_PATH_PATTERN)
        .map(TreeViewSchema::forceSeparateStringToPartialPath)
        .orElseThrow(
            () ->
                new IoTDBRuntimeException(
                    String.format(
                        "Failed to get the original database, because the %s is null for table %s",
                        TreeViewSchema.TREE_PATH_PATTERN, table.getTableName()),
                    TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
  }

  public static PartialPath forceSeparateStringToPartialPath(final String string) {
    final PartialPath partialPath;
    try {
      partialPath = new PartialPath(string);
    } catch (final IllegalPathException e) {
      throw new IoTDBRuntimeException(
          String.format(
              "Failed to parse the tree view string %s when convert to IDeviceID", string),
          TSStatusCode.SEMANTIC_ERROR.getStatusCode());
    }
    return partialPath;
  }

  public static String getSourceName(final TsTableColumnSchema schema) {
    return Objects.nonNull(TreeViewSchema.getOriginalName(schema))
        ? TreeViewSchema.getOriginalName(schema)
        : schema.getColumnName();
  }

  public static String getOriginalName(final TsTableColumnSchema schema) {
    return schema.getProps().get(ORIGINAL_NAME);
  }

  public static void setOriginalName(final TsTableColumnSchema schema, final String name) {
    schema.getProps().put(ORIGINAL_NAME, name);
  }

  public static boolean isRestrict(final TsTable table) {
    return table.getPropValue(RESTRICT).isPresent();
  }

  public static void setRestrict(final TsTable table) {
    table.addProp(RESTRICT, "");
  }

  public static String setPathPattern(final TsTable table, final PartialPath pathPattern) {
    final String[] nodes = pathPattern.getNodes();
    if (!PathPatternUtil.isMultiLevelMatchWildcard(nodes[nodes.length - 1])) {
      return "The last node must be '**'";
    }
    for (int i = nodes.length - 2; i >= 0; --i) {
      if (PathPatternUtil.hasWildcard(nodes[i])) {
        return "The wildCard is not permitted to set before the last node";
      }
    }
    table.addProp(TREE_PATH_PATTERN, pathPattern.toString());
    return null;
  }

  private TreeViewSchema() {
    // Private constructor
  }
}
