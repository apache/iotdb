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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;

import java.util.Objects;

public final class ViewColumnSchemaUtils {

  /**
   * Persisted key storing the underlying source/original column name of a view column.
   *
   * <p>The serialized key remains {@code __original_name} for backward compatibility with existing
   * snapshots and RPC payloads. New code that is not tree-view specific should depend on this class
   * instead of {@link TreeViewSchema} so writable view support does not look like a TreeView
   * capability.
   */
  public static final String SOURCE_NAME = "__original_name";

  private ViewColumnSchemaUtils() {
    // util class
  }

  public static String getSourceName(final TsTableColumnSchema schema) {
    return Objects.nonNull(getMappedSourceName(schema))
        ? getMappedSourceName(schema)
        : schema.getColumnName();
  }

  public static String getMappedSourceName(final TsTableColumnSchema schema) {
    return schema.getProps().get(SOURCE_NAME);
  }

  public static void setSourceName(final TsTableColumnSchema schema, final String name) {
    schema.getProps().put(SOURCE_NAME, name);
  }
}
