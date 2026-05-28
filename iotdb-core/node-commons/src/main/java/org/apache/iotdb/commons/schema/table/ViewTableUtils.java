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

/**
 * Helpers for distinguishing the currently supported table-model view families.
 *
 * <p>Writable views and tree views both reuse {@link ViewColumnSchemaUtils} for lightweight
 * per-column source-name metadata. That shared carrier only means the column mapping shape is the
 * same; it does not imply that writable views can source from any other view family.
 */
public final class ViewTableUtils {

  public static boolean isWritableView(final TsTable table) {
    return table instanceof WritableView;
  }

  public static boolean isTreeView(final TsTable table) {
    return TreeViewSchema.isTreeViewTable(table);
  }

  public static boolean isView(final TsTable table) {
    return isWritableView(table) || isTreeView(table);
  }

  public static boolean isBaseTable(final TsTable table) {
    return !isView(table);
  }

  /**
   * Writable views currently support only base-table sources.
   *
   * <p>If future work adds writable-view-on-view support, this predicate should be relaxed together
   * with the corresponding cascade, rewrite and validation paths.
   */
  public static boolean canUseAsWritableViewSource(final TsTable table) {
    return isBaseTable(table);
  }

  private ViewTableUtils() {
    // util class
  }
}
