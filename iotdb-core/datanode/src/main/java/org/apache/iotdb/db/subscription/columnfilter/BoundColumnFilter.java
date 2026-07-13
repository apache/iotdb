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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BoundColumnFilter {

  private static final BoundColumnFilter MATCH_ALL = new BoundColumnFilter(true, false, null, null);

  private final boolean matchAll;
  private final boolean timeSelected;
  private final Map<TableKey, Set<String>> selectedColumnsByTable;
  private final Map<TableKey, Boolean> timeSelectedByTable;

  private BoundColumnFilter(
      final boolean matchAll,
      final boolean timeSelected,
      final Map<TableKey, Set<String>> selectedColumnsByTable,
      final Map<TableKey, Boolean> timeSelectedByTable) {
    this.matchAll = matchAll;
    this.timeSelected = timeSelected;
    this.selectedColumnsByTable = selectedColumnsByTable;
    this.timeSelectedByTable = timeSelectedByTable;
  }

  public static BoundColumnFilter matchAll() {
    return MATCH_ALL;
  }

  public static BoundColumnFilter of(
      final Map<TableKey, Set<String>> selectedColumnsByTable,
      final boolean timeSelected,
      final Map<TableKey, Boolean> timeSelectedByTable) {
    final Map<TableKey, Set<String>> copied = new HashMap<>();
    selectedColumnsByTable.forEach(
        (key, value) -> copied.put(key, Collections.unmodifiableSet(new HashSet<>(value))));
    return new BoundColumnFilter(
        false,
        timeSelected,
        Collections.unmodifiableMap(copied),
        Objects.nonNull(timeSelectedByTable)
            ? Collections.unmodifiableMap(new HashMap<>(timeSelectedByTable))
            : Collections.emptyMap());
  }

  public boolean isMatchAll() {
    return matchAll;
  }

  public boolean isTimeSelected() {
    return timeSelected;
  }

  public boolean isTimeSelected(final String databaseName, final String tableName) {
    if (matchAll) {
      return true;
    }
    return Boolean.TRUE.equals(timeSelectedByTable.get(TableKey.of(databaseName, tableName)));
  }

  public boolean match(final String databaseName, final String tableName, final String columnName) {
    if (matchAll) {
      return true;
    }
    final Set<String> selectedColumns =
        selectedColumnsByTable.get(TableKey.of(databaseName, tableName));
    return Objects.nonNull(selectedColumns) && selectedColumns.contains(normalize(columnName));
  }

  public Map<TableKey, Set<String>> getSelectedColumnsByTable() {
    return selectedColumnsByTable;
  }

  public Map<TableKey, Boolean> getTimeSelectedByTable() {
    return timeSelectedByTable;
  }

  static String normalize(final String value) {
    return Objects.nonNull(value) ? value.trim().toLowerCase(Locale.ROOT) : "";
  }

  public static final class TableKey {

    private final String databaseName;
    private final String tableName;

    private TableKey(final String databaseName, final String tableName) {
      this.databaseName = normalize(databaseName);
      this.tableName = normalize(tableName);
    }

    public static TableKey of(final String databaseName, final String tableName) {
      return new TableKey(databaseName, tableName);
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TableKey)) {
        return false;
      }
      final TableKey tableKey = (TableKey) o;
      return Objects.equals(databaseName, tableKey.databaseName)
          && Objects.equals(tableName, tableKey.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(databaseName, tableName);
    }
  }
}
