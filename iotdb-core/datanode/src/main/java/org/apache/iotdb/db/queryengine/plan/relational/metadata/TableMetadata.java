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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class TableMetadata {
  private final String table;
  private final Optional<String> comment;
  private final List<ColumnMetadata> columns;
  private final Map<String, Object> properties;

  public TableMetadata(String table, List<ColumnMetadata> columns) {
    this(table, columns, emptyMap());
  }

  public TableMetadata(String table, List<ColumnMetadata> columns, Map<String, Object> properties) {
    this(table, columns, properties, Optional.empty());
  }

  public TableMetadata(
      String table,
      List<ColumnMetadata> columns,
      Map<String, Object> properties,
      Optional<String> comment) {
    requireNonNull(table, "table is null");
    requireNonNull(columns, "columns is null");
    requireNonNull(comment, "comment is null");

    this.table = table;
    this.columns = new ArrayList<>(columns);
    this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    this.comment = comment;
  }

  public String getTable() {
    return table;
  }

  public List<ColumnMetadata> getColumns() {
    return columns;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Optional<String> getComment() {
    return comment;
  }

  public TableSchema getTableSchema() {
    return new TableSchema(
        table, columns.stream().map(ColumnMetadata::getColumnSchema).collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ConnectorTableMetadata{");
    sb.append("table=").append(table);
    sb.append(", columns=").append(columns);
    sb.append(", properties=").append(properties);
    comment.ifPresent(value -> sb.append(", comment='").append(value).append("'"));
    sb.append('}');
    return sb.toString();
  }
}
