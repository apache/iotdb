/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache;

import java.util.Objects;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache.CacheMemoryControlUtil.estimateStringSize;

public class TableId {

  private final String database;

  private final String tableName;

  public TableId(String database, String tableName) {
    this.database = database;
    this.tableName = tableName;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public int estimateSize() {
    return 8 + 8 + 8 + estimateStringSize(database) + estimateStringSize(tableName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableId)) return false;
    TableId tableId = (TableId) o;
    return Objects.equals(database, tableId.database)
        && Objects.equals(tableName, tableId.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName);
  }
}
