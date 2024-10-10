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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Objects;

class TableId {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(TableId.class);

  // In tree model: null
  // In table model: real database
  private final String database;

  private final String tableName;

  TableId(final @Nullable String database, final @Nonnull String tableName) {
    this.database = database;
    this.tableName = tableName;
  }

  String getDatabase() {
    return database;
  }

  String getTableName() {
    return tableName;
  }

  boolean belongTo(final String database) {
    return Objects.equals(this.database, database);
  }

  int estimateSize() {
    // The size of table name has been computed in deviceID
    return (int) (INSTANCE_SIZE + RamUsageEstimator.sizeOf(database));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableId)) {
      return false;
    }
    final TableId tableId = (TableId) o;
    return Objects.equals(database, tableId.database)
        && Objects.equals(tableName, tableId.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName);
  }
}
