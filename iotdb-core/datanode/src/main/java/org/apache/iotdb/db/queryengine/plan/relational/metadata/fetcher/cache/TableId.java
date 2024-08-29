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

import java.util.Objects;

public class TableId {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(TableId.class);

  // In tree model: node[1], like root.db.device.measurement -> db
  // In table model: real database
  private final String prefix;

  private final String tableName;

  public TableId(final String prefix, final String tableName) {
    this.prefix = prefix;
    this.tableName = tableName;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean belongTo(final String prefix) {
    return Objects.equals(this.prefix, prefix);
  }

  public int estimateSize() {
    // The size of table name has been computed in deviceID
    return (int) (INSTANCE_SIZE + RamUsageEstimator.sizeOf(prefix));
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
    return Objects.equals(prefix, tableId.prefix) && Objects.equals(tableName, tableId.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, tableName);
  }
}
