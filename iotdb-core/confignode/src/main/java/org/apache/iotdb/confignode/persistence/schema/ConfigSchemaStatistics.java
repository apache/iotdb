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

package org.apache.iotdb.confignode.persistence.schema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link ConfigSchemaStatistics} is used to:
 *
 * <p>1. Serve the {@link org.apache.iotdb.confignode.manager.partition.PartitionMetrics} for schema
 * related metrics, to prevent too much scanning and lock acquisition, and to simplify the metric
 * getter logic.
 *
 * <p>2. Be reserved for potential mem-control and quota functionalities expansion.
 *
 * <p>3. Be in consistency with SchemaRegionMemMetric in dataNode.
 */
public class ConfigSchemaStatistics {
  public AtomicLong treeDatabaseNum = new AtomicLong(0);

  // Add 1 for information_schema
  public AtomicLong tableDatabaseNum = new AtomicLong(1);

  public ConcurrentMap<String, Long> treeViewTableNum = new ConcurrentHashMap<>();
  public ConcurrentMap<String, Long> baseTableNum = new ConcurrentHashMap<>();

  // Getter

  public long getTreeDatabaseNum() {
    return treeDatabaseNum.get();
  }

  public long getTableDatabaseNum() {
    return tableDatabaseNum.get();
  }

  public long getTreeViewTableNum(final String database) {
    return treeViewTableNum.getOrDefault(database, 0L);
  }

  public long getBaseTableNum(final String database) {
    return baseTableNum.getOrDefault(database, 0L);
  }

  // Setter

  public void increaseTreeDatabaseNum() {
    treeDatabaseNum.incrementAndGet();
  }

  public void decreaseTreeDatabaseNum() {
    treeDatabaseNum.decrementAndGet();
  }

  public void increaseTableDatabaseNum() {
    tableDatabaseNum.incrementAndGet();
  }

  public void decreaseTableDatabaseNum() {
    tableDatabaseNum.decrementAndGet();
  }

  public void increaseTreeViewTableNum(final String database) {
    treeViewTableNum.compute(database, (db, num) -> num == null ? 0 : num + 1);
  }

  public void decreaseTreeViewTableNum(final String database) {
    treeViewTableNum.compute(database, (db, num) -> num != null && num > 1 ? num - 1 : null);
  }

  public void increaseBaseTableNum(final String database) {
    baseTableNum.compute(database, (db, num) -> num == null ? 0 : num + 1);
  }

  public void decreaseBaseTableNum(final String database) {
    baseTableNum.compute(database, (db, num) -> num != null && num > 1 ? num - 1 : null);
  }
}
