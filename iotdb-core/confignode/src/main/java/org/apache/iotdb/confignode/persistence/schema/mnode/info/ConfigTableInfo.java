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

package org.apache.iotdb.confignode.persistence.schema.mnode.info;

import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.BasicMNodeInfo;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashSet;
import java.util.Set;

public class ConfigTableInfo extends BasicMNodeInfo {

  private static final int SET_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(HashSet.class);
  private TsTable table;

  private TableNodeStatus status;

  // This shall be only one because concurrent modifications of one table is not allowed
  private final Set<String> preDeletedColumns = new HashSet<>();

  public ConfigTableInfo(final String name) {
    super(name);
  }

  public TsTable getTable() {
    return table;
  }

  public void setTable(final TsTable table) {
    this.table = table;
  }

  public TableNodeStatus getStatus() {
    return status;
  }

  public void setStatus(final TableNodeStatus status) {
    this.status = status;
  }

  public Set<String> getPreDeletedColumns() {
    return preDeletedColumns;
  }

  public void addPreDeletedColumn(final String column) {
    preDeletedColumns.add(column);
  }

  public void removePreDeletedColumn(final String column) {
    preDeletedColumns.remove(column);
  }

  @Override
  public int estimateSize() {
    return 1
        + 8
        + table.getColumnNum() * 30
        + 8
        + SET_SIZE
        + preDeletedColumns.stream()
            .map(column -> (int) RamUsageEstimator.sizeOf(column))
            .reduce(0, Integer::sum);
  }
}
