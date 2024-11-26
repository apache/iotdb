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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;

import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class DeviceBlackListConstructor extends DeviceUpdater {
  private final List<IDeviceMNode<IMemMNode>> nodes =
      new ArrayList<>(DEFAULT_MAX_TS_BLOCK_LINE_NUMBER);

  private long preDeletedNum = 0;
  private final MemSchemaRegionStatistics regionStatistics;

  public DeviceBlackListConstructor(
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final String database,
      final String tableName,
      final List<ColumnHeader> columnHeaderList,
      final BiFunction<Integer, String, Binary> attributeProvider,
      final MemSchemaRegionStatistics regionStatistics) {
    super(
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        database,
        tableName,
        columnHeaderList,
        attributeProvider);
    this.regionStatistics = regionStatistics;
  }

  public void handleDeviceNode(final IDeviceMNode<IMemMNode> node) throws MetadataException {
    if (withoutFilter()) {
      node.preDeactivateSelfOrTemplate();
      preDeletedNum++;
      regionStatistics.decreaseTableDevice(tableName, 1);
      return;
    }
    nodes.add(node);
    super.handleDeviceNode(node);
  }

  @Override
  protected void update() {
    preDeletedNum += indexes.size();
    regionStatistics.decreaseTableDevice(tableName, indexes.size());
    for (final Integer index : indexes) {
      nodes.get(index).preDeactivateSelfOrTemplate();
    }
    nodes.clear();
    super.clear();
  }

  public long getPreDeletedNum() {
    return preDeletedNum;
  }
}
