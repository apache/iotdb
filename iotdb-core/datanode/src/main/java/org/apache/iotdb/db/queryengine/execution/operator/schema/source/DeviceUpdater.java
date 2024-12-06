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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;

import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.function.BiFunction;

public abstract class DeviceUpdater extends DevicePredicateHandler {

  private final BiFunction<Integer, String, Binary> attributeProvider;

  protected DeviceUpdater(
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final String database,
      final String tableName,
      final List<ColumnHeader> columnHeaderList,
      final BiFunction<Integer, String, Binary> attributeProvider) {
    super(
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        database,
        tableName,
        columnHeaderList);
    this.attributeProvider = attributeProvider;
  }

  public void handleDeviceNode(final IDeviceMNode<IMemMNode> node) throws MetadataException {
    final ShowDevicesResult result =
        new ShowDevicesResult(
            null,
            node.isAlignedNullable(),
            node.getSchemaTemplateId(),
            node.getPartialPath().getNodes());
    final int pointer = ((TableDeviceInfo<IMemMNode>) node.getDeviceInfo()).getAttributePointer();
    result.setAttributeProvider(k -> attributeProvider.apply(pointer, k));
    addBatch(result);
    if (hasComputedResult()) {
      update();
    }
  }

  protected abstract void update() throws MetadataException;

  @Override
  public void close() throws MetadataException {
    prepareBatchResult();
    if (hasComputedResult()) {
      update();
    }
    super.close();
  }
}
