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
package org.apache.iotdb.db.metadata.mtree.store.disk.file;

import org.apache.iotdb.db.metadata.mnode.CachedMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ISegment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MockSchemaFile implements ISchemaFile {

  private long fileTail = 0;

  private final Map<Long, Map<String, IMNode>> mockFile = new HashMap<>();

  @Override
  public IMNode getChildNode(IMNode parent, String childName) {
    return cloneMNode(mockFile.get(getSegmentAddress(parent)).get(childName));
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) {
    Map<String, IMNode> map = new HashMap<>();
    for (IMNode node : mockFile.get(getSegmentAddress(parent)).values()) {
      map.put(node.getName(), cloneMNode(node));
    }
    return map.values().iterator();
  }

  @Override
  public void writeMNode(IMNode parent) {
    ISegment segment = getSegment(parent);
    long address = segment.getSegmentAddress();
    if (segment.isVolatile()) {
      address = fileTail++;
      segment.setSegmentAddress(address);
      mockFile.put(address, new HashMap<>());
    }
    write(address, segment.getUpdatedChildBuffer());
    write(address, segment.getNewChildBuffer());
  }

  private void write(long address, Map<String, IMNode> nodeMap) {
    for (IMNode node : nodeMap.values()) {
      mockFile.get(address).put(node.getName(), cloneMNode(node));
    }
  }

  @Override
  public void deleteMNode(IMNode targetNode) {
    ISegment segment = getSegment(targetNode.getParent());
    mockFile.get(segment.getSegmentAddress()).remove(targetNode.getName());
  }

  @Override
  public void sync() {}

  @Override
  public void close() {}

  @Override
  public void clear() {
    mockFile.clear();
  }

  private ISegment getSegment(IMNode node) {
    return node.getChildren().getSegment();
  }

  private long getSegmentAddress(IMNode node) {
    return node.getChildren().getSegment().getSegmentAddress();
  }

  private IMNode cloneMNode(IMNode node) {
    if (node.isMeasurement()) {
      IMeasurementMNode measurementMNode = node.getAsMeasurementMNode();
      IMeasurementMNode result =
          MeasurementMNode.getMeasurementMNode(
              null,
              measurementMNode.getName(),
              measurementMNode.getSchema(),
              measurementMNode.getAlias());
      result.setOffset(measurementMNode.getOffset());
      return result;
    } else if (node.isStorageGroup() && node.isEntity()) {
      StorageGroupEntityMNode result =
          new StorageGroupEntityMNode(
              node.getParent(), node.getName(), node.getAsStorageGroupMNode().getDataTTL());
      result.setAligned(node.getAsEntityMNode().isAligned());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isEntity()) {
      IEntityMNode result = new EntityMNode(node.getParent(), node.getName());
      result.setAligned(node.getAsEntityMNode().isAligned());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isStorageGroup()) {
      StorageGroupMNode result =
          new StorageGroupMNode(
              node.getParent(), node.getName(), node.getAsStorageGroupMNode().getDataTTL());
      cloneInternalMNodeData(node, result);
      return result;
    } else {
      InternalMNode result = new InternalMNode(node.getParent(), node.getName());
      cloneInternalMNodeData(node, result);
      return result;
    }
  }

  private void cloneInternalMNodeData(IMNode node, IMNode result) {
    result.setUseTemplate(node.isUseTemplate());
    result.setSchemaTemplate(node.getSchemaTemplate());
    IMNodeContainer container = new CachedMNodeContainer();
    ISegment segment = container.getSegment();
    segment.setSegmentAddress(node.getChildren().getSegment().getSegmentAddress());
    result.setChildren(container);
  }
}
