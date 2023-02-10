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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.CachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class MockSchemaFile implements ISchemaFile {

  private PartialPath storageGroupPath;
  private IStorageGroupMNode storageGroupMNode;

  private long fileTail = 0;
  private final Map<Long, Map<String, IMNode>> mockFile = new HashMap<>();

  public MockSchemaFile(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  @Override
  public IMNode init() {
    storageGroupMNode =
        new StorageGroupMNode(
            null,
            storageGroupPath.getTailNode(),
            CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    writeMNode(storageGroupMNode);
    return cloneMNode(storageGroupMNode);
  }

  @Override
  public boolean updateStorageGroupNode(IStorageGroupMNode sgNode) throws IOException {
    this.storageGroupMNode = cloneMNode(sgNode).getAsStorageGroupMNode();
    return true;
  }

  @Override
  public IMNode getChildNode(IMNode parent, String childName) {
    Map<String, IMNode> segment = getSegment(parent);
    IMNode result = null;
    if (segment != null) {
      result = cloneMNode(segment.get(childName));
      if (result == null && parent.isEntity()) {
        for (IMNode node : segment.values()) {
          if (node.isMeasurement() && childName.equals(node.getAsMeasurementMNode().getAlias())) {
            result = cloneMNode(node);
            break;
          }
        }
      }
    }
    return result;
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) {

    Map<String, IMNode> segment = getSegment(parent);
    if (segment == null) {
      return Collections.emptyIterator();
    }
    return new MockSchemaFileIterator(getSegment(parent).values().iterator());
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return false;
  }

  @Override
  public void writeMNode(IMNode parent) {
    ICachedMNodeContainer container = getCachedMNodeContainer(parent);
    long address = container.getSegmentAddress();
    if (container.isVolatile()) {
      address = allocateSegment();
      container.setSegmentAddress(address);
    }
    write(address, container.getUpdatedChildBuffer());
    write(address, container.getNewChildBuffer());
  }

  private void write(long address, Map<String, IMNode> nodeMap) {
    for (IMNode node : nodeMap.values()) {
      if (!node.isMeasurement()) {
        ICachedMNodeContainer container = getCachedMNodeContainer(node);
        if (container.isVolatile()) {
          container.setSegmentAddress(allocateSegment());
        }
      }
      mockFile.get(address).put(node.getName(), cloneMNode(node));
    }
  }

  @Override
  public void delete(IMNode targetNode) {
    IMNode removedNode = getSegment(targetNode.getParent()).remove(targetNode.getName());
    if (removedNode == null || removedNode.isMeasurement()) {
      return;
    }
    deleteMNodeRecursively(removedNode);
  }

  private void deleteMNodeRecursively(IMNode node) {
    ICachedMNodeContainer container = getCachedMNodeContainer(node);
    Map<String, IMNode> removedSegment = mockFile.remove(container.getSegmentAddress());
    if (removedSegment != null) {
      for (IMNode child : removedSegment.values()) {
        deleteMNodeRecursively(child);
      }
    }
  }

  @Override
  public void sync() {}

  @Override
  public void close() {}

  @Override
  public void clear() {
    mockFile.clear();
  }

  private long getSegmentAddress(IMNode node) {
    return getCachedMNodeContainer(node).getSegmentAddress();
  }

  private Map<String, IMNode> getSegment(IMNode node) {
    return mockFile.get(getSegmentAddress(node));
  }

  private long allocateSegment() {
    long address = fileTail++;
    mockFile.put(address, new ConcurrentHashMap<>());
    return address;
  }

  static IMNode cloneMNode(IMNode node) {
    if (node == null) {
      return null;
    }
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
              null, node.getName(), node.getAsStorageGroupMNode().getDataTTL());
      result.setAligned(node.getAsEntityMNode().isAligned());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isEntity()) {
      IEntityMNode result = new EntityMNode(null, node.getName());
      result.setAligned(node.getAsEntityMNode().isAligned());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isStorageGroup()) {
      StorageGroupMNode result =
          new StorageGroupMNode(null, node.getName(), node.getAsStorageGroupMNode().getDataTTL());
      cloneInternalMNodeData(node, result);
      return result;
    } else {
      InternalMNode result = new InternalMNode(null, node.getName());
      cloneInternalMNodeData(node, result);
      return result;
    }
  }

  private static void cloneInternalMNodeData(IMNode node, IMNode result) {
    result.setUseTemplate(node.isUseTemplate());
    ICachedMNodeContainer container = new CachedMNodeContainer();
    container.setSegmentAddress((getCachedMNodeContainer(node)).getSegmentAddress());
    result.setChildren(container);
  }

  private class MockSchemaFileIterator implements Iterator<IMNode> {

    Iterator<IMNode> iterator;

    MockSchemaFileIterator(Iterator<IMNode> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public IMNode next() {
      return cloneMNode(iterator.next());
    }
  }
}
