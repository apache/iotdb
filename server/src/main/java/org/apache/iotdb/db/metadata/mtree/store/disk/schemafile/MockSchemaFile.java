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
import org.apache.iotdb.db.metadata.mtree.store.disk.CachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.newnode.ICacheMNode;
import org.apache.iotdb.db.metadata.newnode.basic.CacheBasicMNode;
import org.apache.iotdb.db.metadata.newnode.database.CacheDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.database.IDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.databasedevice.CacheDatabaseDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.CacheDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.CacheMeasurementMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;

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
  private IDatabaseMNode<ICacheMNode> storageGroupMNode;

  private long fileTail = 0;
  private final Map<Long, Map<String, ICacheMNode>> mockFile = new HashMap<>();

  public MockSchemaFile(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  @Override
  public ICacheMNode init() {
    storageGroupMNode =
        new CacheDatabaseMNode(
            null,
            storageGroupPath.getTailNode(),
            CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    writeMNode(storageGroupMNode.getAsMNode());
    return cloneMNode(storageGroupMNode.getAsMNode());
  }

  @Override
  public boolean updateStorageGroupNode(IDatabaseMNode<ICacheMNode> sgNode) throws IOException {
    this.storageGroupMNode = cloneMNode(sgNode.getAsMNode()).getAsDatabaseMNode();
    return true;
  }

  @Override
  public ICacheMNode getChildNode(ICacheMNode parent, String childName) {
    Map<String, ICacheMNode> segment = getSegment(parent);
    ICacheMNode result = null;
    if (segment != null) {
      result = cloneMNode(segment.get(childName));
      if (result == null && parent.isEntity()) {
        for (ICacheMNode node : segment.values()) {
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
  public Iterator<ICacheMNode> getChildren(ICacheMNode parent) {

    Map<String, ICacheMNode> segment = getSegment(parent);
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
  public void writeMNode(ICacheMNode parent) {
    ICachedMNodeContainer container = getCachedMNodeContainer(parent);
    long address = container.getSegmentAddress();
    if (container.isVolatile()) {
      address = allocateSegment();
      container.setSegmentAddress(address);
    }
    write(address, container.getUpdatedChildBuffer());
    write(address, container.getNewChildBuffer());
  }

  private void write(long address, Map<String, ICacheMNode> nodeMap) {
    for (ICacheMNode node : nodeMap.values()) {
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
  public void delete(ICacheMNode targetNode) {
    ICacheMNode removedNode = getSegment(targetNode.getParent()).remove(targetNode.getName());
    if (removedNode == null || removedNode.isMeasurement()) {
      return;
    }
    deleteMNodeRecursively(removedNode);
  }

  private void deleteMNodeRecursively(ICacheMNode node) {
    ICachedMNodeContainer container = getCachedMNodeContainer(node);
    Map<String, ICacheMNode> removedSegment = mockFile.remove(container.getSegmentAddress());
    if (removedSegment != null) {
      for (ICacheMNode child : removedSegment.values()) {
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

  private long getSegmentAddress(ICacheMNode node) {
    return getCachedMNodeContainer(node).getSegmentAddress();
  }

  private Map<String, ICacheMNode> getSegment(ICacheMNode node) {
    return mockFile.get(getSegmentAddress(node));
  }

  private long allocateSegment() {
    long address = fileTail++;
    mockFile.put(address, new ConcurrentHashMap<>());
    return address;
  }

  static ICacheMNode cloneMNode(ICacheMNode node) {
    if (node == null) {
      return null;
    }
    if (node.isMeasurement()) {
      IMeasurementMNode<ICacheMNode> measurementMNode = node.getAsMeasurementMNode();
      CacheMeasurementMNode result =
          new CacheMeasurementMNode(
              null,
              measurementMNode.getName(),
              measurementMNode.getSchema(),
              measurementMNode.getAlias());
      result.setOffset(measurementMNode.getOffset());
      return result;
    } else if (node.isDatabase() && node.isEntity()) {
      CacheDatabaseDeviceMNode result =
          new CacheDatabaseDeviceMNode(
              null, node.getName(), node.getAsDatabaseMNode().getDataTTL());
      result.setAligned(node.getAsEntityMNode().isAligned());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isEntity()) {
      CacheDeviceMNode result = new CacheDeviceMNode(null, node.getName());
      result.setAligned(node.getAsEntityMNode().isAligned());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isDatabase()) {
      CacheDatabaseMNode result =
          new CacheDatabaseMNode(null, node.getName(), node.getAsDatabaseMNode().getDataTTL());
      cloneInternalMNodeData(node, result);
      return result;
    } else {
      CacheBasicMNode result = new CacheBasicMNode(null, node.getName());
      cloneInternalMNodeData(node, result);
      return result;
    }
  }

  private static void cloneInternalMNodeData(ICacheMNode node, ICacheMNode result) {
    ICachedMNodeContainer container = new CachedMNodeContainer();
    container.setSegmentAddress((getCachedMNodeContainer(node)).getSegmentAddress());
    result.setChildren(container);
  }

  private class MockSchemaFileIterator implements Iterator<ICacheMNode> {

    Iterator<ICacheMNode> iterator;

    MockSchemaFileIterator(Iterator<ICacheMNode> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public ICacheMNode next() {
      return cloneMNode(iterator.next());
    }
  }
}
