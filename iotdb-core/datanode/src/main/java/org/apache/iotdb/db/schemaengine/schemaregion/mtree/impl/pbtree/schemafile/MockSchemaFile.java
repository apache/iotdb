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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.CachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer.getCachedMNodeContainer;

public class MockSchemaFile implements ISchemaFile {

  private PartialPath storageGroupPath;
  private IDatabaseMNode<ICachedMNode> storageGroupMNode;
  private static final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();

  private long fileTail = 0;
  private final Map<Long, Map<String, ICachedMNode>> mockFile = new HashMap<>();

  public MockSchemaFile(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  @Override
  public ICachedMNode init() {
    storageGroupMNode =
        nodeFactory.createDatabaseMNode(
            null,
            storageGroupPath.getTailNode(),
            CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    writeMNode(storageGroupMNode.getAsMNode());
    return cloneMNode(storageGroupMNode.getAsMNode());
  }

  @Override
  public boolean updateDatabaseNode(IDatabaseMNode<ICachedMNode> sgNode) throws IOException {
    this.storageGroupMNode = cloneMNode(sgNode.getAsMNode()).getAsDatabaseMNode();
    return true;
  }

  @Override
  public ICachedMNode getChildNode(ICachedMNode parent, String childName) {
    Map<String, ICachedMNode> segment = getSegment(parent);
    ICachedMNode result = null;
    if (segment != null) {
      result = cloneMNode(segment.get(childName));
      if (result == null && parent.isDevice()) {
        for (ICachedMNode node : segment.values()) {
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
  public Iterator<ICachedMNode> getChildren(ICachedMNode parent) {

    Map<String, ICachedMNode> segment = getSegment(parent);
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
  public void writeMNode(ICachedMNode parent) {
    ICachedMNodeContainer container = getCachedMNodeContainer(parent);
    long address = container.getSegmentAddress();
    if (container.isVolatile()) {
      address = allocateSegment();
      container.setSegmentAddress(address);
    }
    write(address, container.getUpdatedChildBuffer());
    write(address, container.getNewChildBuffer());
  }

  private void write(long address, Map<String, ICachedMNode> nodeMap) {
    for (ICachedMNode node : nodeMap.values()) {
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
  public void delete(ICachedMNode targetNode) {
    ICachedMNode removedNode = getSegment(targetNode.getParent()).remove(targetNode.getName());
    if (removedNode == null || removedNode.isMeasurement()) {
      return;
    }
    deleteMNodeRecursively(removedNode);
  }

  private void deleteMNodeRecursively(ICachedMNode node) {
    ICachedMNodeContainer container = getCachedMNodeContainer(node);
    Map<String, ICachedMNode> removedSegment = mockFile.remove(container.getSegmentAddress());
    if (removedSegment != null) {
      for (ICachedMNode child : removedSegment.values()) {
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

  private long getSegmentAddress(ICachedMNode node) {
    return getCachedMNodeContainer(node).getSegmentAddress();
  }

  private Map<String, ICachedMNode> getSegment(ICachedMNode node) {
    return mockFile.get(getSegmentAddress(node));
  }

  private long allocateSegment() {
    long address = fileTail++;
    mockFile.put(address, new ConcurrentHashMap<>());
    return address;
  }

  static ICachedMNode cloneMNode(ICachedMNode node) {
    if (node == null) {
      return null;
    }
    if (node.isMeasurement()) {
      IMeasurementMNode<ICachedMNode> measurementMNode = node.getAsMeasurementMNode();
      ICachedMNode result =
          nodeFactory
              .createMeasurementMNode(
                  null,
                  measurementMNode.getName(),
                  measurementMNode.getSchema(),
                  measurementMNode.getAlias())
              .getAsMNode();
      result.getAsMeasurementMNode().setOffset(measurementMNode.getOffset());
      return result;
    } else if (node.isDatabase() && node.isDevice()) {
      ICachedMNode result =
          nodeFactory.createDatabaseDeviceMNode(
              null, node.getName(), node.getAsDatabaseMNode().getDataTTL());
      result.getAsDeviceMNode().setAligned(node.getAsDeviceMNode().isAlignedNullable());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isDevice()) {
      ICachedMNode result = nodeFactory.createDeviceMNode(null, node.getName()).getAsMNode();
      result.getAsDeviceMNode().setAligned(node.getAsDeviceMNode().isAlignedNullable());
      cloneInternalMNodeData(node, result);
      return result;
    } else if (node.isDatabase()) {
      ICachedMNode result =
          nodeFactory
              .createDatabaseMNode(null, node.getName(), node.getAsDatabaseMNode().getDataTTL())
              .getAsMNode();
      cloneInternalMNodeData(node, result);
      return result;
    } else {
      ICachedMNode result = nodeFactory.createInternalMNode(null, node.getName());
      cloneInternalMNodeData(node, result);
      return result;
    }
  }

  private static void cloneInternalMNodeData(ICachedMNode node, ICachedMNode result) {
    ICachedMNodeContainer container = new CachedMNodeContainer();
    container.setSegmentAddress((getCachedMNodeContainer(node)).getSegmentAddress());
    result.setChildren(container);
  }

  private class MockSchemaFileIterator implements Iterator<ICachedMNode> {

    Iterator<ICachedMNode> iterator;

    MockSchemaFileIterator(Iterator<ICachedMNode> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public ICachedMNode next() {
      return cloneMNode(iterator.next());
    }
  }
}
