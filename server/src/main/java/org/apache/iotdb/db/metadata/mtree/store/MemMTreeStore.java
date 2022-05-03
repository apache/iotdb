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
package org.apache.iotdb.db.metadata.mtree.store;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.estimator.BasicMNodSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.estimator.IMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.MNodeIterator;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.MetadataConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.STORAGE_GROUP_MNODE_TYPE;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class MemMTreeStore implements IMTreeStore {

  private final Logger logger = LoggerFactory.getLogger(MemMTreeStore.class);

  private MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();
  private IMNodeSizeEstimator estimator = new BasicMNodSizeEstimator();
  private AtomicLong localMemoryUsage = new AtomicLong(0);

  private IMNode root;

  public MemMTreeStore(PartialPath rootPath, boolean isStorageGroup) {
    if (isStorageGroup) {
      this.root =
          new StorageGroupMNode(
              null,
              rootPath.getTailNode(),
              IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
    } else {
      this.root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
    }
  }

  @Override
  public IMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMNode parent, String name) {
    return parent.hasChild(name);
  }

  @Override
  public IMNode getChild(IMNode parent, String name) {
    return parent.getChild(name);
  }

  @Override
  public IMNodeIterator getChildrenIterator(IMNode parent) {
    return new MNodeIterator(parent.getChildren().values().iterator());
  }

  @Override
  public IMNode addChild(IMNode parent, String childName, IMNode child) {
    IMNode result = parent.addChild(childName, child);
    if (result == child) {
      requestMemory(estimator.estimateSize(child));
    }
    return result;
  }

  @Override
  public void deleteChild(IMNode parent, String childName) {
    releaseMemory(estimator.estimateSize(parent.deleteChild(childName)));
  }

  @Override
  public void updateMNode(IMNode node) {}

  @Override
  public IEntityMNode setToEntity(IMNode node) {
    IEntityMNode result = MNodeUtils.setToEntity(node);
    if (result != node) {
      requestMemory(IMNodeSizeEstimator.getEntityNodeBaseSize());
    }

    if (result.isStorageGroup()) {
      root = result;
    }
    return result;
  }

  @Override
  public IMNode setToInternal(IEntityMNode entityMNode) {
    IMNode result = MNodeUtils.setToInternal(entityMNode);
    if (result != entityMNode) {
      releaseMemory(IMNodeSizeEstimator.getEntityNodeBaseSize());
    }
    if (result.isStorageGroup()) {
      root = result;
    }
    return result;
  }

  @Override
  public void setAlias(IMeasurementMNode measurementMNode, String alias) {
    String existingAlias = measurementMNode.getAlias();
    if (existingAlias == null && alias == null) {
      return;
    }

    measurementMNode.setAlias(alias);
    updateMNode(measurementMNode);

    if (existingAlias != null && alias != null) {
      int delta = alias.length() - existingAlias.length();
      if (delta > 0) {
        requestMemory(delta);
      } else if (delta < 0) {
        releaseMemory(-delta);
      }
    } else if (alias == null) {
      releaseMemory(IMNodeSizeEstimator.getAliasBaseSize() + existingAlias.length());
    } else {
      requestMemory(IMNodeSizeEstimator.getAliasBaseSize() + alias.length());
    }
  }

  @Override
  public void pin(IMNode node) {}

  @Override
  public void unPin(IMNode node) {}

  @Override
  public void unPinPath(IMNode node) {}

  @Override
  public void clear() {
    root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
    memoryStatistics.releaseMemory(localMemoryUsage.get());
    localMemoryUsage.set(0);
  }

  private void requestMemory(int size) {
    memoryStatistics.requestMemory(size);
    localMemoryUsage.getAndUpdate(v -> v += size);
  }

  private void releaseMemory(int size) {
    localMemoryUsage.getAndUpdate(v -> v -= size);
    memoryStatistics.releaseMemory(size);
  }

  public void serialize(ByteBuffer buffer) {
    serializeInternalNode((InternalMNode) this.root, buffer);
  }

  private void serializeInternalNode(InternalMNode node, ByteBuffer buffer) {
    for (IMNode child : node.getChildren().values()) {
      if (child.isStorageGroup()) {
        serializeStorageGroupNode((StorageGroupMNode) child, buffer);
      } else {
        serializeInternalNode((InternalMNode) child, buffer);
      }
    }

    buffer.put(INTERNAL_MNODE_TYPE);
    ReadWriteIOUtils.write(node.getName(), buffer);
    ReadWriteIOUtils.write(node.getChildren().size(), buffer);
  }

  private void serializeStorageGroupNode(StorageGroupMNode storageGroupNode, ByteBuffer buffer) {
    buffer.put(STORAGE_GROUP_MNODE_TYPE);
    ReadWriteIOUtils.write(storageGroupNode.getName(), buffer);
    ThriftConfigNodeSerDeUtils.writeTStorageGroupSchema(
        storageGroupNode.getStorageGroupSchema(), buffer);
  }

  public void deserialize(ByteBuffer buffer) {
    byte type = buffer.get();
    if (type != STORAGE_GROUP_MNODE_TYPE) {
      logger.error("Wrong node type. Cannot deserialize MTreeAboveSG from given buffer");
      return;
    }

    StorageGroupMNode storageGroupMNode = deserializeStorageGroupMNode(buffer);
    InternalMNode internalMNode;

    Stack<InternalMNode> stack = new Stack<>();
    stack.push(storageGroupMNode);

    String name = storageGroupMNode.getName();
    int childNum = 0;

    while (!PATH_ROOT.equals(name)) {
      type = buffer.get();
      switch (type) {
        case INTERNAL_MNODE_TYPE:
          internalMNode = deserializeInternalMNode(buffer);
          childNum = ReadWriteIOUtils.readInt(buffer);
          while (childNum > 0) {
            internalMNode.addChild(stack.pop());
            childNum--;
          }
          stack.push(internalMNode);
          name = internalMNode.getName();
          break;
        case STORAGE_GROUP_MNODE_TYPE:
          storageGroupMNode = deserializeStorageGroupMNode(buffer);
          childNum = 0;
          stack.push(storageGroupMNode);
          name = storageGroupMNode.getName();
          break;
        default:
          logger.error("Unrecognized node type. Cannot deserialize MTreeAboveSG from given buffer");
          return;
      }
    }
    this.root = stack.peek();
  }

  private InternalMNode deserializeInternalMNode(ByteBuffer buffer) {
    return new InternalMNode(null, ReadWriteIOUtils.readString(buffer));
  }

  private StorageGroupMNode deserializeStorageGroupMNode(ByteBuffer buffer) {
    StorageGroupMNode storageGroupMNode =
        new StorageGroupMNode(null, ReadWriteIOUtils.readString(buffer));
    storageGroupMNode.setStorageGroupSchema(
        ThriftConfigNodeSerDeUtils.readTStorageGroupSchema(buffer));
    return storageGroupMNode;
  }
}
