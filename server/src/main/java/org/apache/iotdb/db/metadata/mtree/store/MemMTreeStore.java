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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
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
import org.apache.iotdb.db.metadata.mtree.snapshot.MemMTreeSnapshotUtil;
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class MemMTreeStore implements IMTreeStore {

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
              CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    } else {
      this.root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
    }
  }

  private MemMTreeStore(IMNode root) {
    this.root = root;
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

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return MemMTreeSnapshotUtil.createSnapshot(snapshotDir, this);
  }

  public static MemMTreeStore loadFromSnapshot(
      File snapshotDir, Consumer<IMeasurementMNode> measurementProcess) throws IOException {
    return new MemMTreeStore(MemMTreeSnapshotUtil.loadSnapshot(snapshotDir, measurementProcess));
  }

  private void requestMemory(int size) {
    memoryStatistics.requestMemory(size);
    localMemoryUsage.getAndUpdate(v -> v += size);
  }

  private void releaseMemory(int size) {
    localMemoryUsage.getAndUpdate(v -> v -= size);
    memoryStatistics.releaseMemory(size);
  }
}
