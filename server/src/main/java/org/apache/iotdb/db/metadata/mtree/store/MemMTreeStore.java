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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.AboveDatabaseMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.estimator.BasicMNodSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.estimator.IMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.MNodeIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.MemoryTraverserIterator;
import org.apache.iotdb.db.metadata.mtree.snapshot.MemMTreeSnapshotUtil;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class MemMTreeStore implements IMTreeStore {

  private final IMNodeSizeEstimator estimator = new BasicMNodSizeEstimator();
  private MemSchemaRegionStatistics regionStatistics;

  private IMNode root;

  // Only used for ConfigMTree
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

  public MemMTreeStore(
      PartialPath rootPath, boolean isStorageGroup, MemSchemaRegionStatistics regionStatistics) {
    if (isStorageGroup) {
      this.root =
          new StorageGroupMNode(
              null,
              rootPath.getTailNode(),
              CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    } else {
      this.root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
    }
    this.regionStatistics = regionStatistics;
  }

  private MemMTreeStore(IMNode root, MemSchemaRegionStatistics regionStatistics) {
    this.root = root;
    this.regionStatistics = regionStatistics;
  }

  @Override
  public IMNode generatePrefix(PartialPath storageGroupPath) {
    String[] nodes = storageGroupPath.getNodes();
    // nodes[0] must be root
    IMNode res = new AboveDatabaseMNode(null, nodes[0]);
    IMNode cur = res;
    IMNode child;
    for (int i = 1; i < nodes.length - 1; i++) {
      child = new AboveDatabaseMNode(cur, nodes[i]);
      cur.addChild(nodes[i], child);
      cur = child;
    }
    root.setParent(cur);
    cur.addChild(root);
    requestMemory(estimator.estimateSize(root));
    return res;
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
  public IMNodeIterator getTraverserIterator(
      IMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    if (parent.isEntity()) {
      AbstractTraverserIterator iterator =
          new MemoryTraverserIterator(this, parent.getAsEntityMNode(), templateMap);
      iterator.setSkipPreDeletedSchema(skipPreDeletedSchema);
      return iterator;
    } else {
      return getChildrenIterator(parent);
    }
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
  public IMTreeStore getWithReentrantReadLock() {
    return this;
  }

  @Override
  public void clear() {
    root = new InternalMNode(null, IoTDBConstant.PATH_ROOT);
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return MemMTreeSnapshotUtil.createSnapshot(snapshotDir, this);
  }

  public static MemMTreeStore loadFromSnapshot(
      File snapshotDir,
      Consumer<IMeasurementMNode> measurementProcess,
      MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    return new MemMTreeStore(
        MemMTreeSnapshotUtil.loadSnapshot(snapshotDir, measurementProcess, regionStatistics),
        regionStatistics);
  }

  private void requestMemory(int size) {
    if (regionStatistics != null) {
      regionStatistics.requestMemory(size);
    }
  }

  private void releaseMemory(int size) {
    if (regionStatistics != null) {
      regionStatistics.releaseMemory(size);
    }
  }
}
