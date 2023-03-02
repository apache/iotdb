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
import org.apache.iotdb.db.metadata.mnode.BasicMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.estimator.BasicMNodSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.estimator.IMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.MNodeIterator;
import org.apache.iotdb.db.metadata.mnode.iterator.MemoryTraverserIterator;
import org.apache.iotdb.db.metadata.mtree.snapshot.MemMTreeSnapshotUtil;
import org.apache.iotdb.db.metadata.newnode.IMemMNode;
import org.apache.iotdb.db.metadata.newnode.abovedatabase.AboveDatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.database.DatabaseMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class MemMTreeStore implements IMTreeStore<IMemMNode> {

  private final IMNodeSizeEstimator estimator = new BasicMNodSizeEstimator();
  private MemSchemaRegionStatistics regionStatistics;

  private IMemMNode root;

  public MemMTreeStore(PartialPath rootPath, MemSchemaRegionStatistics regionStatistics) {
    this.root =
        new DatabaseMNode(
            null,
            rootPath.getTailNode(),
            CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    this.regionStatistics = regionStatistics;
  }

  private MemMTreeStore(IMemMNode root, MemSchemaRegionStatistics regionStatistics) {
    this.root = root;
    this.regionStatistics = regionStatistics;
  }

  @Override
  public IMemMNode generatePrefix(PartialPath storageGroupPath) {
    String[] nodes = storageGroupPath.getNodes();
    // nodes[0] must be root
    IMemMNode res = new AboveDatabaseMNode(null, nodes[0]);
    IMemMNode cur = res;
    IMemMNode child;
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
  public IMemMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMemMNode parent, String name) {
    return parent.hasChild(name);
  }

  @Override
  public IMemMNode getChild(IMemMNode parent, String name) {
    return parent.getChild(name);
  }

  @Override
  public IMNodeIterator<IMemMNode> getChildrenIterator(IMemMNode parent) {
    return new MNodeIterator<>(parent.getChildren().values().iterator());
  }

  @Override
  public IMNodeIterator<IMemMNode> getTraverserIterator(
      IMemMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
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
  public IMemMNode addChild(IMemMNode parent, String childName, IMemMNode child) {
    IMemMNode result = parent.addChild(childName, child);
    if (result == child) {
      requestMemory(estimator.estimateSize(child));
    }
    return result;
  }

  @Override
  public void deleteChild(IMemMNode parent, String childName) {
    releaseMemory(estimator.estimateSize(parent.deleteChild(childName)));
  }

  @Override
  public void updateMNode(IMemMNode node) {}

  @Override
  public IDeviceMNode<IMemMNode> setToEntity(IMemMNode node) {
    IDeviceMNode<IMemMNode> result = MNodeUtils.setToEntity(node);
    if (result != node) {
      requestMemory(IMNodeSizeEstimator.getEntityNodeBaseSize());
    }

    if (result.isDatabase()) {
      root = result.getAsMNode();
    }
    return result;
  }

  @Override
  public IMemMNode setToInternal(IDeviceMNode<IMemMNode> entityMNode) {
    IMemMNode result = MNodeUtils.setToInternal(entityMNode);
    if (result != entityMNode) {
      releaseMemory(IMNodeSizeEstimator.getEntityNodeBaseSize());
    }
    if (result.isDatabase()) {
      root = result;
      // TODO: 这里和cached不一致，记得检查
    }
    return result;
  }

  @Override
  public void setAlias(IMeasurementMNode<IMemMNode> measurementMNode, String alias) {
    String existingAlias = measurementMNode.getAlias();
    if (existingAlias == null && alias == null) {
      return;
    }

    measurementMNode.setAlias(alias);

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
  public void pin(IMemMNode node) {}

  @Override
  public void unPin(IMemMNode node) {}

  @Override
  public void unPinPath(IMemMNode node) {}

  @Override
  public IMTreeStore<IMemMNode> getWithReentrantReadLock() {
    return this;
  }

  @Override
  public void clear() {
    root = new BasicMNode(null, IoTDBConstant.PATH_ROOT);
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return MemMTreeSnapshotUtil.createSnapshot(snapshotDir, this);
  }

  public static MemMTreeStore loadFromSnapshot(
      File snapshotDir,
      Consumer<IMeasurementMNode<IMemMNode>> measurementProcess,
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
