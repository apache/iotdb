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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.MNodeIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.MemoryTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;

import java.io.File;
import java.util.Map;
import java.util.function.Consumer;

/** This is a memory-based implementation of IMTreeStore. All MNodes are stored in memory. */
public class ConfigMTreeStore implements IMTreeStore<IConfigMNode> {

  private IConfigMNode root;
  private final IMNodeFactory<IConfigMNode> nodeFactory;

  // Only used for ConfigMTree
  public ConfigMTreeStore(IMNodeFactory<IConfigMNode> nodeFactory) {
    this.root = nodeFactory.createInternalMNode(null, IoTDBConstant.PATH_ROOT);
    this.nodeFactory = nodeFactory;
  }

  @Override
  public IConfigMNode generatePrefix(PartialPath storageGroupPath) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IConfigMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IConfigMNode parent, String name) {
    return parent.hasChild(name);
  }

  @Override
  public IConfigMNode getChild(IConfigMNode parent, String name) {
    return parent.getChild(name);
  }

  @Override
  public IMNodeIterator<IConfigMNode> getChildrenIterator(IConfigMNode parent) {
    return new MNodeIterator<>(parent.getChildren().values().iterator());
  }

  @Override
  public IMNodeIterator<IConfigMNode> getTraverserIterator(
      IConfigMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    if (parent.isDevice()) {
      AbstractTraverserIterator<IConfigMNode> iterator =
          new MemoryTraverserIterator<>(this, parent.getAsDeviceMNode(), templateMap, nodeFactory);
      iterator.setSkipPreDeletedSchema(skipPreDeletedSchema);
      return iterator;
    } else {
      return getChildrenIterator(parent);
    }
  }

  @Override
  public IConfigMNode addChild(IConfigMNode parent, String childName, IConfigMNode child) {
    return parent.addChild(childName, child);
  }

  @Override
  public void deleteChild(IConfigMNode parent, String childName) {
    parent.deleteChild(childName);
  }

  @Override
  public void updateMNode(IConfigMNode node, Consumer<IConfigMNode> operation) {}

  @Override
  public IDeviceMNode<IConfigMNode> setToEntity(IConfigMNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IConfigMNode setToInternal(IDeviceMNode<IConfigMNode> entityMNode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAlias(IMeasurementMNode<IConfigMNode> measurementMNode, String alias) {
    String existingAlias = measurementMNode.getAlias();
    if (existingAlias == null && alias == null) {
      return;
    }
    measurementMNode.setAlias(alias);
  }

  @Override
  public void pin(IConfigMNode node) {}

  @Override
  public void unPin(IConfigMNode node) {}

  @Override
  public void unPinPath(IConfigMNode node) {}

  @Override
  public IMTreeStore getWithReentrantReadLock() {
    return this;
  }

  @Override
  public void clear() {
    root = nodeFactory.createInternalMNode(null, IoTDBConstant.PATH_ROOT);
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReleaseFlushMonitor.RecordNode recordTraverserStatistics() {
    // do nothing
    return null;
  }

  @Override
  public void recordTraverserMetric(long costTime) {
    // do nothing
  }
}
