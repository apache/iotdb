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

package org.apache.iotdb.db.metadata.storagegroup;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StorageGroupManagerTrieImpl implements IStorageGroupManager {

  private StorageGroupTrie storageGroupTrie;

  public StorageGroupManagerTrieImpl() {
    storageGroupTrie = new StorageGroupTrie();
  }

  public synchronized void init() {
    storageGroupTrie.init();
  }

  /** function for clearing MTree */
  public synchronized void clear() {
    if (storageGroupTrie != null) {
      storageGroupTrie.clear();
    }
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    IStorageGroupMNode storageGroupMNode = storageGroupTrie.setStorageGroup(path);
    SGMManager sgmManager = new SGMManager(storageGroupMNode);
    storageGroupMNode.setSGMManager(sgmManager);
    sgmManager.init();
  }

  @Override
  public List<IMeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException {
    IStorageGroupMNode storageGroupMNode = storageGroupTrie.deleteStorageGroup(path);
    return storageGroupMNode.getSGMManager().deleteStorageGroup();
  }

  @Override
  public SGMManager getSGMManager(PartialPath path) throws MetadataException {
    return storageGroupTrie.getStorageGroupNodeByPath(path).getSGMManager();
  }

  @Override
  public List<SGMManager> getInvolvedSGMManager(PartialPath pathPattern) throws MetadataException {
    List<SGMManager> result = new ArrayList<>();
    for (IStorageGroupMNode storageGroupMNode :
        storageGroupTrie.getInvolvedStorageGroupNodes(pathPattern)) {
      result.add(storageGroupMNode.getSGMManager());
    }
    return result;
  }

  @Override
  public List<SGMManager> getAllSGMManager() {
    List<SGMManager> result = new ArrayList<>();
    for (IStorageGroupMNode storageGroupMNode : storageGroupTrie.getAllStorageGroupNodes()) {
      result.add(storageGroupMNode.getSGMManager());
    }
    return result;
  }

  @Override
  public boolean isStorageGroup(PartialPath path) {
    return storageGroupTrie.isStorageGroup(path);
  }

  @Override
  public boolean checkStorageGroupByPath(PartialPath path) {
    return storageGroupTrie.checkStorageGroupByPath(path);
  }

  @Override
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return storageGroupTrie.getBelongedStorageGroup(path);
  }

  @Override
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return storageGroupTrie.getBelongedStorageGroups(pathPattern);
  }

  @Override
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return storageGroupTrie.getMatchedStorageGroups(pathPattern, isPrefixMatch);
  }

  @Override
  public List<PartialPath> getAllStorageGroupPaths() {
    return storageGroupTrie.getAllStorageGroupPaths();
  }

  @Override
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    return storageGroupTrie.groupPathByStorageGroup(path);
  }

  @Override
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return storageGroupTrie.getStorageGroupNum(pathPattern, isPrefixMatch);
  }

  @Override
  public int getStorageGroupNum(PartialPath pathPattern) throws MetadataException {
    return storageGroupTrie.getStorageGroupNum(pathPattern, false);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return storageGroupTrie.getStorageGroupNodeByStorageGroupPath(path);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return storageGroupTrie.getStorageGroupNodeByPath(path);
  }

  @Override
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return storageGroupTrie.getAllStorageGroupNodes();
  }
}
