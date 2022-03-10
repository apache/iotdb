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
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StorageGroupManagerTreeImpl implements IStorageGroupManager {

  private StorageGroupTree storageGroupTree;

  public StorageGroupManagerTreeImpl() {
    storageGroupTree = new StorageGroupTree();
  }

  public synchronized void init() {
    storageGroupTree.init();
    // todo implement this as multi thread process
    for (SGMManager sgmManager : getAllSGMManager()) {
      sgmManager.init();
    }
  }

  /** function for clearing MTree */
  public synchronized void clear() {
    for (SGMManager sgmManager : getAllSGMManager()) {
      sgmManager.clear();
    }
    if (storageGroupTree != null) {
      storageGroupTree.clear();
    }
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    IStorageGroupMNode storageGroupMNode = storageGroupTree.setStorageGroup(path);
    SGMManager sgmManager = new SGMManager(storageGroupMNode);
    storageGroupMNode.setSGMManager(sgmManager);
    sgmManager.init();
  }

  @Override
  public SGMManager getSGMManager(PartialPath path) throws MetadataException {
    return storageGroupTree.getStorageGroupNodeByPath(path).getSGMManager();
  }

  @Override
  public List<SGMManager> getInvolvedSGMManager(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<SGMManager> result = new ArrayList<>();
    for (IStorageGroupMNode storageGroupMNode :
        storageGroupTree.getInvolvedStorageGroupNodes(pathPattern, isPrefixMatch)) {
      result.add(storageGroupMNode.getSGMManager());
    }
    return result;
  }

  @Override
  public List<SGMManager> getAllSGMManager() {
    List<SGMManager> result = new ArrayList<>();
    for (IStorageGroupMNode storageGroupMNode : storageGroupTree.getAllStorageGroupNodes()) {
      result.add(storageGroupMNode.getSGMManager());
    }
    return result;
  }

  @Override
  public List<SGMManager> deleteStorageGroup(List<PartialPath> storageGroups)
      throws MetadataException {
    List<SGMManager> sgmManagers = new ArrayList<>();
    for (PartialPath path : storageGroups) {
      sgmManagers.add(storageGroupTree.deleteStorageGroup(path).getSGMManager());
    }
    return sgmManagers;
  }

  @Override
  public boolean isStorageGroup(PartialPath path) {
    return storageGroupTree.isStorageGroup(path);
  }

  @Override
  public boolean checkStorageGroupByPath(PartialPath path) {
    return storageGroupTree.checkStorageGroupByPath(path);
  }

  @Override
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return storageGroupTree.getBelongedStorageGroup(path);
  }

  @Override
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return storageGroupTree.getBelongedStorageGroups(pathPattern);
  }

  @Override
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return storageGroupTree.getMatchedStorageGroups(pathPattern, isPrefixMatch);
  }

  @Override
  public List<PartialPath> getAllStorageGroupPaths() {
    return storageGroupTree.getAllStorageGroupPaths();
  }

  @Override
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    return storageGroupTree.groupPathByStorageGroup(path);
  }

  @Override
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return storageGroupTree.getStorageGroupNum(pathPattern, isPrefixMatch);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return storageGroupTree.getStorageGroupNodeByStorageGroupPath(path);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return storageGroupTree.getStorageGroupNodeByPath(path);
  }

  @Override
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return storageGroupTree.getAllStorageGroupNodes();
  }

  @Override
  public boolean isPathExist(PartialPath path) {
    return storageGroupTree.isPathExist(path);
  }

  @Override
  public Pair<Integer, List<SGMManager>> getNodesCountInGivenLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    Pair<Integer, Set<IStorageGroupMNode>> resultAboveSG =
        storageGroupTree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
    List<SGMManager> sgmManagers = new LinkedList<>();
    for (IStorageGroupMNode storageGroupMNode : resultAboveSG.right) {
      sgmManagers.add(storageGroupMNode.getSGMManager());
    }
    return new Pair<>(resultAboveSG.left, sgmManagers);
  }

  @Override
  public Pair<List<PartialPath>, List<SGMManager>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, MManager.StorageGroupFilter filter)
      throws MetadataException {
    Pair<List<PartialPath>, Set<IStorageGroupMNode>> resultAboveSG =
        storageGroupTree.getNodesListInGivenLevel(pathPattern, nodeLevel, filter);
    List<SGMManager> sgmManagers = new LinkedList<>();
    for (IStorageGroupMNode storageGroupMNode : resultAboveSG.right) {
      sgmManagers.add(storageGroupMNode.getSGMManager());
    }
    return new Pair<>(resultAboveSG.left, sgmManagers);
  }

  @Override
  public Pair<Set<String>, List<SGMManager>> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    Pair<Set<String>, Set<IStorageGroupMNode>> resultAboveSG =
        storageGroupTree.getChildNodePathInNextLevel(pathPattern);
    List<SGMManager> sgmManagers = new LinkedList<>();
    for (IStorageGroupMNode storageGroupMNode : resultAboveSG.right) {
      sgmManagers.add(storageGroupMNode.getSGMManager());
    }
    return new Pair<>(resultAboveSG.left, sgmManagers);
  }

  @Override
  public Pair<Set<String>, List<SGMManager>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    Pair<Set<String>, Set<IStorageGroupMNode>> resultAboveSG =
        storageGroupTree.getChildNodeNameInNextLevel(pathPattern);
    List<SGMManager> sgmManagers = new LinkedList<>();
    for (IStorageGroupMNode storageGroupMNode : resultAboveSG.right) {
      sgmManagers.add(storageGroupMNode.getSGMManager());
    }
    return new Pair<>(resultAboveSG.left, sgmManagers);
  }
}
