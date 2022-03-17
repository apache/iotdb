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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeAboveSG;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

// This class implements all the interfaces for storage group management. The MTreeAboveSg is used
// to manage all the storage groups and MNodes above storage group.
public class StorageGroupSchemaManager implements IStorageGroupSchemaManager {

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupSchemaManager.class);

  private MTreeAboveSG mtree;

  private static class StorageGroupManagerHolder {

    private static final StorageGroupSchemaManager INSTANCE = new StorageGroupSchemaManager();

    private StorageGroupManagerHolder() {}
  }

  public static StorageGroupSchemaManager getInstance() {
    return StorageGroupManagerHolder.INSTANCE;
  }

  private StorageGroupSchemaManager() {
    mtree = new MTreeAboveSG();
  }

  public synchronized void init() {
    mtree.init();
    File dir = new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir());
    File[] sgDirs = dir.listFiles((dir1, name) -> name.startsWith(PATH_ROOT + PATH_SEPARATOR));
    if (sgDirs != null) {
      for (File sgDir : sgDirs) {
        try {
          setStorageGroup(new PartialPath(sgDir.getName()));
        } catch (MetadataException e) {
          logger.error("Cannot recover storage group from dir {} because", sgDir.getName(), e);
        }
      }
    }
  }

  public synchronized void clear() {
    if (mtree != null) {
      mtree.clear();
    }
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    mtree.setStorageGroup(path);
  }

  @Override
  public SGMManager getBelongedSGMManager(PartialPath path) throws MetadataException {
    return mtree.getStorageGroupNodeByPath(path).getSGMManager();
  }

  @Override
  public SGMManager getSGMManagerByStorageGroupPath(PartialPath path) throws MetadataException {
    return getStorageGroupNodeByStorageGroupPath(path).getSGMManager();
  }

  @Override
  public List<SGMManager> getInvolvedSGMManagers(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<SGMManager> result = new ArrayList<>();
    for (IStorageGroupMNode storageGroupMNode :
        mtree.getInvolvedStorageGroupNodes(pathPattern, isPrefixMatch)) {
      result.add(storageGroupMNode.getSGMManager());
    }
    return result;
  }

  @Override
  public List<SGMManager> getAllSGMManagers() {
    List<SGMManager> result = new ArrayList<>();
    for (IStorageGroupMNode storageGroupMNode : mtree.getAllStorageGroupNodes()) {
      result.add(storageGroupMNode.getSGMManager());
    }
    return result;
  }

  @Override
  public synchronized void deleteStorageGroup(PartialPath storageGroup) throws MetadataException {
    mtree.deleteStorageGroup(storageGroup);
  }

  @Override
  public boolean isStorageGroup(PartialPath path) {
    return mtree.isStorageGroup(path);
  }

  @Override
  public boolean checkStorageGroupByPath(PartialPath path) {
    return mtree.checkStorageGroupByPath(path);
  }

  @Override
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return mtree.getBelongedStorageGroup(path);
  }

  @Override
  public List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getBelongedStorageGroups(pathPattern);
  }

  @Override
  public List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getMatchedStorageGroups(pathPattern, isPrefixMatch);
  }

  @Override
  public List<PartialPath> getAllStorageGroupPaths() {
    return mtree.getAllStorageGroupPaths();
  }

  @Override
  public Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path)
      throws MetadataException {
    return mtree.groupPathByStorageGroup(path);
  }

  @Override
  public int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getStorageGroupNum(pathPattern, isPrefixMatch);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return mtree.getStorageGroupNodeByStorageGroupPath(path);
  }

  @Override
  public IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getStorageGroupNodeByPath(path);
  }

  @Override
  public List<IStorageGroupMNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  @Override
  public boolean isStorageGroupAlreadySet(PartialPath path) {
    return mtree.isStorageGroupAlreadySet(path);
  }

  @Override
  public Pair<Integer, List<SGMManager>> getNodesCountInGivenLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    Pair<Integer, Set<IStorageGroupMNode>> resultAboveSG =
        mtree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
    List<SGMManager> sgmManagers = new LinkedList<>();
    for (IStorageGroupMNode storageGroupMNode : resultAboveSG.right) {
      sgmManagers.add(storageGroupMNode.getSGMManager());
    }
    return new Pair<>(resultAboveSG.left, sgmManagers);
  }

  @Override
  public Pair<List<PartialPath>, List<SGMManager>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, SchemaEngine.StorageGroupFilter filter)
      throws MetadataException {
    Pair<List<PartialPath>, Set<IStorageGroupMNode>> resultAboveSG =
        mtree.getNodesListInGivenLevel(pathPattern, nodeLevel, filter);
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
        mtree.getChildNodePathInNextLevel(pathPattern);
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
        mtree.getChildNodeNameInNextLevel(pathPattern);
    List<SGMManager> sgmManagers = new LinkedList<>();
    for (IStorageGroupMNode storageGroupMNode : resultAboveSG.right) {
      sgmManagers.add(storageGroupMNode.getSGMManager());
    }
    return new Pair<>(resultAboveSG.left, sgmManagers);
  }

  @Override
  public String getMetadataInString() {
    return mtree.toString();
  }

  @TestOnly
  public static StorageGroupSchemaManager getNewInstanceForTest() {
    return new StorageGroupSchemaManager();
  }
}
