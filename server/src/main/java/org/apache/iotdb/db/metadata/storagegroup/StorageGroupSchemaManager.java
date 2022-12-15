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

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.ConfigMTree;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Never support restart
// This class implements all the interfaces for database management. The MTreeAboveSg is used
// to manage all the databases and MNodes above database.
@Deprecated
public class StorageGroupSchemaManager implements IStorageGroupSchemaManager {

  private ConfigMTree mtree;

  private static class StorageGroupManagerHolder {

    private static final StorageGroupSchemaManager INSTANCE = new StorageGroupSchemaManager();

    private StorageGroupManagerHolder() {}
  }

  public static StorageGroupSchemaManager getInstance() {
    return StorageGroupManagerHolder.INSTANCE;
  }

  private StorageGroupSchemaManager() {}

  public synchronized void init() throws MetadataException, IOException {

    mtree = new ConfigMTree();

    recoverLog();
  }

  public void recoverLog() throws IOException {
    //    File logFile = new File(config.getSchemaDir(), STORAGE_GROUP_LOG);
    //    if (!logFile.exists()) {
    //      return;
    //    }
    //    try (StorageGroupLogReader logReader =
    //        new StorageGroupLogReader(config.getSchemaDir(), STORAGE_GROUP_LOG)) {
    //      PhysicalPlan plan;
    //      while (logReader.hasNext()) {
    //        plan = logReader.next();
    //        try {
    //          switch (plan.getOperatorType()) {
    //            case SET_STORAGE_GROUP:
    //              SetStorageGroupPlan setStorageGroupPlan = (SetStorageGroupPlan) plan;
    //              setStorageGroup(setStorageGroupPlan.getPath());
    //              break;
    //            case DELETE_STORAGE_GROUP:
    //              DeleteStorageGroupPlan deleteStorageGroupPlan = (DeleteStorageGroupPlan) plan;
    //              deleteStorageGroup(deleteStorageGroupPlan.getPaths().get(0));
    //              break;
    //            case TTL:
    //              SetTTLPlan setTTLPlan = (SetTTLPlan) plan;
    //              setTTL(setTTLPlan.getStorageGroup(), setTTLPlan.getDataTTL());
    //              break;
    //            default:
    //              logger.error("Unrecognizable command {}", plan.getOperatorType());
    //          }
    //        } catch (MetadataException | IOException e) {
    //          logger.error("Error occurred while redo database log", e);
    //        }
    //      }
    //    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void forceLog() {}

  public synchronized void clear() throws IOException {

    if (mtree != null) {
      mtree.clear();
    }
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    mtree.setStorageGroup(path);
  }

  @Override
  public IStorageGroupMNode ensureStorageGroupByStorageGroupPath(PartialPath storageGroup)
      throws MetadataException {
    try {
      return mtree.getStorageGroupNodeByStorageGroupPath(storageGroup);
    } catch (StorageGroupNotSetException e) {
      try {
        setStorageGroup(storageGroup);
      } catch (StorageGroupAlreadySetException storageGroupAlreadySetException) {
        // do nothing
        // concurrent timeseries creation may result concurrent ensureStorageGroup
        // it's ok that the storageGroup has already been set

        if (storageGroupAlreadySetException.isHasChild()) {
          // if setStorageGroup failure is because of child, the deviceNode should not be created.
          // Timeseries can't be created under a deviceNode without storageGroup.
          throw storageGroupAlreadySetException;
        }
      }

      return mtree.getStorageGroupNodeByStorageGroupPath(storageGroup);
    }
  }

  @Override
  public synchronized void deleteStorageGroup(PartialPath storageGroup) throws MetadataException {
    mtree.deleteStorageGroup(storageGroup);
  }

  @Override
  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    mtree.getStorageGroupNodeByStorageGroupPath(storageGroup).setDataTTL(dataTTL);
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
  public List<PartialPath> getInvolvedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    return mtree.getInvolvedStorageGroupNodes(pathPattern, isPrefixMatch);
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
  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, boolean isPrefixMatch) throws MetadataException {
    return mtree.getNodesListInGivenLevel(pathPattern, nodeLevel, isPrefixMatch);
  }

  @Override
  public Pair<Set<TSchemaNode>, Set<PartialPath>> getChildNodePathInNextLevel(
      PartialPath pathPattern) throws MetadataException {
    return mtree.getChildNodePathInNextLevel(pathPattern);
  }

  @Override
  public Pair<Set<String>, Set<PartialPath>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getChildNodeNameInNextLevel(pathPattern);
  }
}
