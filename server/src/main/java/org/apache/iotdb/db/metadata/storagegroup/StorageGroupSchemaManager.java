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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeAboveSG;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.metadata.MetadataConstant.TTL_LOG;

// This class implements all the interfaces for storage group management. The MTreeAboveSg is used
// to manage all the storage groups and MNodes above storage group.
public class StorageGroupSchemaManager implements IStorageGroupSchemaManager {

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupSchemaManager.class);

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private MTreeAboveSG mtree;

  private boolean isRecover = true;

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
    isRecover = true;
    mtree.init();
    File dir = new File(config.getSchemaDir());
    File[] sgDirs = dir.listFiles((dir1, name) -> name.startsWith(PATH_ROOT + PATH_SEPARATOR));
    if (sgDirs != null) {
      for (File sgDir : sgDirs) {
        try {
          setStorageGroup(new PartialPath(sgDir.getName()));
          File ttlLog = new File(sgDir, TTL_LOG);
          if (!ttlLog.exists()) {
            continue;
          }
          try (TTLLogReader logReader = new TTLLogReader(sgDir.getAbsolutePath(), TTL_LOG)) {
            SetTTLPlan plan;
            while (logReader.hasNext()) {
              plan = (SetTTLPlan) logReader.next();
              setTTL(plan.getStorageGroup(), plan.getDataTTL());
            }
          }
        } catch (MetadataException | IOException e) {
          logger.error("Cannot recover storage group from dir {} because", sgDir.getName(), e);
        }
      }
    }
    isRecover = false;
  }

  public synchronized void clear() {
    if (mtree != null) {
      mtree.clear();
    }
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {
    mtree.setStorageGroup(path);
    String sgDirPath = config.getSchemaDir() + File.separator + path.getFullPath();
    File sgSchemaFolder = SystemFileFactory.INSTANCE.getFile(sgDirPath);
    if (!sgSchemaFolder.exists()) {
      if (sgSchemaFolder.mkdirs()) {
        logger.info("create storage group schema folder {}", sgDirPath);
      } else {
        logger.error("create storage group schema folder {} failed.", sgDirPath);
        throw new SchemaDirCreationFailureException(sgDirPath);
      }
    }
  }

  @Override
  public synchronized void deleteStorageGroup(PartialPath storageGroup) throws MetadataException {
    mtree.deleteStorageGroup(storageGroup);
    File sgDir = new File(config.getSchemaDir() + File.separator + storageGroup.getFullPath());
    if (sgDir.delete()) {
      logger.info("delete storage group folder {}", sgDir.getAbsolutePath());
    } else {
      logger.info("delete storage group folder {} failed.", sgDir.getAbsolutePath());
      throw new MetadataException(
          String.format("Failed to delete storage group folder %s", sgDir.getAbsolutePath()));
    }
  }

  @Override
  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    mtree.getStorageGroupNodeByStorageGroupPath(storageGroup).setDataTTL(dataTTL);
    if (!isRecover) {
      String sgDir = config.getSchemaDir() + File.separator + storageGroup.getFullPath();
      try (TTLLogWriter logWriter = new TTLLogWriter(sgDir, TTL_LOG)) {
        logWriter.setTTL(storageGroup, dataTTL);
      }
    }
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
  public Pair<Integer, Set<PartialPath>> getNodesCountInGivenLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    return mtree.getNodesCountInGivenLevel(pathPattern, level, isPrefixMatch);
  }

  @Override
  public Pair<List<PartialPath>, Set<PartialPath>> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, SchemaEngine.StorageGroupFilter filter)
      throws MetadataException {
    return mtree.getNodesListInGivenLevel(pathPattern, nodeLevel, filter);
  }

  @Override
  public Pair<Set<String>, Set<PartialPath>> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getChildNodePathInNextLevel(pathPattern);
  }

  @Override
  public Pair<Set<String>, Set<PartialPath>> getChildNodeNameInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    return mtree.getChildNodeNameInNextLevel(pathPattern);
  }

  @TestOnly
  public static StorageGroupSchemaManager getNewInstanceForTest() {
    return new StorageGroupSchemaManager();
  }
}
