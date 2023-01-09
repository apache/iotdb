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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class simulates the behaviour of configNode to manage the configs locally. The schema
 * configs include database and schema region. The data config is dataRegion.
 */
@Deprecated
public class LocalConfigNode {

  private static final Logger logger = LoggerFactory.getLogger(LocalConfigNode.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private volatile boolean initialized = false;

  private final IStorageGroupSchemaManager storageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final LocalSchemaPartitionTable schemaPartitionTable =
      LocalSchemaPartitionTable.getInstance();

  private final StorageEngine storageEngine = StorageEngine.getInstance();

  private final LocalDataPartitionInfo dataPartitionInfo = LocalDataPartitionInfo.getInstance();

  private LocalConfigNode() {
    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.error("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
  }

  // region LocalSchemaConfigManager SingleTone
  private static class LocalSchemaConfigManagerHolder {
    private static final LocalConfigNode INSTANCE = new LocalConfigNode();

    private LocalSchemaConfigManagerHolder() {}
  }

  public static LocalConfigNode getInstance() {
    return LocalSchemaConfigManagerHolder.INSTANCE;
  }

  // endregion

  // region Interfaces for LocalSchemaConfigManager init, force and clear
  public synchronized void init() {
    if (initialized) {
      return;
    }

    try {
      storageGroupSchemaManager.init();

      Map<PartialPath, List<SchemaRegionId>> recoveredLocalSchemaRegionInfo =
          schemaEngine.initForLocalConfigNode();
      schemaPartitionTable.init(recoveredLocalSchemaRegionInfo);

      // TODO: the judgment should be removed after old standalone removed
      if (!config.isClusterMode()) {
        Map<String, List<DataRegionId>> recoveredLocalDataRegionInfo =
            storageEngine.getLocalDataRegionInfo();
        dataPartitionInfo.init(recoveredLocalDataRegionInfo);
      }
    } catch (MetadataException | IOException e) {
      logger.error(
          "Cannot recover all MTree from file, we try to recover as possible as we can", e);
    }

    initialized = true;
  }

  public synchronized void clear() {
    if (!initialized) {
      return;
    }

    try {

      schemaPartitionTable.clear();
      schemaEngine.clear();
      storageGroupSchemaManager.clear();

      dataPartitionInfo.clear();

    } catch (IOException e) {
      logger.error("Error occurred when clearing LocalConfigNode:", e);
    }

    initialized = false;
  }

  // endregion

  // region Interfaces for database management

  // region Interfaces for database write operation

  /**
   * CREATE DATABASE of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    storageGroupSchemaManager.setStorageGroup(storageGroup);
    for (SchemaRegionId schemaRegionId : schemaPartitionTable.setStorageGroup(storageGroup)) {
      schemaEngine.createSchemaRegion(storageGroup, schemaRegionId);
    }

    if (!config.isEnableMemControl()) {
      MemTableManager.getInstance().addOrDeleteStorageGroup(1);
    }
  }

  private PartialPath ensureStorageGroup(PartialPath path) throws MetadataException {
    try {
      return getBelongedStorageGroup(path);
    } catch (StorageGroupNotSetException e) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw e;
      }
      PartialPath storageGroupPath =
          MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
      try {
        setStorageGroup(storageGroupPath);
        return storageGroupPath;
      } catch (StorageGroupAlreadySetException storageGroupAlreadySetException) {
        if (storageGroupAlreadySetException.isHasChild()) {
          // if setStorageGroup failure is because of child, the deviceNode should not be created.
          // Timeseries can't be created under a deviceNode without storageGroup.
          throw storageGroupAlreadySetException;
        }

        // concurrent timeseries creation may result concurrent ensureStorageGroup
        // it's ok that the storageGroup has already been set
        return getBelongedStorageGroup(path);
      }
    }
  }

  // endregion

  // region Interfaces for database info query

  /**
   * Get database name by path
   *
   * <p>e.g., root.sg1 is a database and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return database in the given path
   */
  public PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException {
    return storageGroupSchemaManager.getBelongedStorageGroup(path);
  }

  // endregion

  // endregion

  // region Interfaces for SchemaRegionId Management

  // This interface involves database and schema region auto creation
  public SchemaRegionId getBelongedSchemaRegionIdWithAutoCreate(PartialPath path)
      throws MetadataException {
    PartialPath storageGroup = ensureStorageGroup(path);
    SchemaRegionId schemaRegionId = schemaPartitionTable.getSchemaRegionId(storageGroup, path);
    if (schemaRegionId == null) {
      schemaPartitionTable.setStorageGroup(storageGroup);
      schemaRegionId = schemaPartitionTable.getSchemaRegionId(storageGroup, path);
    }
    ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
    if (schemaRegion == null) {
      schemaEngine.createSchemaRegion(storageGroup, schemaRegionId);
    }
    return schemaRegionId;
  }

  /**
   * Get the target SchemaRegionIds, which will be involved/covered by the given pathPattern. The
   * path may contain wildcards, * or **. This method is the first step when there's a task on
   * multiple paths represented by the given pathPattern. If isPrefixMatch, all databases under the
   * prefixPath that matches the given pathPattern will be collected.
   */
  public List<SchemaRegionId> getInvolvedSchemaRegionIds(PartialPath pathPattern)
      throws MetadataException {
    List<SchemaRegionId> result = new ArrayList<>();
    for (PartialPath storageGroup :
        storageGroupSchemaManager.getInvolvedStorageGroups(pathPattern)) {
      result.addAll(schemaPartitionTable.getInvolvedSchemaRegionIds(storageGroup, pathPattern));
    }
    return result;
  }

  // endregion

  public TSStatus executeMergeOperation() {
    try {
      storageEngine.mergeAll();
    } catch (StorageEngineException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus executeFlushOperation(TFlushReq tFlushReq) {
    return storageEngine.operateFlush(tFlushReq);
  }

  public TSStatus executeClearCacheOperation() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus executeLoadConfigurationOperation() {
    try {
      IoTDBDescriptor.getInstance().loadHotModifiedProps();
    } catch (QueryProcessException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus executeSetSystemStatus(NodeStatus status) {
    try {
      CommonDescriptor.getInstance().getConfig().setNodeStatus(status);
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }
}
