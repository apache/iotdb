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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

      Map<PartialPath, List<SchemaRegionId>> recoveredLocalSchemaRegionInfo =
          schemaEngine.initForLocalConfigNode();
      schemaPartitionTable.init(recoveredLocalSchemaRegionInfo);

      // TODO: the judgment should be removed after old standalone removed
      if (!config.isClusterMode()) {
        Map<String, List<DataRegionId>> recoveredLocalDataRegionInfo =
            storageEngine.getLocalDataRegionInfo();
        dataPartitionInfo.init(recoveredLocalDataRegionInfo);
      }
    } catch (MetadataException e) {
      logger.error(
          "Cannot recover all MTree from file, we try to recover as possible as we can", e);
    }

    initialized = true;
  }

  public synchronized void clear() {
    if (!initialized) {
      return;
    }

    schemaPartitionTable.clear();
    schemaEngine.clear();

    dataPartitionInfo.clear();

    initialized = false;
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
