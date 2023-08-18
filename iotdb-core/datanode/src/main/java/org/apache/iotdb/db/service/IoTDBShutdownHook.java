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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.SchemaEngineMode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.rescon.disk.DirectoryChecker;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IoTDBShutdownHook extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBShutdownHook.class);

  public IoTDBShutdownHook() {
    super(ThreadName.IOTDB_SHUTDOWN_HOOK.getName());
  }

  @Override
  public void run() {
    // close rocksdb if possible to avoid lose data
    if (SchemaEngineMode.valueOf(CommonDescriptor.getInstance().getConfig().getSchemaEngineMode())
        .equals(SchemaEngineMode.Rocksdb_based)) {
      SchemaEngine.getInstance().clear();
    }

    // reject write operations to make sure all tsfiles will be sealed
    CommonDescriptor.getInstance().getConfig().setStopping(true);
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
    // wait all wal are flushed
    WALManager.getInstance().waitAllWALFlushed();

    // flush data to Tsfile and remove WAL log files
    if (!IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      StorageEngine.getInstance().syncCloseAllProcessor();
    }
    WALManager.getInstance().deleteOutdatedWALFiles();

    // We did this work because the RatisConsensus recovery mechanism is different from other
    // consensus algorithms, which will replace the underlying storage engine based on its
    // own
    // latest snapshot, while other consensus algorithms will not. This judgement ensures that
    // compaction work is not discarded even if there are frequent restarts
    if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()
        && IoTDBDescriptor.getInstance()
            .getConfig()
            .getDataRegionConsensusProtocolClass()
            .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      DataRegionConsensusImpl.getInstance()
          .getAllConsensusGroupIds()
          .parallelStream()
          .forEach(
              id -> {
                try {
                  DataRegionConsensusImpl.getInstance().triggerSnapshot(id);
                } catch (ConsensusException e) {
                  logger.warn(
                      "Something wrong happened while calling consensus layer's "
                          + "triggerSnapshot API.",
                      e);
                }
              });
    }

    // close consensusImpl
    try {
      SchemaRegionConsensusImpl.getInstance().stop();
      DataRegionConsensusImpl.getInstance().stop();
    } catch (IOException e) {
      logger.error("Stop ConsensusImpl error in IoTDBShutdownHook", e);
    }

    // clear lock file
    DirectoryChecker.getInstance().deregisterAll();

    // Set and report shutdown to cluster ConfigNode-leader
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Unknown);
    boolean isReportSuccess = false;
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      isReportSuccess =
          client.reportDataNodeShutdown(DataNode.generateDataNodeLocation()).getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (ClientManagerException e) {
      logger.error("Failed to borrow ConfigNodeClient", e);
    } catch (TException e) {
      logger.error("Failed to report shutdown", e);
    }
    if (!isReportSuccess) {
      logger.error(
          "Reporting DataNode shutdown failed. The cluster will still take the current DataNode as Running for a few seconds.");
    }

    if (logger.isInfoEnabled()) {
      logger.info(
          "IoTDB exits. Jvm memory usage: {}",
          MemUtils.bytesCntToStr(
              Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }
  }
}
