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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeOperator;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.rescon.disk.DirectoryChecker;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DataNodeShutdownHook extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeShutdownHook.class);

  private final TDataNodeLocation nodeLocation;
  private Thread watcherThread;

  public DataNodeShutdownHook(TDataNodeLocation nodeLocation) {
    super(ThreadName.DATANODE_SHUTDOWN_HOOK.getName());
    this.nodeLocation = nodeLocation;
  }

  private void startWatcher() {
    Thread hookThread = Thread.currentThread();
    watcherThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  Thread.sleep(10000);
                  StackTraceElement[] stackTrace = hookThread.getStackTrace();
                  StringBuilder stackTraceBuilder =
                      new StringBuilder("Stack trace of shutdown hook:\n");
                  for (StackTraceElement traceElement : stackTrace) {
                    stackTraceBuilder.append(traceElement.toString()).append("\n");
                  }
                  logger.info(stackTraceBuilder.toString());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            },
            "ShutdownHookWatcher");
    watcherThread.setDaemon(true);
    watcherThread.start();
  }

  @Override
  public void run() {
    logger.info("DataNode exiting...");
    AuditLogFields fields =
        new AuditLogFields(
            -1,
            null,
            null,
            AuditEventType.DN_SHUTDOWN,
            AuditLogOperation.CONTROL,
            PrivilegeType.SYSTEM,
            true,
            null,
            null);
    String logMessage = String.format("DataNode %s exiting...", nodeLocation);
    DNAuditLogger.getInstance().log(fields, () -> logMessage);

    startWatcher();
    // Stop external rpc service firstly.
    ExternalRPCService.getInstance().stop();

    // Reject write operations to make sure all tsfiles will be sealed
    CommonDescriptor.getInstance().getConfig().setStopping(true);
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
    // Wait all wal are flushed
    WALManager.getInstance().waitAllWALFlushed();

    // Wait all deletions are flushed
    DeletionResourceManager.exit();

    // Flush data to Tsfile and remove WAL log files
    if (!IoTDBDescriptor.getInstance()
        .getConfig()
        .getDataRegionConsensusProtocolClass()
        .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      StorageEngine.getInstance().syncCloseAllProcessor();
    }

    // We did this work because the RatisConsensus recovery mechanism is different from other
    // consensus algorithms, which will replace the underlying storage engine based on its
    // own
    // latest snapshot, while other consensus algorithms will not. This judgement ensures that
    // compaction work is not discarded even if there are frequent restarts
    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getDataRegionConsensusProtocolClass()
        .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      triggerSnapshotForAllDataRegion();
    }

    long startTime = System.currentTimeMillis();
    if (PipeDataNodeAgent.task().getPipeCount() != 0) {
      for (Map.Entry<String, PipeDataNodeRemainingEventAndTimeOperator> entry :
          PipeDataNodeSinglePipeMetrics.getInstance().remainingEventAndTimeOperatorMap.entrySet()) {
        boolean timeout = false;
        while (true) {
          if (entry.getValue().getRemainingNonHeartbeatEvents() == 0) {
            logger.info(
                "Successfully waited for pipe {} to finish.", entry.getValue().getPipeName());
            break;
          }
          if (System.currentTimeMillis() - startTime
              > PipeConfig.getInstance().getPipeMaxWaitFinishTime()) {
            timeout = true;
            break;
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Interrupted when waiting for pipe to finish");
          }
        }
        if (timeout) {
          logger.info("Timed out when waiting for pipes to finish, will break");
          break;
        }
      }
    }
    // Persist progress index before shutdown to accurate recovery after restart
    PipeDataNodeAgent.task().persistAllProgressIndex();
    // Shutdown all consensus pipe's receiver
    PipeDataNodeAgent.receiver().pipeConsensus().closeReceiverExecutor();

    // set encryption key to 16-byte zero.
    TSFileDescriptor.getInstance().getConfig().setEncryptKey(new byte[16]);

    // Actually stop all services started by the DataNode.
    // If we don't call this, services like the RestService are not stopped and I can't re-start
    // it.
    DataNode.getInstance().stop();

    // Set and report shutdown to cluster ConfigNode-leader
    if (!reportShutdownToConfigNodeLeader()) {
      logger.warn(
          "Failed to report DataNode's shutdown to ConfigNode. The cluster will still take the current DataNode as Running for a few seconds.");
    }

    // Clear lock file. All services should be shutdown before this line.
    DirectoryChecker.getInstance().deregisterAll();

    logger.info(
        "DataNode exits. Jvm memory usage: {}",
        MemUtils.bytesCntToStr(
            Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));

    watcherThread.interrupt();
  }

  private void triggerSnapshotForAllDataRegion() {
    DataRegionConsensusImpl.getInstance().getAllConsensusGroupIds().parallelStream()
        .forEach(
            id -> {
              try {
                DataRegionConsensusImpl.getInstance().triggerSnapshot(id, true);
              } catch (ConsensusException e) {
                logger.warn(
                    "Something wrong happened while calling consensus layer's "
                        + "triggerSnapshot API.",
                    e);
              }
            });
  }

  private boolean reportShutdownToConfigNodeLeader() {
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      return client.reportDataNodeShutdown(nodeLocation).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (ClientManagerException e) {
      logger.error("Failed to borrow ConfigNodeClient", e);
    } catch (TException e) {
      logger.error("Failed to report shutdown", e);
    }
    return false;
  }
}
