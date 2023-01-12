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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig.RPC;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.config.RatisConfig.Snapshot;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.DataRegionStateMachine;
import org.apache.iotdb.db.engine.StorageEngine;

import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * We can use DataRegionConsensusImpl.getInstance() to obtain a consensus layer reference for
 * dataRegion's reading and writing
 */
public class DataRegionConsensusImpl {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static IConsensus INSTANCE = null;

  private DataRegionConsensusImpl() {}

  // need to create instance before calling this method
  public static IConsensus getInstance() {
    return INSTANCE;
  }

  public static synchronized IConsensus setupAndGetInstance() {
    if (INSTANCE == null) {
      INSTANCE =
          ConsensusFactory.getConsensusImpl(
              COMMON_CONFIG.getDataRegionConsensusProtocolClass().getProtocol(),
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(IOTDB_CONFIG.getDataNodeId())
                      .setThisNode(
                          new TEndPoint(
                              IOTDB_CONFIG.getDnInternalAddress(), IOTDB_CONFIG.getDnDataRegionConsensusPort()))
                      .setStorageDir(IOTDB_CONFIG.getDataRegionConsensusDir())
                      .setIoTConsensusConfig(
                          IoTConsensusConfig.newBuilder()
                              .setRpc(
                                  RPC.newBuilder()
                                      .setConnectionTimeoutInMs(IOTDB_CONFIG.getDnConnectionTimeoutInMS())
                                      .setRpcSelectorThreadNum(IOTDB_CONFIG.getDnRpcSelectorThreadCount())
                                      .setRpcMinConcurrentClientNum(
                                          IOTDB_CONFIG.getDnRpcMinConcurrentClientNum())
                                      .setRpcMaxConcurrentClientNum(
                                          IOTDB_CONFIG.getDnRpcMaxConcurrentClientNum())
                                      .setRpcThriftCompressionEnabled(
                                          IOTDB_CONFIG.isDnRpcThriftCompressionEnable())
                                      .setSelectorNumOfClientManager(
                                          IOTDB_CONFIG.getDnSelectorThreadCountOfClientManager())
                                      .setThriftServerAwaitTimeForStopService(
                                          IOTDB_CONFIG.getThriftServerAwaitTimeForStopService())
                                      .setThriftMaxFrameSize(IOTDB_CONFIG.getDnThriftMaxFrameSize())
                                      .setCoreClientNumForEachNode(
                                          IOTDB_CONFIG.getDnCoreClientCountForEachNodeInClientManager())
                                      .setMaxClientNumForEachNode(IOTDB_CONFIG.getDnMaxClientCountForEachNodeInClientManager())
                                      .build())
                              .setReplication(
                                  IoTConsensusConfig.Replication.newBuilder()
                                      .setWalThrottleThreshold(COMMON_CONFIG.getIotConsensusThrottleThresholdInByte())
                                      .setAllocateMemoryForConsensus(
                                        COMMON_CONFIG.getAllocateMemoryForConsensus())
                                      .build())
                              .build())
                      .setRatisConfig(
                          RatisConfig.newBuilder()
                              // An empty log is committed after each restart, even if no data is
                              // written. This setting ensures that compaction work is not discarded
                              // even if there are frequent restarts
                              .setSnapshot(
                                  Snapshot.newBuilder()
                                      .setCreationGap(1)
                                      .setAutoTriggerThreshold(
                                        COMMON_CONFIG.getDataRegionRatisSnapshotTriggerThreshold())
                                      .build())
                              .setLog(
                                  RatisConfig.Log.newBuilder()
                                      .setUnsafeFlushEnabled(
                                        COMMON_CONFIG.isDataRegionRatisLogUnsafeFlushEnable())
                                      .setSegmentSizeMax(
                                          SizeInBytes.valueOf(
                                            COMMON_CONFIG.getDataRegionRatisLogSegmentSizeMax()))
                                      .setPreserveNumsWhenPurge(
                                        COMMON_CONFIG.getDataRegionRatisPreserveLogsWhenPurge())
                                      .build())
                              .setGrpc(
                                  RatisConfig.Grpc.newBuilder()
                                      .setFlowControlWindow(
                                          SizeInBytes.valueOf(
                                            COMMON_CONFIG.getDataRegionRatisGrpcFlowControlWindow()))
                                      .build())
                              .setRpc(
                                  RatisConfig.Rpc.newBuilder()
                                      .setTimeoutMin(
                                          TimeDuration.valueOf(
                                            COMMON_CONFIG
                                                  .getDataRegionRatisRpcLeaderElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setTimeoutMax(
                                          TimeDuration.valueOf(
                                            COMMON_CONFIG
                                                  .getDataRegionRatisRpcLeaderElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setRequestTimeout(
                                          TimeDuration.valueOf(
                                            COMMON_CONFIG.getDataRegionRatisRequestTimeoutMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMin(
                                          TimeDuration.valueOf(
                                            COMMON_CONFIG.getRatisFirstElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMax(
                                          TimeDuration.valueOf(
                                            COMMON_CONFIG.getRatisFirstElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .build())
                              .setClient(
                                  RatisConfig.Client.newBuilder()
                                      .setClientRequestTimeoutMillis(
                                        COMMON_CONFIG.getDataRegionRatisRequestTimeoutMs())
                                      .setClientMaxRetryAttempt(
                                        COMMON_CONFIG.getDataRegionRatisMaxRetryAttempts())
                                      .setClientRetryInitialSleepTimeMs(
                                        COMMON_CONFIG.getDataRegionRatisInitialSleepTimeMs())
                                      .setClientRetryMaxSleepTimeMs(
                                        COMMON_CONFIG.getDataRegionRatisMaxSleepTimeMs())
                                      .setCoreClientNumForEachNode(
                                        IOTDB_CONFIG.getDnCoreClientCountForEachNodeInClientManager())
                                      .setMaxClientNumForEachNode(IOTDB_CONFIG.getDnMaxClientCountForEachNodeInClientManager())
                                      .build())
                              .setImpl(
                                  RatisConfig.Impl.newBuilder()
                                      .setTriggerSnapshotFileSize(COMMON_CONFIG.getDataRegionRatisLogMax())
                                      .build())
                              .setLeaderLogAppender(
                                  RatisConfig.LeaderLogAppender.newBuilder()
                                      .setBufferByteLimit(
                                        COMMON_CONFIG.getDataRegionRatisConsensusLogAppenderBufferSize())
                                      .build())
                              .build())
                      .build(),
                  gid ->
                      new DataRegionStateMachine(
                          StorageEngine.getInstance().getDataRegion((DataRegionId) gid)))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            COMMON_CONFIG.getDataRegionConsensusProtocolClass())));
    }
    return INSTANCE;
  }
}
