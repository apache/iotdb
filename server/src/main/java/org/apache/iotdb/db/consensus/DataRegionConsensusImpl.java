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
  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

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
                  conf.getDataRegionConsensusProtocolClass(),
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(conf.getDataNodeId())
                      .setThisNode(
                          new TEndPoint(
                              conf.getInternalAddress(), conf.getDataRegionConsensusPort()))
                      .setStorageDir(conf.getDataRegionConsensusDir())
                      .setIoTConsensusConfig(
                          IoTConsensusConfig.newBuilder()
                              .setRpc(
                                  RPC.newBuilder()
                                      .setConnectionTimeoutInMs(conf.getConnectionTimeoutInMS())
                                      .setRpcSelectorThreadNum(conf.getRpcSelectorThreadCount())
                                      .setRpcMinConcurrentClientNum(
                                          conf.getRpcMinConcurrentClientNum())
                                      .setRpcMaxConcurrentClientNum(
                                          conf.getRpcMaxConcurrentClientNum())
                                      .setRpcThriftCompressionEnabled(
                                          conf.isRpcThriftCompressionEnable())
                                      .setSelectorNumOfClientManager(
                                          conf.getSelectorNumOfClientManager())
                                      .setThriftServerAwaitTimeForStopService(
                                          conf.getThriftServerAwaitTimeForStopService())
                                      .setThriftMaxFrameSize(conf.getThriftMaxFrameSize())
                                      .build())
                              .setReplication(
                                  IoTConsensusConfig.Replication.newBuilder()
                                      .setWalThrottleThreshold(conf.getThrottleThreshold())
                                      .setAllocateMemoryForConsensus(
                                          conf.getAllocateMemoryForConsensus())
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
                                          conf.getDataRatisConsensusSnapshotTriggerThreshold())
                                      .build())
                              .setLog(
                                  RatisConfig.Log.newBuilder()
                                      .setUnsafeFlushEnabled(
                                          conf.isDataRatisConsensusLogUnsafeFlushEnable())
                                      .setSegmentSizeMax(
                                          SizeInBytes.valueOf(
                                              conf.getDataRatisConsensusLogSegmentSizeMax()))
                                      .setPreserveNumsWhenPurge(
                                          conf.getDataRatisConsensusPreserveWhenPurge())
                                      .build())
                              .setGrpc(
                                  RatisConfig.Grpc.newBuilder()
                                      .setFlowControlWindow(
                                          SizeInBytes.valueOf(
                                              conf.getDataRatisConsensusGrpcFlowControlWindow()))
                                      .build())
                              .setRpc(
                                  RatisConfig.Rpc.newBuilder()
                                      .setTimeoutMin(
                                          TimeDuration.valueOf(
                                              conf
                                                  .getDataRatisConsensusLeaderElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setTimeoutMax(
                                          TimeDuration.valueOf(
                                              conf
                                                  .getDataRatisConsensusLeaderElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setRequestTimeout(
                                          TimeDuration.valueOf(
                                              conf.getDataRatisConsensusRequestTimeoutMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMin(
                                          TimeDuration.valueOf(
                                              conf.getRatisFirstElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMax(
                                          TimeDuration.valueOf(
                                              conf.getRatisFirstElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .build())
                              .setLeaderLogAppender(
                                  RatisConfig.LeaderLogAppender.newBuilder()
                                      .setBufferByteLimit(
                                          conf.getDataRatisConsensusLogAppenderBufferSizeMax())
                                      .build())
                              .setRatisConsensus(
                                  RatisConfig.RatisConsensus.newBuilder()
                                      .setClientRequestTimeoutMillis(
                                          conf.getDataRatisConsensusRequestTimeoutMs())
                                      .setClientMaxRetryAttempt(
                                          conf.getDataRatisConsensusMaxRetryAttempts())
                                      .setClientRetryInitialSleepTimeMs(
                                          conf.getDataRatisConsensusInitialSleepTimeMs())
                                      .setClientRetryMaxSleepTimeMs(
                                          conf.getDataRatisConsensusMaxSleepTimeMs())
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
                              conf.getDataRegionConsensusProtocolClass())));
    }
    return INSTANCE;
  }
}
