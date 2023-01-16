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
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.SchemaRegionStateMachine;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;

import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * We can use SchemaRegionConsensusImpl.getInstance() to obtain a consensus layer reference for
 * schemaRegion's reading and writing
 */
public class SchemaRegionConsensusImpl {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConf();
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConf();

  private static IConsensus INSTANCE = null;

  private SchemaRegionConsensusImpl() {}

  // need to create instance before calling this method
  public static IConsensus getInstance() {
    return INSTANCE;
  }

  public static synchronized IConsensus setupAndGetInstance() {
    if (INSTANCE == null) {
      INSTANCE =
          ConsensusFactory.getConsensusImpl(
                  COMMON_CONFIG.getSchemaRegionConsensusProtocolClass().getProtocol(),
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(IOTDB_CONFIG.getDataNodeId())
                      .setThisNode(
                          new TEndPoint(
                              IOTDB_CONFIG.getDnInternalAddress(),
                              IOTDB_CONFIG.getDnSchemaRegionConsensusPort()))
                      .setRatisConfig(
                          RatisConfig.newBuilder()
                              .setSnapshot(
                                  RatisConfig.Snapshot.newBuilder()
                                      .setAutoTriggerThreshold(
                                          COMMON_CONFIG
                                              .getSchemaRegionRatisSnapshotTriggerThreshold())
                                      .build())
                              .setLog(
                                  RatisConfig.Log.newBuilder()
                                      .setUnsafeFlushEnabled(
                                          COMMON_CONFIG.isSchemaRegionRatisLogUnsafeFlushEnable())
                                      .setSegmentSizeMax(
                                          SizeInBytes.valueOf(
                                              COMMON_CONFIG
                                                  .getSchemaRegionRatisLogSegmentSizeMax()))
                                      .setPreserveNumsWhenPurge(
                                          COMMON_CONFIG.getSchemaRegionRatisPreserveLogsWhenPurge())
                                      .build())
                              .setGrpc(
                                  RatisConfig.Grpc.newBuilder()
                                      .setFlowControlWindow(
                                          SizeInBytes.valueOf(
                                              COMMON_CONFIG
                                                  .getSchemaRegionRatisGrpcFlowControlWindow()))
                                      .build())
                              .setRpc(
                                  RatisConfig.Rpc.newBuilder()
                                      .setTimeoutMin(
                                          TimeDuration.valueOf(
                                              COMMON_CONFIG
                                                  .getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setTimeoutMax(
                                          TimeDuration.valueOf(
                                              COMMON_CONFIG
                                                  .getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setRequestTimeout(
                                          TimeDuration.valueOf(
                                              COMMON_CONFIG.getSchemaRegionRatisRequestTimeoutMs(),
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
                                          COMMON_CONFIG.getSchemaRegionRatisRequestTimeoutMs())
                                      .setClientMaxRetryAttempt(
                                          COMMON_CONFIG.getSchemaRegionRatisMaxRetryAttempts())
                                      .setClientRetryInitialSleepTimeMs(
                                          COMMON_CONFIG.getSchemaRegionRatisInitialSleepTimeMs())
                                      .setClientRetryMaxSleepTimeMs(
                                          COMMON_CONFIG.getSchemaRegionRatisMaxSleepTimeMs())
                                      .setCoreClientNumForEachNode(
                                          IOTDB_CONFIG
                                              .getDnCoreClientCountForEachNodeInClientManager())
                                      .setMaxClientNumForEachNode(
                                          IOTDB_CONFIG
                                              .getDnMaxClientCountForEachNodeInClientManager())
                                      .build())
                              .setImpl(
                                  RatisConfig.Impl.newBuilder()
                                      .setTriggerSnapshotFileSize(
                                          COMMON_CONFIG.getSchemaRegionRatisLogMax())
                                      .build())
                              .setLeaderLogAppender(
                                  RatisConfig.LeaderLogAppender.newBuilder()
                                      .setBufferByteLimit(
                                          COMMON_CONFIG
                                              .getSchemaRegionRatisConsensusLogAppenderBufferSize())
                                      .build())
                              .build())
                      .setStorageDir(IOTDB_CONFIG.getSchemaRegionConsensusDir())
                      .build(),
                  gid ->
                      new SchemaRegionStateMachine(
                          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) gid)))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                              COMMON_CONFIG.getSchemaRegionConsensusProtocolClass())));
    }
    return INSTANCE;
  }
}
