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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
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

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

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
                  conf.getSchemaRegionConsensusProtocolClass(),
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(conf.getDataNodeId())
                      .setThisNode(
                          new TEndPoint(
                              conf.getInternalAddress(), conf.getSchemaRegionConsensusPort()))
                      .setRatisConfig(
                          RatisConfig.newBuilder()
                              .setSnapshot(
                                  RatisConfig.Snapshot.newBuilder()
                                      .setAutoTriggerThreshold(
                                          conf.getSchemaRatisConsensusSnapshotTriggerThreshold())
                                      .build())
                              .setLog(
                                  RatisConfig.Log.newBuilder()
                                      .setUnsafeFlushEnabled(
                                          conf.isSchemaRatisConsensusLogUnsafeFlushEnable())
                                      .setSegmentSizeMax(
                                          SizeInBytes.valueOf(
                                              conf.getSchemaRatisConsensusLogSegmentSizeMax()))
                                      .setPreserveNumsWhenPurge(
                                          conf.getSchemaRatisConsensusPreserveWhenPurge())
                                      .build())
                              .setGrpc(
                                  RatisConfig.Grpc.newBuilder()
                                      .setFlowControlWindow(
                                          SizeInBytes.valueOf(
                                              conf.getSchemaRatisConsensusGrpcFlowControlWindow()))
                                      .build())
                              .setRpc(
                                  RatisConfig.Rpc.newBuilder()
                                      .setTimeoutMin(
                                          TimeDuration.valueOf(
                                              conf
                                                  .getSchemaRatisConsensusLeaderElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setTimeoutMax(
                                          TimeDuration.valueOf(
                                              conf
                                                  .getSchemaRatisConsensusLeaderElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setRequestTimeout(
                                          TimeDuration.valueOf(
                                              conf.getSchemaRatisConsensusRequestTimeoutMs(),
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
                              .setClient(
                                  RatisConfig.Client.newBuilder()
                                      .setClientRequestTimeoutMillis(
                                          conf.getDataRatisConsensusRequestTimeoutMs())
                                      .setClientMaxRetryAttempt(
                                          conf.getDataRatisConsensusMaxRetryAttempts())
                                      .setClientRetryInitialSleepTimeMs(
                                          conf.getDataRatisConsensusInitialSleepTimeMs())
                                      .setClientRetryMaxSleepTimeMs(
                                          conf.getDataRatisConsensusMaxSleepTimeMs())
                                      .setCoreClientNumForEachNode(
                                          conf.getCoreClientNumForEachNode())
                                      .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
                                      .build())
                              .setImpl(
                                  RatisConfig.Impl.newBuilder()
                                      .setTriggerSnapshotFileSize(conf.getSchemaRatisLogMax())
                                      .build())
                              .setLeaderLogAppender(
                                  RatisConfig.LeaderLogAppender.newBuilder()
                                      .setBufferByteLimit(
                                          conf.getSchemaRatisConsensusLogAppenderBufferSizeMax())
                                      .build())
                              .build())
                      .setStorageDir(conf.getSchemaRegionConsensusDir())
                      .setConsensusGroupType(TConsensusGroupType.SchemaRegion)
                      .build(),
                  gid ->
                      new SchemaRegionStateMachine(
                          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) gid)))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                              conf.getSchemaRegionConsensusProtocolClass())));
    }
    return INSTANCE;
  }
}
