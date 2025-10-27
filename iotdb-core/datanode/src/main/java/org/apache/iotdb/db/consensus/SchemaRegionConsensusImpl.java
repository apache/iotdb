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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.schemaregion.SchemaRegionStateMachine;
import org.apache.iotdb.db.schemaengine.SchemaEngine;

import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * We can use SchemaRegionConsensusImpl.getInstance() to obtain a consensus layer reference for
 * schemaRegion's reading and writing
 */
public class SchemaRegionConsensusImpl {

  private SchemaRegionConsensusImpl() {
    // do nothing
  }

  public static IConsensus getInstance() {
    return SchemaRegionConsensusImplHolder.INSTANCE;
  }

  public static void reinitializeStatics() {
    SchemaRegionConsensusImplHolder.reinitializeStatics();
  }

  private static class SchemaRegionConsensusImplHolder {

    private static final IoTDBConfig CONF = IoTDBDescriptor.getInstance().getConfig();
    private static final CommonConfig COMMON_CONF = CommonDescriptor.getInstance().getConfig();
    private static IConsensus INSTANCE;

    // Make sure both statics are initialized.
    static {
      reinitializeStatics();
    }

    private static void reinitializeStatics() {
      INSTANCE =
          ConsensusFactory.getConsensusImpl(
                  CONF.getSchemaRegionConsensusProtocolClass(),
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(CONF.getDataNodeId())
                      .setThisNode(
                          new TEndPoint(
                              CONF.getInternalAddress(), CONF.getSchemaRegionConsensusPort()))
                      .setConsensusGroupType(TConsensusGroupType.SchemaRegion)
                      .setRatisConfig(
                          RatisConfig.newBuilder()
                              .setUtils(
                                  RatisConfig.Utils.newBuilder()
                                      .setTransferLeaderTimeoutMs(
                                          CONF.getRatisTransferLeaderTimeoutMs())
                                      .build())
                              .setSnapshot(
                                  RatisConfig.Snapshot.newBuilder()
                                      .setAutoTriggerThreshold(
                                          CONF.getSchemaRatisConsensusSnapshotTriggerThreshold())
                                      .build())
                              .setLog(
                                  RatisConfig.Log.newBuilder()
                                      .setUnsafeFlushEnabled(
                                          CONF.isSchemaRatisConsensusLogUnsafeFlushEnable())
                                      .setForceSyncNum(
                                          CONF.getSchemaRatisConsensusLogForceSyncNum())
                                      .setSegmentSizeMax(
                                          SizeInBytes.valueOf(
                                              CONF.getSchemaRatisConsensusLogSegmentSizeMax()))
                                      .setPreserveNumsWhenPurge(
                                          CONF.getSchemaRatisConsensusPreserveWhenPurge())
                                      .build())
                              .setGrpc(
                                  RatisConfig.Grpc.newBuilder()
                                      .setFlowControlWindow(
                                          SizeInBytes.valueOf(
                                              CONF.getSchemaRatisConsensusGrpcFlowControlWindow()))
                                      .setLeaderOutstandingAppendsMax(
                                          CONF
                                              .getSchemaRatisConsensusGrpcLeaderOutstandingAppendsMax())
                                      .setEnableSSL(COMMON_CONF.isEnableInternalSSL())
                                      .setSslKeyStorePath(COMMON_CONF.getKeyStorePath())
                                      .setSslKeyStorePassword(COMMON_CONF.getKeyStorePwd())
                                      .setSslTrustStorePath(COMMON_CONF.getTrustStorePath())
                                      .setSslTrustStorePassword(COMMON_CONF.getTrustStorePwd())
                                      .build())
                              .setRpc(
                                  RatisConfig.Rpc.newBuilder()
                                      .setTimeoutMin(
                                          TimeDuration.valueOf(
                                              CONF
                                                  .getSchemaRatisConsensusLeaderElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setTimeoutMax(
                                          TimeDuration.valueOf(
                                              CONF
                                                  .getSchemaRatisConsensusLeaderElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setRequestTimeout(
                                          TimeDuration.valueOf(
                                              CONF.getSchemaRatisConsensusRequestTimeoutMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setSlownessTimeout(
                                          TimeDuration.valueOf(
                                              CONF.getSchemaRatisConsensusRequestTimeoutMs() * 6,
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMin(
                                          TimeDuration.valueOf(
                                              CONF.getRatisFirstElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMax(
                                          TimeDuration.valueOf(
                                              CONF.getRatisFirstElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .build())
                              .setClient(
                                  RatisConfig.Client.newBuilder()
                                      .setClientRequestTimeoutMillis(
                                          CONF.getDataRatisConsensusRequestTimeoutMs())
                                      .setClientMaxRetryAttempt(
                                          CONF.getDataRatisConsensusMaxRetryAttempts())
                                      .setClientRetryInitialSleepTimeMs(
                                          CONF.getDataRatisConsensusInitialSleepTimeMs())
                                      .setClientRetryMaxSleepTimeMs(
                                          CONF.getDataRatisConsensusMaxSleepTimeMs())
                                      .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                                      .build())
                              .setImpl(
                                  RatisConfig.Impl.newBuilder()
                                      .setRaftLogSizeMaxThreshold(CONF.getSchemaRatisLogMax())
                                      .setForceSnapshotInterval(
                                          CONF.getSchemaRatisPeriodicSnapshotInterval())
                                      .setRetryTimesMax(10)
                                      .setRetryWaitMillis(CONF.getConnectionTimeoutInMS() / 10)
                                      .build())
                              .setLeaderLogAppender(
                                  RatisConfig.LeaderLogAppender.newBuilder()
                                      .setBufferByteLimit(
                                          CONF.getSchemaRatisConsensusLogAppenderBufferSizeMax())
                                      .build())
                              .setRead(
                                  RatisConfig.Read.newBuilder()
                                      .setReadOption(RatisConfig.Read.Option.LINEARIZABLE)
                                      // use thrift connection timeout to unify read timeout
                                      .setReadTimeout(
                                          TimeDuration.valueOf(
                                              CONF.getConnectionTimeoutInMS(),
                                              TimeUnit.MILLISECONDS))
                                      .build())
                              .build())
                      .setStorageDir(CONF.getSchemaRegionConsensusDir())
                      .build(),
                  gid ->
                      new SchemaRegionStateMachine(
                          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) gid)))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                              CONF.getSchemaRegionConsensusProtocolClass())));
    }
  }
}
