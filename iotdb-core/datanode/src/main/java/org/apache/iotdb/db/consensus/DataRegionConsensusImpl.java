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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig.RPC;
import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.config.PipeConsensusConfig.ReplicateMode;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.config.RatisConfig.Snapshot;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.dataregion.DataRegionStateMachine;
import org.apache.iotdb.db.consensus.statemachine.dataregion.IoTConsensusDataRegionStateMachine;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.consensus.ConsensusPipeDataNodeDispatcher;
import org.apache.iotdb.db.pipe.consensus.ConsensusPipeDataNodeRuntimeAgentGuardian;
import org.apache.iotdb.db.pipe.consensus.ProgressIndexDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * We can use DataRegionConsensusImpl.getInstance() to obtain a consensus layer reference for
 * dataRegion's reading and writing
 */
public class DataRegionConsensusImpl {

  private DataRegionConsensusImpl() {
    // do nothing
  }

  public static IConsensus getInstance() {
    return DataRegionConsensusImplHolder.INSTANCE;
  }

  // TODO: This needs removal of statics ...
  public static void reinitializeStatics() {
    DataRegionConsensusImpl.DataRegionConsensusImplHolder.reinitializeStatics();
  }

  public static void reloadConsensusConfig() {
    getInstance().reloadConsensusConfig(DataRegionConsensusImplHolder.buildConsensusConfig());
  }

  private static class DataRegionConsensusImplHolder {

    private static final IoTDBConfig CONF = IoTDBDescriptor.getInstance().getConfig();

    private static IConsensus INSTANCE;

    // Make sure both statics are initialized.
    static {
      reinitializeStatics();
      PipeDataNodeAgent.receiver().pipeConsensus().initConsensusInRuntime();
      DeletionResourceManager.build();
    }

    private static void reinitializeStatics() {
      INSTANCE =
          ConsensusFactory.getConsensusImpl(
                  CONF.getDataRegionConsensusProtocolClass(),
                  buildConsensusConfig(),
                  DataRegionConsensusImplHolder::createDataRegionStateMachine)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                              CONF.getDataRegionConsensusProtocolClass())));
    }

    private static DataRegionStateMachine createDataRegionStateMachine(ConsensusGroupId gid) {
      DataRegion dataRegion = StorageEngine.getInstance().getDataRegion((DataRegionId) gid);
      if (ConsensusFactory.IOT_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
        return new IoTConsensusDataRegionStateMachine(dataRegion);
      } else {
        return new DataRegionStateMachine(dataRegion);
      }
    }

    private static ConsensusConfig buildConsensusConfig() {
      return ConsensusConfig.newBuilder()
          .setThisNodeId(CONF.getDataNodeId())
          .setThisNode(new TEndPoint(CONF.getInternalAddress(), CONF.getDataRegionConsensusPort()))
          .setStorageDir(CONF.getDataRegionConsensusDir())
          .setConsensusGroupType(TConsensusGroupType.DataRegion)
          .setIoTConsensusConfig(
              IoTConsensusConfig.newBuilder()
                  .setRpc(
                      RPC.newBuilder()
                          .setConnectionTimeoutInMs(CONF.getConnectionTimeoutInMS())
                          .setRpcSelectorThreadNum(CONF.getRpcSelectorThreadCount())
                          .setRpcMinConcurrentClientNum(CONF.getRpcMinConcurrentClientNum())
                          .setRpcMaxConcurrentClientNum(CONF.getRpcMaxConcurrentClientNum())
                          .setRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnable())
                          .setSelectorNumOfClientManager(CONF.getSelectorNumOfClientManager())
                          .setThriftServerAwaitTimeForStopService(
                              CONF.getThriftServerAwaitTimeForStopService())
                          .setThriftMaxFrameSize(CONF.getThriftMaxFrameSize())
                          .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                          .build())
                  .setReplication(
                      IoTConsensusConfig.Replication.newBuilder()
                          .setWalThrottleThreshold(CONF.getThrottleThreshold())
                          .setAllocateMemoryForConsensus(CONF.getAllocateMemoryForConsensus())
                          .setMaxLogEntriesNumPerBatch(CONF.getMaxLogEntriesNumPerBatch())
                          .setMaxSizePerBatch(CONF.getMaxSizePerBatch())
                          .setMaxPendingBatchesNum(CONF.getMaxPendingBatchesNum())
                          .setMaxMemoryRatioForQueue(CONF.getMaxMemoryRatioForQueue())
                          .setRegionMigrationSpeedLimitBytesPerSecond(
                              CONF.getRegionMigrationSpeedLimitBytesPerSecond())
                          .build())
                  .build())
          .setPipeConsensusConfig(
              PipeConsensusConfig.newBuilder()
                  .setRPC(
                      PipeConsensusConfig.RPC
                          .newBuilder()
                          .setConnectionTimeoutInMs(CONF.getConnectionTimeoutInMS())
                          .setRpcSelectorThreadNum(CONF.getRpcSelectorThreadCount())
                          .setRpcMinConcurrentClientNum(CONF.getRpcMinConcurrentClientNum())
                          .setRpcMaxConcurrentClientNum(CONF.getRpcMaxConcurrentClientNum())
                          .setIsRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnable())
                          .setThriftServerAwaitTimeForStopService(
                              CONF.getThriftServerAwaitTimeForStopService())
                          .setThriftMaxFrameSize(CONF.getThriftMaxFrameSize())
                          .build())
                  .setPipe(
                      PipeConsensusConfig.Pipe.newBuilder()
                          .setExtractorPluginName(
                              BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
                          .setProcessorPluginName(
                              BuiltinPipePlugin.PIPE_CONSENSUS_PROCESSOR.getPipePluginName())
                          .setConnectorPluginName(
                              BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName())
                          // name
                          .setConsensusPipeDispatcher(new ConsensusPipeDataNodeDispatcher())
                          .setConsensusPipeGuardian(new ConsensusPipeDataNodeRuntimeAgentGuardian())
                          .setConsensusPipeSelector(
                              () -> PipeDataNodeAgent.task().getAllConsensusPipe())
                          .setConsensusPipeReceiver(PipeDataNodeAgent.receiver().pipeConsensus())
                          .setProgressIndexManager(new ProgressIndexDataNodeManager())
                          .setConsensusPipeGuardJobIntervalInSeconds(300)
                          .build())
                  .setReplicateMode(ReplicateMode.fromValue(CONF.getIotConsensusV2Mode()))
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
                              CONF.getDataRatisConsensusSnapshotTriggerThreshold())
                          .build())
                  .setLog(
                      RatisConfig.Log.newBuilder()
                          .setUnsafeFlushEnabled(CONF.isDataRatisConsensusLogUnsafeFlushEnable())
                          .setForceSyncNum(CONF.getDataRatisConsensusLogForceSyncNum())
                          .setSegmentSizeMax(
                              SizeInBytes.valueOf(CONF.getDataRatisConsensusLogSegmentSizeMax()))
                          .setPreserveNumsWhenPurge(CONF.getDataRatisConsensusPreserveWhenPurge())
                          .build())
                  .setGrpc(
                      RatisConfig.Grpc.newBuilder()
                          .setFlowControlWindow(
                              SizeInBytes.valueOf(
                                  CONF.getDataRatisConsensusGrpcFlowControlWindow()))
                          .setLeaderOutstandingAppendsMax(
                              CONF.getDataRatisConsensusGrpcLeaderOutstandingAppendsMax())
                          .build())
                  .setRpc(
                      RatisConfig.Rpc.newBuilder()
                          .setTimeoutMin(
                              TimeDuration.valueOf(
                                  CONF.getDataRatisConsensusLeaderElectionTimeoutMinMs(),
                                  TimeUnit.MILLISECONDS))
                          .setTimeoutMax(
                              TimeDuration.valueOf(
                                  CONF.getDataRatisConsensusLeaderElectionTimeoutMaxMs(),
                                  TimeUnit.MILLISECONDS))
                          .setRequestTimeout(
                              TimeDuration.valueOf(
                                  CONF.getDataRatisConsensusRequestTimeoutMs(),
                                  TimeUnit.MILLISECONDS))
                          .setSlownessTimeout(
                              TimeDuration.valueOf(
                                  CONF.getDataRatisConsensusRequestTimeoutMs() * 6,
                                  TimeUnit.MILLISECONDS))
                          .setFirstElectionTimeoutMin(
                              TimeDuration.valueOf(
                                  CONF.getRatisFirstElectionTimeoutMinMs(), TimeUnit.MILLISECONDS))
                          .setFirstElectionTimeoutMax(
                              TimeDuration.valueOf(
                                  CONF.getRatisFirstElectionTimeoutMaxMs(), TimeUnit.MILLISECONDS))
                          .build())
                  .setClient(
                      RatisConfig.Client.newBuilder()
                          .setClientRequestTimeoutMillis(
                              CONF.getDataRatisConsensusRequestTimeoutMs())
                          .setClientMaxRetryAttempt(CONF.getDataRatisConsensusMaxRetryAttempts())
                          .setClientRetryInitialSleepTimeMs(
                              CONF.getDataRatisConsensusInitialSleepTimeMs())
                          .setClientRetryMaxSleepTimeMs(CONF.getDataRatisConsensusMaxSleepTimeMs())
                          .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                          .build())
                  .setImpl(
                      RatisConfig.Impl.newBuilder()
                          .setRaftLogSizeMaxThreshold(CONF.getDataRatisLogMax())
                          .setForceSnapshotInterval(CONF.getDataRatisPeriodicSnapshotInterval())
                          .setRetryTimesMax(10)
                          .setRetryWaitMillis(CONF.getConnectionTimeoutInMS() / 10)
                          .build())
                  .setLeaderLogAppender(
                      RatisConfig.LeaderLogAppender.newBuilder()
                          .setBufferByteLimit(CONF.getDataRatisConsensusLogAppenderBufferSizeMax())
                          .build())
                  .setRead(
                      RatisConfig.Read.newBuilder()
                          // use thrift connection timeout to unify read timeout
                          .setReadTimeout(
                              TimeDuration.valueOf(
                                  CONF.getConnectionTimeoutInMS(), TimeUnit.MILLISECONDS))
                          .build())
                  .build())
          .build();
    }
  }
}
