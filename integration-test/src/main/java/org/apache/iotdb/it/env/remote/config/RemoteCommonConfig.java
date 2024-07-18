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

package org.apache.iotdb.it.env.remote.config;

import org.apache.iotdb.itbase.env.CommonConfig;

import java.util.concurrent.TimeUnit;

public class RemoteCommonConfig implements CommonConfig {
  @Override
  public CommonConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    return this;
  }

  @Override
  public CommonConfig setPageSizeInByte(int pageSizeInByte) {
    return this;
  }

  @Override
  public CommonConfig setGroupSizeInByte(int groupSizeInByte) {
    return this;
  }

  @Override
  public CommonConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    return this;
  }

  @Override
  public CommonConfig setPartitionInterval(long partitionInterval) {
    return this;
  }

  @Override
  public CommonConfig setCompressor(String compressor) {
    return this;
  }

  @Override
  public CommonConfig setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(int maxMs) {
    return this;
  }

  @Override
  public CommonConfig setUdfMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    return this;
  }

  @Override
  public CommonConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    return this;
  }

  @Override
  public CommonConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    return this;
  }

  @Override
  public CommonConfig setEnableMemControl(boolean enableMemControl) {
    return this;
  }

  @Override
  public CommonConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    return this;
  }

  @Override
  public CommonConfig setMaxInnerCompactionCandidateFileNum(
      int maxInnerCompactionCandidateFileNum) {
    return this;
  }

  @Override
  public CommonConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    return this;
  }

  @Override
  public CommonConfig setEnableLastCache(boolean lastCacheEnable) {
    return this;
  }

  @Override
  public CommonConfig setPrimitiveArraySize(int primitiveArraySize) {
    return this;
  }

  @Override
  public CommonConfig setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    return this;
  }

  @Override
  public CommonConfig setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    return this;
  }

  @Override
  public CommonConfig setConfigNodeConsensusProtocolClass(String configNodeConsensusProtocolClass) {
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionConsensusProtocolClass(
      String schemaRegionConsensusProtocolClass) {
    return this;
  }

  @Override
  public CommonConfig setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionGroupExtensionPolicy(String schemaRegionGroupExtensionPolicy) {
    return this;
  }

  @Override
  public CommonConfig setDefaultSchemaRegionGroupNumPerDatabase(int schemaRegionGroupPerDatabase) {
    return this;
  }

  @Override
  public CommonConfig setDataRegionGroupExtensionPolicy(String dataRegionGroupExtensionPolicy) {
    return this;
  }

  @Override
  public CommonConfig setDefaultDataRegionGroupNumPerDatabase(int dataRegionGroupPerDatabase) {
    return this;
  }

  @Override
  public CommonConfig setSchemaReplicationFactor(int schemaReplicationFactor) {
    return this;
  }

  @Override
  public CommonConfig setDataReplicationFactor(int dataReplicationFactor) {
    return this;
  }

  @Override
  public CommonConfig setTimePartitionInterval(long timePartitionInterval) {
    return this;
  }

  @Override
  public CommonConfig setTimePartitionOrigin(long timePartitionOrigin) {
    return this;
  }

  @Override
  public CommonConfig setTimestampPrecision(String timestampPrecision) {
    return this;
  }

  @Override
  public TimeUnit getTimestampPrecision() {
    return TimeUnit.MILLISECONDS;
  }

  @Override
  public CommonConfig setTimestampPrecisionCheckEnabled(boolean timestampPrecisionCheckEnabled) {
    return this;
  }

  @Override
  public CommonConfig setConfigNodeRatisSnapshotTriggerThreshold(
      int ratisSnapshotTriggerThreshold) {
    return this;
  }

  @Override
  public CommonConfig setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode) {
    return this;
  }

  @Override
  public CommonConfig setEnableMQTTService(boolean enableMQTTService) {
    return this;
  }

  @Override
  public CommonConfig setSchemaEngineMode(String schemaEngineMode) {
    return this;
  }

  @Override
  public CommonConfig setSelectIntoInsertTabletPlanRowLimit(
      int selectIntoInsertTabletPlanRowLimit) {
    return this;
  }

  @Override
  public CommonConfig setEnableAutoLeaderBalanceForRatisConsensus(
      boolean enableAutoLeaderBalanceForRatisConsensus) {
    return this;
  }

  @Override
  public CommonConfig setEnableAutoLeaderBalanceForIoTConsensus(
      boolean enableAutoLeaderBalanceForIoTConsensus) {
    return this;
  }

  @Override
  public CommonConfig setQueryThreadCount(int queryThreadCount) {
    return this;
  }

  @Override
  public CommonConfig setWalBufferSize(int walBufferSize) {
    return this;
  }

  @Override
  public CommonConfig setDegreeOfParallelism(int degreeOfParallelism) {
    return this;
  }

  @Override
  public CommonConfig setDataRatisTriggerSnapshotThreshold(long threshold) {
    return this;
  }

  @Override
  public CommonConfig setSeriesSlotNum(int seriesSlotNum) {
    return this;
  }

  @Override
  public CommonConfig setSchemaMemoryAllocate(String schemaMemoryAllocate) {
    return this;
  }

  @Override
  public CommonConfig setWriteMemoryProportion(String writeMemoryProportion) {
    return this;
  }

  @Override
  public CommonConfig setQuotaEnable(boolean quotaEnable) {
    return this;
  }

  @Override
  public CommonConfig setSortBufferSize(long sortBufferSize) {
    return this;
  }

  @Override
  public CommonConfig setMaxTsBlockSizeInByte(long maxTsBlockSizeInByte) {
    return this;
  }

  @Override
  public CommonConfig setClusterTimeseriesLimitThreshold(long clusterSchemaLimitThreshold) {
    return this;
  }

  @Override
  public CommonConfig setClusterDeviceLimitThreshold(long clusterDeviceLimitThreshold) {
    return null;
  }

  @Override
  public CommonConfig setDatabaseLimitThreshold(long databaseLimitThreshold) {
    return this;
  }

  @Override
  public CommonConfig setDataRegionPerDataNode(double dataRegionPerDataNode) {
    return this;
  }

  public CommonConfig setSchemaRegionPerDataNode(double schemaRegionPerDataNode) {
    return this;
  }

  @Override
  public CommonConfig setPipeAirGapReceiverEnabled(boolean isPipeAirGapReceiverEnabled) {
    return this;
  }

  @Override
  public CommonConfig setDriverTaskExecutionTimeSliceInMs(long driverTaskExecutionTimeSliceInMs) {
    return this;
  }

  @Override
  public CommonConfig setWalMode(String walMode) {
    return this;
  }

  @Override
  public CommonConfig setTagAttributeTotalSize(int tagAttributeTotalSize) {
    return this;
  }

  @Override
  public CommonConfig setCnConnectionTimeoutMs(int connectionTimeoutMs) {
    return this;
  }

  @Override
  public CommonConfig setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
      int pipeHeartbeatIntervalSecondsForCollectingPipeMeta) {
    return this;
  }

  @Override
  public CommonConfig setPipeMetaSyncerInitialSyncDelayMinutes(
      long pipeMetaSyncerInitialSyncDelayMinutes) {
    return this;
  }

  @Override
  public CommonConfig setPipeMetaSyncerSyncIntervalMinutes(long pipeMetaSyncerSyncIntervalMinutes) {
    return this;
  }
}
