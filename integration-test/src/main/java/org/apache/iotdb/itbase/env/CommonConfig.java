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

package org.apache.iotdb.itbase.env;

import java.util.concurrent.TimeUnit;

/** This interface is used to handle properties in iotdb-common.properties. */
public interface CommonConfig {

  CommonConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage);

  CommonConfig setPageSizeInByte(int pageSizeInByte);

  CommonConfig setGroupSizeInByte(int groupSizeInByte);

  CommonConfig setMemtableSizeThreshold(long memtableSizeThreshold);

  CommonConfig setPartitionInterval(long partitionInterval);

  CommonConfig setCompressor(String compressor);

  CommonConfig setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(int maxMs);

  CommonConfig setUdfMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB);

  CommonConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction);

  CommonConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction);

  CommonConfig setEnableMemControl(boolean enableMemControl);

  CommonConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction);

  CommonConfig setMaxInnerCompactionCandidateFileNum(int maxInnerCompactionCandidateFileNum);

  CommonConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema);

  CommonConfig setEnableLastCache(boolean lastCacheEnable);

  CommonConfig setPrimitiveArraySize(int primitiveArraySize);

  CommonConfig setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold);

  CommonConfig setMaxTsBlockLineNumber(int maxTsBlockLineNumber);

  CommonConfig setConfigNodeConsensusProtocolClass(String configNodeConsensusProtocolClass);

  CommonConfig setSchemaRegionConsensusProtocolClass(String schemaRegionConsensusProtocolClass);

  CommonConfig setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass);

  CommonConfig setSchemaRegionGroupExtensionPolicy(String schemaRegionGroupExtensionPolicy);

  CommonConfig setDefaultSchemaRegionGroupNumPerDatabase(int schemaRegionGroupPerDatabase);

  CommonConfig setDataRegionGroupExtensionPolicy(String dataRegionGroupExtensionPolicy);

  CommonConfig setDefaultDataRegionGroupNumPerDatabase(int dataRegionGroupPerDatabase);

  CommonConfig setSchemaReplicationFactor(int schemaReplicationFactor);

  CommonConfig setDataReplicationFactor(int dataReplicationFactor);

  CommonConfig setTimePartitionInterval(long timePartitionInterval);

  CommonConfig setTimePartitionOrigin(long timePartitionOrigin);

  CommonConfig setTimestampPrecision(String timestampPrecision);

  TimeUnit getTimestampPrecision();

  CommonConfig setTimestampPrecisionCheckEnabled(boolean timestampPrecisionCheckEnabled);

  CommonConfig setConfigNodeRatisSnapshotTriggerThreshold(int ratisSnapshotTriggerThreshold);

  CommonConfig setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode);

  CommonConfig setEnableMQTTService(boolean enableMQTTService);

  CommonConfig setSchemaEngineMode(String schemaEngineMode);

  CommonConfig setSelectIntoInsertTabletPlanRowLimit(int selectIntoInsertTabletPlanRowLimit);

  CommonConfig setEnableAutoLeaderBalanceForRatisConsensus(
      boolean enableAutoLeaderBalanceForRatisConsensus);

  CommonConfig setEnableAutoLeaderBalanceForIoTConsensus(
      boolean enableAutoLeaderBalanceForIoTConsensus);

  CommonConfig setQueryThreadCount(int queryThreadCount);

  CommonConfig setWalBufferSize(int walBufferSize);

  CommonConfig setDegreeOfParallelism(int degreeOfParallelism);

  CommonConfig setDataRatisTriggerSnapshotThreshold(long threshold);

  CommonConfig setSeriesSlotNum(int seriesSlotNum);

  CommonConfig setSchemaMemoryAllocate(String schemaMemoryAllocate);

  CommonConfig setWriteMemoryProportion(String writeMemoryProportion);

  CommonConfig setClusterTimeseriesLimitThreshold(long clusterTimeseriesLimitThreshold);

  CommonConfig setClusterDeviceLimitThreshold(long clusterDeviceLimitThreshold);

  CommonConfig setDatabaseLimitThreshold(long databaseLimitThreshold);

  CommonConfig setQuotaEnable(boolean quotaEnable);

  CommonConfig setSortBufferSize(long sortBufferSize);

  CommonConfig setMaxTsBlockSizeInByte(long maxTsBlockSizeInByte);

  CommonConfig setDataRegionPerDataNode(double dataRegionPerDataNode);

  CommonConfig setSchemaRegionPerDataNode(double schemaRegionPerDataNode);

  CommonConfig setPipeAirGapReceiverEnabled(boolean isPipeAirGapReceiverEnabled);

  CommonConfig setDriverTaskExecutionTimeSliceInMs(long driverTaskExecutionTimeSliceInMs);

  CommonConfig setWalMode(String walMode);

  CommonConfig setTagAttributeTotalSize(int tagAttributeTotalSize);

  CommonConfig setCnConnectionTimeoutMs(int connectionTimeoutMs);

  CommonConfig setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
      int pipeHeartbeatIntervalSecondsForCollectingPipeMeta);

  CommonConfig setPipeMetaSyncerInitialSyncDelayMinutes(long pipeMetaSyncerInitialSyncDelayMinutes);

  CommonConfig setPipeMetaSyncerSyncIntervalMinutes(long pipeMetaSyncerSyncIntervalMinutes);
}
