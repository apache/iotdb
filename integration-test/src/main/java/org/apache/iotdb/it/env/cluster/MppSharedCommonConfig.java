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
package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.itbase.env.CommonConfig;

public class MppSharedCommonConfig implements CommonConfig {

  private final MppCommonConfig cnConfig;
  private final MppCommonConfig dnConfig;

  public MppSharedCommonConfig(MppCommonConfig cnConfig, MppCommonConfig dnConfig) {
    this.cnConfig = cnConfig;
    this.dnConfig = dnConfig;
  }

  @Override
  public CommonConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    cnConfig.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    dnConfig.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    return this;
  }

  @Override
  public CommonConfig setPageSizeInByte(int pageSizeInByte) {
    cnConfig.setPageSizeInByte(pageSizeInByte);
    dnConfig.setPageSizeInByte(pageSizeInByte);
    return this;
  }

  @Override
  public CommonConfig setGroupSizeInByte(int groupSizeInByte) {
    cnConfig.setGroupSizeInByte(groupSizeInByte);
    dnConfig.setGroupSizeInByte(groupSizeInByte);
    return this;
  }

  @Override
  public CommonConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    cnConfig.setMemtableSizeThreshold(memtableSizeThreshold);
    dnConfig.setMemtableSizeThreshold(memtableSizeThreshold);
    return this;
  }

  @Override
  public CommonConfig setPartitionInterval(long partitionInterval) {
    cnConfig.setPartitionInterval(partitionInterval);
    dnConfig.setPartitionInterval(partitionInterval);
    return this;
  }

  @Override
  public CommonConfig setCompressor(String compressor) {
    cnConfig.setCompressor(compressor);
    dnConfig.setCompressor(compressor);
    return this;
  }

  @Override
  public CommonConfig setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(int maxMs) {
    cnConfig.setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(maxMs);
    dnConfig.setConfigRegionRatisRPCLeaderElectionTimeoutMaxMs(maxMs);
    return this;
  }

  @Override
  public CommonConfig setUdfMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    cnConfig.setUdfMemoryBudgetInMB(udfCollectorMemoryBudgetInMB);
    dnConfig.setUdfMemoryBudgetInMB(udfCollectorMemoryBudgetInMB);
    return this;
  }

  @Override
  public CommonConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    cnConfig.setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    dnConfig.setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    return this;
  }

  @Override
  public CommonConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    cnConfig.setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    dnConfig.setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    return this;
  }

  @Override
  public CommonConfig setEnableMemControl(boolean enableMemControl) {
    cnConfig.setEnableMemControl(enableMemControl);
    dnConfig.setEnableMemControl(enableMemControl);
    return this;
  }

  @Override
  public CommonConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    cnConfig.setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    dnConfig.setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    return this;
  }

  @Override
  public CommonConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    cnConfig.setAutoCreateSchemaEnabled(enableAutoCreateSchema);
    dnConfig.setAutoCreateSchemaEnabled(enableAutoCreateSchema);
    return this;
  }

  @Override
  public CommonConfig setEnableLastCache(boolean lastCacheEnable) {
    cnConfig.setEnableLastCache(lastCacheEnable);
    dnConfig.setEnableLastCache(lastCacheEnable);
    return this;
  }

  @Override
  public CommonConfig setPrimitiveArraySize(int primitiveArraySize) {
    cnConfig.setPrimitiveArraySize(primitiveArraySize);
    dnConfig.setPrimitiveArraySize(primitiveArraySize);
    return this;
  }

  @Override
  public CommonConfig setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    cnConfig.setAvgSeriesPointNumberThreshold(avgSeriesPointNumberThreshold);
    dnConfig.setAvgSeriesPointNumberThreshold(avgSeriesPointNumberThreshold);
    return this;
  }

  @Override
  public CommonConfig setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    cnConfig.setMaxTsBlockLineNumber(maxTsBlockLineNumber);
    dnConfig.setMaxTsBlockLineNumber(maxTsBlockLineNumber);
    return this;
  }

  @Override
  public CommonConfig setConfigNodeConsensusProtocolClass(String configNodeConsensusProtocolClass) {
    cnConfig.setConfigNodeConsensusProtocolClass(configNodeConsensusProtocolClass);
    dnConfig.setConfigNodeConsensusProtocolClass(configNodeConsensusProtocolClass);
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionConsensusProtocolClass(
      String schemaRegionConsensusProtocolClass) {
    cnConfig.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    dnConfig.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    return this;
  }

  @Override
  public CommonConfig setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    cnConfig.setDataRegionConsensusProtocolClass(dataRegionConsensusProtocolClass);
    dnConfig.setDataRegionConsensusProtocolClass(dataRegionConsensusProtocolClass);
    return this;
  }

  @Override
  public CommonConfig setEnableDataPartitionInheritPolicy(
      boolean enableDataPartitionInheritPolicy) {
    cnConfig.setEnableDataPartitionInheritPolicy(enableDataPartitionInheritPolicy);
    dnConfig.setEnableDataPartitionInheritPolicy(enableDataPartitionInheritPolicy);
    return this;
  }

  @Override
  public CommonConfig setSchemaRegionGroupExtensionPolicy(String schemaRegionGroupExtensionPolicy) {
    cnConfig.setSchemaRegionGroupExtensionPolicy(schemaRegionGroupExtensionPolicy);
    dnConfig.setSchemaRegionGroupExtensionPolicy(schemaRegionGroupExtensionPolicy);
    return this;
  }

  @Override
  public CommonConfig setDefaultSchemaRegionGroupNumPerDatabase(int schemaRegionGroupPerDatabase) {
    cnConfig.setDefaultSchemaRegionGroupNumPerDatabase(schemaRegionGroupPerDatabase);
    dnConfig.setDefaultSchemaRegionGroupNumPerDatabase(schemaRegionGroupPerDatabase);
    return this;
  }

  @Override
  public CommonConfig setDataRegionGroupExtensionPolicy(String dataRegionGroupExtensionPolicy) {
    cnConfig.setDataRegionGroupExtensionPolicy(dataRegionGroupExtensionPolicy);
    dnConfig.setDataRegionGroupExtensionPolicy(dataRegionGroupExtensionPolicy);
    return this;
  }

  @Override
  public CommonConfig setDefaultDataRegionGroupNumPerDatabase(int dataRegionGroupPerDatabase) {
    cnConfig.setDefaultDataRegionGroupNumPerDatabase(dataRegionGroupPerDatabase);
    dnConfig.setDefaultDataRegionGroupNumPerDatabase(dataRegionGroupPerDatabase);
    return this;
  }

  @Override
  public CommonConfig setSchemaReplicationFactor(int schemaReplicationFactor) {
    cnConfig.setSchemaReplicationFactor(schemaReplicationFactor);
    dnConfig.setSchemaReplicationFactor(schemaReplicationFactor);
    return this;
  }

  @Override
  public CommonConfig setDataReplicationFactor(int dataReplicationFactor) {
    cnConfig.setDataReplicationFactor(dataReplicationFactor);
    dnConfig.setDataReplicationFactor(dataReplicationFactor);
    return this;
  }

  @Override
  public CommonConfig setTimePartitionInterval(long timePartitionInterval) {
    cnConfig.setTimePartitionInterval(timePartitionInterval);
    dnConfig.setTimePartitionInterval(timePartitionInterval);
    return this;
  }

  @Override
  public CommonConfig setConfigNodeRatisSnapshotTriggerThreshold(
      int ratisSnapshotTriggerThreshold) {
    cnConfig.setConfigNodeRatisSnapshotTriggerThreshold(ratisSnapshotTriggerThreshold);
    dnConfig.setConfigNodeRatisSnapshotTriggerThreshold(ratisSnapshotTriggerThreshold);
    return this;
  }

  @Override
  public CommonConfig setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode) {
    cnConfig.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
    dnConfig.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
    return this;
  }

  @Override
  public CommonConfig setEnableWatermark(boolean enableWatermark) {
    cnConfig.setEnableWatermark(enableWatermark);
    dnConfig.setEnableWatermark(enableWatermark);
    return this;
  }

  @Override
  public CommonConfig setWatermarkSecretKey(String watermarkSecretKey) {
    cnConfig.setWatermarkSecretKey(watermarkSecretKey);
    dnConfig.setWatermarkSecretKey(watermarkSecretKey);
    return this;
  }

  @Override
  public CommonConfig setWatermarkBitString(String watermarkBitString) {
    cnConfig.setWatermarkBitString(watermarkBitString);
    dnConfig.setWatermarkBitString(watermarkBitString);
    return this;
  }

  @Override
  public CommonConfig setWatermarkMethod(String watermarkMethod) {
    cnConfig.setWatermarkMethod(watermarkMethod);
    dnConfig.setWatermarkMethod(watermarkMethod);
    return this;
  }

  @Override
  public CommonConfig setEnableMQTTService(boolean enableMQTTService) {
    cnConfig.setEnableMQTTService(enableMQTTService);
    dnConfig.setEnableMQTTService(enableMQTTService);
    return this;
  }

  @Override
  public CommonConfig setSchemaEngineMode(String schemaEngineMode) {
    cnConfig.setSchemaEngineMode(schemaEngineMode);
    dnConfig.setSchemaEngineMode(schemaEngineMode);
    return this;
  }

  @Override
  public CommonConfig setSelectIntoInsertTabletPlanRowLimit(
      int selectIntoInsertTabletPlanRowLimit) {
    cnConfig.setSelectIntoInsertTabletPlanRowLimit(selectIntoInsertTabletPlanRowLimit);
    dnConfig.setSelectIntoInsertTabletPlanRowLimit(selectIntoInsertTabletPlanRowLimit);
    return this;
  }

  @Override
  public CommonConfig setEnableAutoLeaderBalanceForRatisConsensus(
      boolean enableAutoLeaderBalanceForRatisConsensus) {
    cnConfig.setEnableAutoLeaderBalanceForRatisConsensus(enableAutoLeaderBalanceForRatisConsensus);
    dnConfig.setEnableAutoLeaderBalanceForRatisConsensus(enableAutoLeaderBalanceForRatisConsensus);
    return this;
  }

  @Override
  public CommonConfig setEnableAutoLeaderBalanceForIoTConsensus(
      boolean enableAutoLeaderBalanceForIoTConsensus) {
    cnConfig.setEnableAutoLeaderBalanceForIoTConsensus(enableAutoLeaderBalanceForIoTConsensus);
    dnConfig.setEnableAutoLeaderBalanceForIoTConsensus(enableAutoLeaderBalanceForIoTConsensus);
    return this;
  }

  @Override
  public CommonConfig setQueryThreadCount(int queryThreadCount) {
    cnConfig.setQueryThreadCount(queryThreadCount);
    dnConfig.setQueryThreadCount(queryThreadCount);
    return this;
  }

  @Override
  public CommonConfig setDegreeOfParallelism(int degreeOfParallelism) {
    cnConfig.setDegreeOfParallelism(degreeOfParallelism);
    dnConfig.setDegreeOfParallelism(degreeOfParallelism);
    return this;
  }

  @Override
  public CommonConfig setDataRatisTriggerSnapshotThreshold(long threshold) {
    cnConfig.setDataRatisTriggerSnapshotThreshold(threshold);
    dnConfig.setDataRatisTriggerSnapshotThreshold(threshold);
    return this;
  }

  @Override
  public CommonConfig setSeriesSlotNum(int seriesSlotNum) {
    cnConfig.setSeriesSlotNum(seriesSlotNum);
    dnConfig.setSeriesSlotNum(seriesSlotNum);
    return this;
  }

  @Override
  public CommonConfig setSchemaMemoryAllocate(String schemaMemoryAllocate) {
    dnConfig.setSchemaMemoryAllocate(schemaMemoryAllocate);
    cnConfig.setSchemaMemoryAllocate(schemaMemoryAllocate);
    return this;
  }

  @Override
  public CommonConfig setWriteMemoryProportion(String writeMemoryProportion) {
    dnConfig.setWriteMemoryProportion(writeMemoryProportion);
    cnConfig.setWriteMemoryProportion(writeMemoryProportion);
    return this;
  }
}
