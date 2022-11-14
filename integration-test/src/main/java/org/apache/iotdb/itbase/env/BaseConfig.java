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

import java.util.Properties;

public interface BaseConfig {

  default void clearAllProperties() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Method clearAllProperties not implement");
  }

  default Properties getEngineProperties() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Method getEngineProperties not implement");
  }

  default Properties getConfignodeProperties() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Method getConfignodeProperties not implement");
  }

  default BaseConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    return this;
  }

  default int getPageSizeInByte() {
    return 64 * 1024;
  }

  default BaseConfig setPageSizeInByte(int pageSizeInByte) {
    return this;
  }

  default int getGroupSizeInByte() {
    return 128 * 1024 * 1024;
  }

  default BaseConfig setGroupSizeInByte(int groupSizeInByte) {
    return this;
  }

  default long getMemtableSizeThreshold() {
    return 1024 * 1024 * 1024L;
  }

  default BaseConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    return this;
  }

  default int getDataRegionNum() {
    return 1;
  }

  default BaseConfig setDataRegionNum(int dataRegionNum) {
    return this;
  }

  default boolean isEnablePartition() {
    return false;
  }

  default BaseConfig setPartitionInterval(long partitionInterval) {
    return this;
  }

  default BaseConfig setTimePartitionIntervalForStorage(long partitionInterval) {
    return this;
  }

  default long getPartitionInterval() {
    return 604800;
  }

  default BaseConfig setCompressor(String compressor) {
    return this;
  }

  default BaseConfig setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    return this;
  }

  default BaseConfig setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    return this;
  }

  default BaseConfig setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    return this;
  }

  default BaseConfig setEnablePartition(boolean enablePartition) {
    return this;
  }

  default BaseConfig setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    return this;
  }

  default BaseConfig setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    return this;
  }

  default BaseConfig setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    return this;
  }

  default BaseConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    return this;
  }

  default boolean isEnableSeqSpaceCompaction() {
    return true;
  }

  default BaseConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    return this;
  }

  default boolean isEnableMemControl() {
    return true;
  }

  default BaseConfig setEnableMemControl(boolean enableMemControl) {
    return this;
  }

  default boolean isEnableUnseqSpaceCompaction() {
    return true;
  }

  default BaseConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    return this;
  }

  default boolean isEnableCrossSpaceCompaction() {
    return true;
  }

  default BaseConfig setEnableIDTable(boolean isEnableIDTable) {
    return this;
  }

  default BaseConfig setDeviceIDTransformationMethod(String deviceIDTransformationMethod) {
    return this;
  }

  default BaseConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    return this;
  }

  default BaseConfig setEnableLastCache(boolean lastCacheEnable) {
    return this;
  }

  default boolean isLastCacheEnabled() {
    return true;
  }

  default int getMaxNumberOfPointsInPage() {
    return 1024 * 1024;
  }

  default boolean isAutoCreateSchemaEnabled() {
    return true;
  }

  default BaseConfig setPrimitiveArraySize(int primitiveArraySize) {
    return this;
  }

  default int getPrimitiveArraySize() {
    return 32;
  }

  default String getFlushCommand() {
    return "flush";
  }

  default int getMaxQueryDeduplicatedPathNum() {
    return 1000;
  }

  default int getAvgSeriesPointNumberThreshold() {
    return 100000;
  }

  default BaseConfig setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    return this;
  }

  default int getMaxTsBlockLineNumber() {
    return 1000;
  }

  default BaseConfig setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    return this;
  }

  default BaseConfig setConfigNodeConsesusProtocolClass(String configNodeConsesusProtocolClass) {
    return this;
  }

  default String getConfigNodeConsesusProtocolClass() {
    return "org.apache.iotdb.consensus.simple.SimpleConsensus";
  }

  default BaseConfig setSchemaRegionConsensusProtocolClass(
      String schemaRegionConsensusProtocolClass) {
    return this;
  }

  default String getSchemaRegionConsensusProtocolClass() {
    return "org.apache.iotdb.consensus.simple.SimpleConsensus";
  }

  default BaseConfig setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    return this;
  }

  default String getDataRegionConsensusProtocolClass() {
    return "org.apache.iotdb.consensus.simple.SimpleConsensus";
  }

  default BaseConfig setSchemaReplicationFactor(int schemaReplicationFactor) {
    return this;
  }

  default int getSchemaReplicationFactor() {
    return 1;
  }

  default BaseConfig setDataReplicationFactor(int dataReplicationFactor) {
    return this;
  }

  default int getDataReplicationFactor() {
    return 1;
  }

  default BaseConfig setTimePartitionIntervalForRouting(long timePartitionInterval) {
    return this;
  }

  default long getTimePartitionInterval() {
    return 86400;
  }

  default BaseConfig setRatisSnapshotTriggerThreshold(int ratisSnapshotTriggerThreshold) {
    return this;
  }

  default int getRatisSnapshotTriggerThreshold() {
    return 400000;
  }

  default int getConfigNodeRegionRatisRPCLeaderElectionTimeoutMaxMs() {
    return 4000;
  }

  default BaseConfig setCompactionThreadCount(int compactionThreadCount) {
    return this;
  }

  default int getConcurrentCompactionThread() {
    return 10;
  }

  default BaseConfig setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode) {
    return this;
  }

  default int getMaxDegreeOfIndexNode() {
    return 256;
  }

  default BaseConfig setEnableWatermark(boolean enableWatermark) {
    return this;
  }

  default boolean isEnableWatermark() {
    return false;
  }

  default String getWatermarkSecretKey() {
    return "IoTDB*2019@Beijing";
  }

  default BaseConfig setWatermarkSecretKey(String watermarkSecretKey) {
    return this;
  }

  default String getWatermarkBitString() {
    return "100101110100";
  }

  default BaseConfig setWatermarkBitString(String watermarkBitString) {
    return this;
  }

  default String getWatermarkMethod() {
    return "GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5)";
  }

  default BaseConfig setWatermarkMethod(String watermarkMethod) {
    return this;
  }

  default boolean isEnableMQTTService() {
    return false;
  }

  default BaseConfig setEnableMQTTService(boolean enableMQTTService) {
    return this;
  }

  default BaseConfig setSchemaEngineMode(String schemaEngineMode) {
    return this;
  }

  default String getSchemaEngineMode() {
    return "Memory";
  }

  default BaseConfig setSelectIntoInsertTabletPlanRowLimit(int selectIntoInsertTabletPlanRowLimit) {
    return this;
  }

  default int getSelectIntoInsertTabletPlanRowLimit() {
    return 10000;
  }

  default BaseConfig setEnableLeaderBalancing(boolean enableLeaderBalancing) {
    return this;
  }

  default boolean isEnableLeaderBalancing() {
    return false;
  }
}
