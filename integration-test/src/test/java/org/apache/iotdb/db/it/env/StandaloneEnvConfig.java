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
package org.apache.iotdb.db.it.env;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.itbase.env.BaseConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

/** This class is used by ConfigFactory with using reflection. */
public class StandaloneEnvConfig implements BaseConfig {

  @Override
  public BaseConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    return this;
  }

  @Override
  public BaseConfig setPageSizeInByte(int pageSizeInByte) {
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(pageSizeInByte);
    return this;
  }

  @Override
  public BaseConfig setGroupSizeInByte(int groupSizeInByte) {
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(groupSizeInByte);
    return this;
  }

  @Override
  public BaseConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(memtableSizeThreshold);
    return this;
  }

  @Override
  public int getDataRegionNum() {
    return IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
  }

  @Override
  public BaseConfig setDataRegionNum(int dataRegionNum) {
    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(dataRegionNum);
    return this;
  }

  @Override
  public BaseConfig setPartitionInterval(long partitionInterval) {
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(partitionInterval);
    return this;
  }

  @Override
  public long getPartitionInterval() {
    return IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
  }

  @Override
  public BaseConfig setCompressor(String compressor) {
    TSFileDescriptor.getInstance().getConfig().setCompressor(compressor);
    return this;
  }

  @Override
  public BaseConfig setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxQueryDeduplicatedPathNum(maxQueryDeduplicatedPathNum);
    return this;
  }

  @Override
  public BaseConfig setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setRpcThriftCompressionEnable(rpcThriftCompressionEnable);
    return this;
  }

  @Override
  public BaseConfig setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setRpcAdvancedCompressionEnable(rpcAdvancedCompressionEnable);
    return this;
  }

  @Override
  public BaseConfig setEnablePartition(boolean enablePartition) {
    IoTDBDescriptor.getInstance().getConfig().setEnablePartition(enablePartition);
    return this;
  }

  @Override
  public BaseConfig setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setUdfCollectorMemoryBudgetInMB(udfCollectorMemoryBudgetInMB);
    return this;
  }

  @Override
  public BaseConfig setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setUdfTransformerMemoryBudgetInMB(udfTransformerMemoryBudgetInMB);
    return this;
  }

  @Override
  public BaseConfig setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setUdfReaderMemoryBudgetInMB(udfReaderMemoryBudgetInMB);
    return this;
  }

  @Override
  public BaseConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    return this;
  }

  @Override
  public BaseConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    return this;
  }

  @Override
  public BaseConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    return this;
  }

  @Override
  public BaseConfig setEnableIDTable(boolean isEnableIDTable) {
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    return this;
  }

  @Override
  public BaseConfig setDeviceIDTransformationMethod(String deviceIDTransformationMethod) {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(deviceIDTransformationMethod);
    return this;
  }

  @Override
  public BaseConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(enableAutoCreateSchema);
    return this;
  }

  @Override
  public BaseConfig setEnableLastCache(boolean lastCacheEnable) {
    IoTDBDescriptor.getInstance().getConfig().setEnableLastCache(lastCacheEnable);
    return this;
  }

  @Override
  public boolean isLastCacheEnabled() {
    return IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();
  }

  @Override
  public boolean isEnableSeqSpaceCompaction() {
    return IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
  }

  @Override
  public boolean isEnableUnseqSpaceCompaction() {
    return IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
  }

  @Override
  public boolean isEnableCrossSpaceCompaction() {
    return IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
  }

  @Override
  public boolean isAutoCreateSchemaEnabled() {
    return IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
  }

  @Override
  public int getMaxNumberOfPointsInPage() {
    return TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
  }

  @Override
  public BaseConfig setPrimitiveArraySize(int primitiveArraySize) {
    IoTDBDescriptor.getInstance().getConfig().setPrimitiveArraySize(primitiveArraySize);
    return this;
  }

  @Override
  public int getPrimitiveArraySize() {
    return IoTDBDescriptor.getInstance().getConfig().getPrimitiveArraySize();
  }
}
