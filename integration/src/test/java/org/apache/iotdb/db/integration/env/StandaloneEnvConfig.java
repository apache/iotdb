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
package org.apache.iotdb.db.integration.env;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.itbase.env.BaseConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

/** This class is used by org.apache.iotdb.integration.env.ConfigFactory with using reflection. */
public class StandaloneEnvConfig implements BaseConfig {

  public BaseConfig setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    return this;
  }

  public BaseConfig setPageSizeInByte(int pageSizeInByte) {
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(pageSizeInByte);
    return this;
  }

  public BaseConfig setGroupSizeInByte(int groupSizeInByte) {
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(groupSizeInByte);
    return this;
  }

  public BaseConfig setMemtableSizeThreshold(long memtableSizeThreshold) {
    CommonDescriptor.getInstance().getConf().setMemtableSizeThreshold(memtableSizeThreshold);
    return this;
  }

  public BaseConfig setPartitionInterval(long partitionInterval) {
    IoTDBDescriptor.getInstance().getConf().setDnTimePartitionInterval(partitionInterval);
    return this;
  }

  public BaseConfig setCompressor(String compressor) {
    TSFileDescriptor.getInstance().getConfig().setCompressor(compressor);
    return this;
  }

  public BaseConfig setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    CommonDescriptor.getInstance()
        .getConf()
        .setMaxDeduplicatedPathNum(maxQueryDeduplicatedPathNum);
    return this;
  }

  public BaseConfig setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    IoTDBDescriptor.getInstance()
        .getConf()
        .setDnRpcThriftCompressionEnable(rpcThriftCompressionEnable);
    return this;
  }

  public BaseConfig setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    IoTDBDescriptor.getInstance()
        .getConf()
        .setDnRpcAdvancedCompressionEnable(rpcAdvancedCompressionEnable);
    return this;
  }

  public BaseConfig setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    CommonDescriptor.getInstance()
        .getConf()
        .setUdfCollectorMemoryBudgetInMB(udfCollectorMemoryBudgetInMB);
    return this;
  }

  public BaseConfig setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    CommonDescriptor.getInstance()
        .getConf()
        .setUdfTransformerMemoryBudgetInMB(udfTransformerMemoryBudgetInMB);
    return this;
  }

  public BaseConfig setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    CommonDescriptor.getInstance()
        .getConf()
        .setUdfReaderMemoryBudgetInMB(udfReaderMemoryBudgetInMB);
    return this;
  }

  public BaseConfig setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    IoTDBDescriptor.getInstance().getConf().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    return this;
  }

  public BaseConfig setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    IoTDBDescriptor.getInstance()
        .getConf()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    return this;
  }

  public BaseConfig setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    IoTDBDescriptor.getInstance()
        .getConf()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    return this;
  }

  public BaseConfig setEnableIDTable(boolean isEnableIDTable) {
    IoTDBDescriptor.getInstance().getConf().setEnableIDTable(isEnableIDTable);
    return this;
  }

  public BaseConfig setDeviceIDTransformationMethod(String deviceIDTransformationMethod) {
    IoTDBDescriptor.getInstance()
        .getConf()
        .setDeviceIDTransformationMethod(deviceIDTransformationMethod);
    return this;
  }

  public BaseConfig setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    CommonDescriptor.getInstance().getConf().setEnableAutoCreateSchema(enableAutoCreateSchema);
    return this;
  }
}
