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

package org.apache.iotdb.flink.tsfile.util;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

/** Utils of setting global TSFileConfig. */
public class TSFileConfigUtil {

  public static void setGlobalTSFileConfig(TSFileConfig config) {
    TSFileConfig globalConfig = TSFileDescriptor.getInstance().getConfig();

    globalConfig.setBatchSize(config.getBatchSize());
    globalConfig.setBloomFilterErrorRate(config.getBloomFilterErrorRate());
    globalConfig.setCompressor(config.getCompressor().toString());
    globalConfig.setCoreSitePath(config.getCoreSitePath());
    globalConfig.setDeltaBlockSize(config.getDeltaBlockSize());
    globalConfig.setDfsClientFailoverProxyProvider(config.getDfsClientFailoverProxyProvider());
    globalConfig.setDfsHaAutomaticFailoverEnabled(config.isDfsHaAutomaticFailoverEnabled());
    globalConfig.setDfsHaNamenodes(config.getDfsHaNamenodes());
    globalConfig.setDfsNameServices(config.getDfsNameServices());
    globalConfig.setDftSatisfyRate(config.getDftSatisfyRate());
    globalConfig.setEndian(config.getEndian());
    globalConfig.setFloatPrecision(config.getFloatPrecision());
    globalConfig.setFreqType(config.getFreqType());
    globalConfig.setGroupSizeInByte(config.getGroupSizeInByte());
    globalConfig.setHdfsIp(config.getHdfsIp());
    globalConfig.setHdfsPort(config.getHdfsPort());
    globalConfig.setHdfsSitePath(config.getHdfsSitePath());
    globalConfig.setKerberosKeytabFilePath(config.getKerberosKeytabFilePath());
    globalConfig.setKerberosPrincipal(config.getKerberosPrincipal());
    globalConfig.setMaxNumberOfPointsInPage(config.getMaxNumberOfPointsInPage());
    globalConfig.setMaxDegreeOfIndexNode(config.getMaxDegreeOfIndexNode());
    globalConfig.setMaxStringLength(config.getMaxStringLength());
    globalConfig.setPageCheckSizeThreshold(config.getPageCheckSizeThreshold());
    globalConfig.setPageSizeInByte(config.getPageSizeInByte());
    globalConfig.setPlaMaxError(config.getPlaMaxError());
    globalConfig.setRleBitWidth(config.getRleBitWidth());
    globalConfig.setSdtMaxError(config.getSdtMaxError());
    globalConfig.setTimeEncoder(config.getTimeEncoder());
    globalConfig.setTimeSeriesDataType(config.getTimeSeriesDataType());
    globalConfig.setTSFileStorageFs(config.getTSFileStorageFs());
    globalConfig.setUseKerberos(config.isUseKerberos());
    globalConfig.setValueEncoder(config.getValueEncoder());
    globalConfig.setCustomizedProperties(config.getCustomizedProperties());
  }
}
