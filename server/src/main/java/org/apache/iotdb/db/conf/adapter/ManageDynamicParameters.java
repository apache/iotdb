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
package org.apache.iotdb.db.conf.adapter;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to get and set dynamic parameters through JMX.
 */
public class ManageDynamicParameters implements ManageDynamicParametersMBean, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManageDynamicParameters.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());

  private ManageDynamicParameters() {

  }

  public static ManageDynamicParameters getInstance() {
    return ManageDynamicParametersHolder.INSTANCE;
  }

  @Override
  public void showDynamicParameters() {
    LOGGER.info(
        "Memtable size threshold: {}B, Memtable number: {}, Tsfile size threshold: {}B, Compression ratio: {}, "
            + "Storage group number: {}, Timeseries number: {}, Maximal timeseries number among storage groups: {}",
        CONFIG.getMemtableSizeThreshold(), CONFIG.getMaxMemtableNumber(),
        CONFIG.getTsFileSizeThreshold(), CompressionRatio.getInstance().getRatio(),
        IoTDBConfigDynamicAdapter.getInstance().getTotalStorageGroup(),
        IoTDBConfigDynamicAdapter.getInstance().getTotalTimeseries(),
        IoTDB.metaManager.getMaximalSeriesNumberAmongStorageGroups());
  }

  @Override
  public boolean isEnableDynamicAdapter() {
    return CONFIG.isEnableParameterAdapter();
  }

  @Override
  public void setEnableDynamicAdapter(boolean enableDynamicAdapter) {
    CONFIG.setEnableParameterAdapter(enableDynamicAdapter);
  }

  @Override
  public long getMemTableSizeThreshold() {
    return CONFIG.getMemtableSizeThreshold();
  }

  @Override
  public void setMemTableSizeThreshold(long memTableSizeThreshold) {
    CONFIG.setMemtableSizeThreshold(memTableSizeThreshold);
  }

  @Override
  public int getMemTableNumber() {
    return CONFIG.getMaxMemtableNumber();
  }

  @Override
  public void setMemTableNumber(int memTableNumber) {
    CONFIG.setMaxMemtableNumber(memTableNumber);
  }

  @Override
  public long getTsfileSizeThreshold() {
    return CONFIG.getTsFileSizeThreshold();
  }

  @Override
  public void setTsfileSizeThreshold(long tsfileSizeThreshold) {
    CONFIG.setTsFileSizeThreshold(tsfileSizeThreshold);
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(getInstance(), mbeanName);
      LOGGER.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    } catch (Exception e) {
      LOGGER.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("{}: stop {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MANAGE_DYNAMIC_PARAMETERS_SERVICE;
  }

  private static class ManageDynamicParametersHolder {

    private static final ManageDynamicParameters INSTANCE = new ManageDynamicParameters();

    private ManageDynamicParametersHolder() {

    }
  }
}
