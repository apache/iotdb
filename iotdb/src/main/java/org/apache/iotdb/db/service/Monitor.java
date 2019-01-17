/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.service;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.OpenFileNumUtil;

public class Monitor implements MonitorMBean, IService {

  public static final Monitor INSTANCE = new Monitor();
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public long getDataSizeInByte() {
    try {
      return FileUtils.sizeOfDirectory(new File(config.dataDir));
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public int getFileNodeNum() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getOverflowCacheSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getBufferWriteCacheSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getBaseDirectory() {
    try {
      File file = new File(config.dataDir);
      return file.getAbsolutePath();
    } catch (Exception e) {
      return "Unavailable";
    }
  }

  @Override
  public boolean getWriteAheadLogStatus() {
    return config.enableWal;
  }

  @Override
  public int getTotalOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
  }

  @Override
  public int getDataOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
  }

  @Override
  public int getDeltaOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.DELTA_OPEN_FILE_NUM);
  }

  @Override
  public int getOverflowOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.OVERFLOW_OPEN_FILE_NUM);
  }

  @Override
  public int getWalOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.WAL_OPEN_FILE_NUM);
  }

  @Override
  public int getMetadataOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.METADATA_OPEN_FILE_NUM);
  }

  @Override
  public int getDigestOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
  }

  @Override
  public int getSocketOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM);
  }

  @Override
  public long getMergePeriodInSecond() {
    return config.periodTimeForMerge;
  }

  @Override
  public long getClosePeriodInSecond() {
    return config.periodTimeForFlush;
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MONITOR_SERVICE;
  }

}
