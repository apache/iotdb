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
package org.apache.iotdb.db.service;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.OpenFileNumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor implements MonitorMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(Monitor.class);

  public static final Monitor INSTANCE = new Monitor();

  public static Monitor getInstance() {
    return INSTANCE;
  }

  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public long getDataSizeInByte() {
    try {
      long totalSize = 0;
      for (String dataDir : config.getDataDirs()) {
        totalSize += FileUtils.sizeOfDirectory(SystemFileFactory.INSTANCE.getFile(dataDir));
      }
      return totalSize;
    } catch (Exception e) {
      logger.error("meet error while trying to get data size.", e);
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
  public String getSystemDirectory() {
    try {
      File file = SystemFileFactory.INSTANCE.getFile(config.getSystemDir());
      return file.getAbsolutePath();
    } catch (Exception e) {
      logger.error("meet error while trying to get base dir.", e);
      return "Unavailable";
    }
  }

  @Override
  public boolean getWriteAheadLogStatus() {
    return config.isEnableWal();
  }

  @Override
  public int getTotalOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
  }

  @Override
  public int getDataOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.SEQUENCE_FILE_OPEN_NUM);
  }

  @Override
  public int getWalOpenFileNum() {
    return OpenFileNumUtil.getInstance()
        .get(OpenFileNumUtil.OpenFileNumStatistics.WAL_OPEN_FILE_NUM);
  }

  @Override
  public int getMetadataOpenFileNum() {
    // TODO
    return 0;
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
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
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
