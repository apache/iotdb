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
package org.apache.iotdb.db.sync.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncSenderDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(SyncSenderDescriptor.class);
  private SyncSenderConfig conf = new SyncSenderConfig();

  private SyncSenderDescriptor() {
    loadProps();
  }

  public static final SyncSenderDescriptor getInstance() {
    return PostBackDescriptorHolder.INSTANCE;
  }

  public SyncSenderConfig getConfig() {
    return conf;
  }

  public void setConfig(SyncSenderConfig conf) {
    this.conf = conf;
  }

  /**
   * load an properties file and set sync config variables
   */
  private void loadProps() {
    conf.init();
    InputStream inputStream;
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar
            + Constans.CONFIG_NAME;
      } else {
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration",
            Constans.CONFIG_NAME);
        return;
      }
    } else {
      url += (File.separatorChar + Constans.CONFIG_NAME);
    }

    try {
      inputStream = new FileInputStream(new File(url));
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find sync config file {}", url, e);
      return;
    }

    logger.info("Start to read sync config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);

      conf.setServerIp(properties.getProperty("server_ip", conf.getServerIp()));
      conf.setServerPort(Integer
          .parseInt(properties.getProperty("server_port", Integer.toString(conf.getServerPort()))));
      conf.setSyncPeriodInSecond(Integer.parseInt(properties
          .getProperty("sync_period_in_second",
              Integer.toString(conf.getSyncPeriodInSecond()))));
      conf.setSchemaPath(properties.getProperty("iotdb_schema_directory", conf.getSchemaPath()));
      conf.setDataDirectory(
          properties.getProperty("iotdb_bufferWrite_directory", conf.getDataDirectory()));
      String dataDirectory = conf.getDataDirectory();
      if (dataDirectory.length() > 0
          && dataDirectory.charAt(dataDirectory.length() - 1) != File.separatorChar) {
        dataDirectory += File.separatorChar;
      }
      conf.setUuidPath(
          dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.UUID_FILE_NAME);
      conf.setLastFileInfo(
          dataDirectory + Constans.SYNC_CLIENT + File.separatorChar
              + Constans.LAST_LOCAL_FILE_NAME);
      String[] sequenceFileDirectory = conf.getSeqFileDirectory();
      String[] snapshots = new String[conf.getSeqFileDirectory().length];
      for (int i = 0; i < conf.getSeqFileDirectory().length; i++) {
        sequenceFileDirectory[i] = FilePathUtils.regularizePath(sequenceFileDirectory[i]);
        snapshots[i] = sequenceFileDirectory[i] + Constans.SYNC_CLIENT + File.separatorChar
            + Constans.DATA_SNAPSHOT_NAME + File.separatorChar;
      }
      conf.setSeqFileDirectory(sequenceFileDirectory);
      conf.setSnapshotPaths(snapshots);
    } catch (IOException e) {
      logger.warn("Cannot load config file because {}, use default configuration", e);
    } catch (Exception e) {
      logger.warn("Error format in config file because {}, use default configuration", e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        logger.error("Fail to close sync config file input stream because ", e);
      }
    }
  }

  private static class PostBackDescriptorHolder {

    private static final SyncSenderDescriptor INSTANCE = new SyncSenderDescriptor();
  }
}