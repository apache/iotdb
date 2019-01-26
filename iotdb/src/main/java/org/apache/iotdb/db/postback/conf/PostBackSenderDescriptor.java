/**
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
package org.apache.iotdb.db.postback.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lta
 */
public class PostBackSenderDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostBackSenderDescriptor.class);
  private static final String POSTBACK = "postback";
  private PostBackSenderConfig conf = new PostBackSenderConfig();

  private PostBackSenderDescriptor() {
    loadProps();
  }

  public static final PostBackSenderDescriptor getInstance() {
    return PostBackDescriptorHolder.INSTANCE;
  }

  public PostBackSenderConfig getConfig() {
    return conf;
  }

  public void setConfig(PostBackSenderConfig conf) {
    this.conf = conf;
  }

  /**
   * load an properties file and set TsfileDBConfig variables
   */
  private void loadProps() {
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar
            + PostBackSenderConfig.CONFIG_NAME;
      } else {
        LOGGER.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration",
            PostBackSenderConfig.CONFIG_NAME);
        return;
      }
    } else {
      url += (File.separatorChar + PostBackSenderConfig.CONFIG_NAME);
    }

    LOGGER.info("Start to read config file {}", url);
    Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(new File(url))) {
      properties.load(inputStream);

      conf.setServerIp(properties.getProperty("server_ip", conf.getServerIp()));
      conf.setServerPort(Integer
          .parseInt(properties.getProperty("server_port", Integer.toString(conf.getServerPort()))));

      conf.setClientPort(Integer
          .parseInt(properties.getProperty("client_port", Integer.toString(conf.getClientPort()))));
      conf.setUploadCycleInSeconds(Integer.parseInt(properties
          .getProperty("upload_cycle_in_seconds",
              Integer.toString(conf.getUploadCycleInSeconds()))));
      conf.setSchemaPath(properties.getProperty("iotdb_schema_directory", conf.getSchemaPath()));
      conf.setClearEnable(Boolean
          .parseBoolean(
              properties.getProperty("is_clear_enable", Boolean.toString(conf.getClearEnable()))));
      conf.setUuidPath(conf.getDataDirectory() + POSTBACK + File.separator + "uuid.txt");
      conf.setLastFileInfo(
          conf.getDataDirectory() + POSTBACK + File.separator + "lastLocalFileList.txt");
      String[] iotdbBufferwriteDirectory = conf.getIotdbBufferwriteDirectory();
      String[] snapshots = new String[conf.getIotdbBufferwriteDirectory().length];
      for (int i = 0; i < conf.getIotdbBufferwriteDirectory().length; i++) {
        iotdbBufferwriteDirectory[i] = new File(iotdbBufferwriteDirectory[i]).getAbsolutePath();
        if (!iotdbBufferwriteDirectory[i].endsWith(File.separator)) {
          iotdbBufferwriteDirectory[i] = iotdbBufferwriteDirectory[i] + File.separator;
        }
        snapshots[i] = iotdbBufferwriteDirectory[i] + POSTBACK + File.separator + "dataSnapshot"
            + File.separator;
      }
      conf.setIotdbBufferwriteDirectory(iotdbBufferwriteDirectory);
      conf.setSnapshotPaths(snapshots);
    } catch (IOException e) {
      LOGGER.warn("Cannot load config file because {}, use default configuration", e);
    } catch (Exception e) {
      LOGGER.warn("Error format in config file because {}, use default configuration", e);
    }
  }

  private static class PostBackDescriptorHolder {

    private static final PostBackSenderDescriptor INSTANCE = new PostBackSenderDescriptor();
  }
}