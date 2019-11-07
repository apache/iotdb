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

package org.apache.iotdb.cluster.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterDescriptor.class);
  private static final ClusterDescriptor INSTANCE = new ClusterDescriptor();

  private ClusterConfig config = new ClusterConfig();

  private ClusterDescriptor() {
    loadProps();
  }

  public ClusterConfig getConfig() {
    return config;
  }

  public static ClusterDescriptor getINSTANCE() {
    return INSTANCE;
  }

  private String getPropsUrl() {
    String url = System.getProperty(ClusterConstant.CLUSTER_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + ClusterConfig.CONFIG_NAME;
      } else {
        logger.warn(
            "Cannot find IOTDB_HOME or CLUSTER_CONF environment variable when loading "
                + "config file {}, use default configuration",
            ClusterConfig.CONFIG_NAME);
        // update all data seriesPath
        return null;
      }
    } else {
      url += (File.separatorChar + ClusterConfig.CONFIG_NAME);
    }
    return url;
  }

  /**
   * load an property file and set TsfileDBConfig variables.
   */
  private void loadProps() {

    String url = getPropsUrl();
    if (url == null) {
      return;
    }

    try (InputStream inputStream = new FileInputStream(new File(url))) {
      logger.info("Start to read config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);

      config.setLocalIP(properties.getProperty("LOCAL_IP", config.getLocalIP()));

      config.setLocalMetaPort(Integer.parseInt(properties.getProperty("LOCAL_META_PORT",
          String.valueOf(config.getLocalMetaPort()))));

      config.setMaxConcurrentClientNum(Integer.parseInt(properties.getProperty(
          "MAX_CONCURRENT_CLIENT_NUM", String.valueOf(config.getMaxConcurrentClientNum()))));

      config.setRpcThriftCompressionEnabled(Boolean.parseBoolean(properties.getProperty(
          "ENABLE_THRIFT_COMPRESSION", String.valueOf(config.isRpcThriftCompressionEnabled()))));

      String seedUrls = properties.getProperty("SEED_NODES");
      if (seedUrls != null) {
        List<String> urlList = new ArrayList<>();
        String[] split = seedUrls.split(",");
        for (String nodeUrl : split) {
          nodeUrl = nodeUrl.trim();
          if ("".equals(nodeUrl)) {
            continue;
          }
          urlList.add(nodeUrl);
        }
        config.setSeedNodeUrls(urlList);
      }


    } catch (IOException e) {
      logger.warn("Fail to find config file {}", url, e);
    }
  }

}
