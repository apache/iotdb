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
package org.apache.iotdb.confignode.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class ConfigNodeDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeDescriptor.class);

  private final ConfigNodeConf conf = new ConfigNodeConf();

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConf getConf() {
    return conf;
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public URL getPropsUrl() {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
      if (urlString != null) {
        urlString =
            urlString
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + ConfigNodeConstant.CONF_NAME;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = ConfigNodeConf.class.getResource("/" + ConfigNodeConstant.CONF_NAME);
        if (uri != null) {
          return uri;
        }
        LOGGER.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            ConfigNodeConstant.CONF_NAME);
        // update all data seriesPath
        // conf.updatePath();
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + ConfigNodeConstant.CONF_NAME);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      return null;
    }
  }

  private void loadProps() {
    URL url = getPropsUrl();
    if (url == null) {
      LOGGER.warn(
          "Couldn't load the ConfigNode configuration from any of the known sources. Use default configuration.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {

      LOGGER.info("start reading ConfigNode conf file: {}", url);

      Properties properties = new Properties();
      properties.load(inputStream);

      conf.setDeviceGroupCount(
          Integer.parseInt(
              properties.getProperty(
                  "device_group_count", String.valueOf(conf.getDeviceGroupCount()))));

      conf.setDeviceGroupHashExecutorClass(
          properties.getProperty(
              "device_group_hash_executor_class", conf.getDeviceGroupHashExecutorClass()));

      conf.setRpcAddress(properties.getProperty("config_node_rpc_address", conf.getRpcAddress()));

      conf.setRpcPort(
          Integer.parseInt(
              properties.getProperty("config_node_rpc_port", String.valueOf(conf.getRpcPort()))));

      conf.setInternalPort(
          Integer.parseInt(
              properties.getProperty(
                  "config_node_internal_port", String.valueOf(conf.getInternalPort()))));

      conf.setAddressLists(
          properties.getProperty("config_node_address_lists", conf.getAddressLists()));

      conf.setRpcAdvancedCompressionEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_advanced_compression_enable",
                  String.valueOf(conf.isRpcAdvancedCompressionEnable()))));

      conf.setRpcThriftCompressionEnabled(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_thrift_compression_enable",
                  String.valueOf(conf.isRpcThriftCompressionEnabled()))));

      conf.setRpcMaxConcurrentClientNum(
          Integer.parseInt(
              properties.getProperty(
                  "rpc_max_concurrent_client_num",
                  String.valueOf(conf.getRpcMaxConcurrentClientNum()))));

      conf.setThriftDefaultBufferSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))));

      conf.setThriftMaxFrameSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

      conf.setSystemDir(properties.getProperty("system_dir", conf.getSystemDir()));

      conf.setDataDirs(properties.getProperty("data_dirs", conf.getDataDirs()[0]).split(","));

      conf.setRegionReplicaCount(
          Integer.parseInt(
              properties.getProperty(
                  "region_replica_count", String.valueOf(conf.getRegionReplicaCount()))));

      conf.setSchemaRegionCount(
          Integer.parseInt(
              properties.getProperty(
                  "schema_region_count", String.valueOf(conf.getSchemaRegionCount()))));

      conf.setDataRegionCount(
          Integer.parseInt(
              properties.getProperty(
                  "data_region_count", String.valueOf(conf.getDataRegionCount()))));

    } catch (IOException e) {
      LOGGER.warn("Couldn't load ConfigNode conf file, use default config", e);
    }
  }

  public static ConfigNodeDescriptor getInstance() {
    return ConfigNodeDescriptorHolder.INSTANCE;
  }

  private static class ConfigNodeDescriptorHolder {

    private static final ConfigNodeDescriptor INSTANCE = new ConfigNodeDescriptor();

    private ConfigNodeDescriptorHolder() {
      // empty constructor
    }
  }
}
