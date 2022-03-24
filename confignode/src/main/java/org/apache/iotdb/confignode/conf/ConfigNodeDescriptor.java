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

  public String getPropsDir() {
    // Check if CONFIG_NODE_CONF is set
    String propsDir = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF, null);
    if (propsDir == null) {
      // Check if CONFIG_NODE_HOME is set
      propsDir = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
      if (propsDir == null) {
        // When start ConfigNode with script, CONFIG_NODE_CONF and CONFIG_NODE_HOME must be set.
        // Therefore, this case is TestOnly
        // TODO: Specify a test dir
      }
      propsDir = propsDir + File.separator + ConfigNodeConstant.CONF_DIR;
    }

    return propsDir;
  }

  public URL getPropsUrl() {
    String url = getPropsDir();

    if (url == null) {
      return null;
    }

    // Add props prefix
    if (!url.startsWith("file:") && !url.startsWith("classpath:")) {
      url = "file:" + url;
    }

    // Add props suffix
    if (!url.endsWith(".properties")) {
      url += File.separator + ConfigNodeConstant.CONF_NAME;
    }

    try {
      return new URL(url);
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
