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
package org.apache.iotdb.confignode.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class ConfigNodeDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeDescriptor.class);

  private final ConfigNodeConfig config = new ConfigNodeConfig();

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConfig getConfig() {
    return config;
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

  private URL getPropsUrl() {
    // The same logic as IoTDBDescriptor
    //    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    //    if (url == null) {
    //      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    //      if (url != null) {
    //        url = url + File.separatorChar + "conf" + File.separatorChar +
    // ConfigNodeConfig.CONFIG_NAME;
    //      } else {
    //        URL uri = ConfigNodeConfig.class.getResource("/" + ConfigNodeConfig.CONFIG_NAME);
    //        if (uri != null) {
    //          return uri;
    //        }
    //        LOGGER.warn(
    //            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config
    // file {}, use default configuration",
    //            ConfigNodeConfig.CONFIG_NAME);
    //        return null;
    //      }
    //    }

    //    try {
    //      return new URL(url);
    //    } catch (MalformedURLException e) {
    //      return null;
    //    }
    return null;
  }

  private void loadProps() {
    URL url = getPropsUrl();
    // TODO: loadProps
  }
}
