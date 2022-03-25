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

import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * ConfigNodeConfCheck checks parameters in iotdb-confignode.properties when started, and the
 * consistency of some specific parameters on restart
 */
public class ConfigNodeConfCheck {

  // TODO: Code optimize, reuse logic in iotdb-server

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeConfCheck.class);

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();

  private final Properties specialProperties;

  private ConfigNodeConfCheck() {
    specialProperties = new Properties();
  }

  public void checkConfig() throws ConfigurationException, IOException, StartupException {
    // If systemDir does not exist, create systemDir
    File systemDir = new File(conf.getSystemDir());
    if (!systemDir.exists()) {
      if (systemDir.mkdirs()) {
        LOGGER.info("Make system dirs: {}", systemDir);
      } else {
        throw new IOException(
            String.format(
                "Start ConfigNode failed, because couldn't make system dirs: %s.",
                systemDir.getAbsolutePath()));
      }
    }

    File specialPropertiesFile =
        new File(conf.getSystemDir() + File.separator + ConfigNodeConstant.SPECIAL_CONF_NAME);
    if (!specialPropertiesFile.exists()) {
      // Create and write the special properties file when first start the ConfigNode
      if (specialPropertiesFile.createNewFile()) {
        LOGGER.info(
            "Special configuration file {} for ConfigNode is created.",
            specialPropertiesFile.getAbsolutePath());
        writeSpecialProperties(specialPropertiesFile);
        return;
      } else {
        LOGGER.error(
            "Can't create special configuration file {} for ConfigNode. IoTDB-ConfigNode is shutdown.",
            specialPropertiesFile.getAbsolutePath());
        throw new StartupException("Can't create special configuration file");
      }
    }

    try (FileInputStream inputStream = new FileInputStream(specialPropertiesFile)) {
      // Check consistency of the special parameters in the special properties file.
      // Make sure these parameters are the same both in the current properties file and the special
      // properties file.
      specialProperties.load(inputStream);
      checkSpecialProperties();
    }
  }

  /**
   * There are some special parameters that can't be changed after once we start ConfigNode.
   * Therefore, store them in iotdb-confignode-special.properties at the first startup
   */
  private void writeSpecialProperties(File specialPropertiesFile) {
    specialProperties.setProperty("device_group_count", String.valueOf(conf.getDeviceGroupCount()));
    specialProperties.setProperty(
        "device_group_hash_executor_class", conf.getDeviceGroupHashExecutorClass());
    try {
      specialProperties.store(new FileOutputStream(specialPropertiesFile), "");
    } catch (IOException e) {
      LOGGER.error(
          "Can't store special properties file {}.", specialPropertiesFile.getAbsolutePath());
    }
  }

  /** Ensure that special parameters are consistent with each startup except the first one */
  private void checkSpecialProperties() throws ConfigurationException {
    int specialDeviceGroupCount =
        Integer.parseInt(
            specialProperties.getProperty(
                "device_group_count", String.valueOf(conf.getDeviceGroupCount())));
    if (specialDeviceGroupCount != conf.getDeviceGroupCount()) {
      throw new ConfigurationException(
          "device_group_count",
          String.valueOf(conf.getDeviceGroupCount()),
          String.valueOf(specialDeviceGroupCount));
    }

    String specialDeviceGroupHashExecutorClass =
        specialProperties.getProperty(
            "device_group_hash_executor_class", conf.getDeviceGroupHashExecutorClass());
    if (!Objects.equals(
        specialDeviceGroupHashExecutorClass, conf.getDeviceGroupHashExecutorClass())) {
      throw new ConfigurationException(
          "device_group_hash_executor_class",
          conf.getDeviceGroupHashExecutorClass(),
          specialDeviceGroupHashExecutorClass);
    }
  }

  private static class ConfigNodeConfCheckHolder {

    private static final ConfigNodeConfCheck INSTANCE = new ConfigNodeConfCheck();

    private ConfigNodeConfCheckHolder() {
      // empty constructor
    }
  }

  public static ConfigNodeConfCheck getInstance() {
    return ConfigNodeConfCheckHolder.INSTANCE;
  }
}
