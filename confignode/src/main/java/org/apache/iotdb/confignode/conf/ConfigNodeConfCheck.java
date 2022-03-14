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

import org.apache.iotdb.confignode.exception.conf.ConfigurationException;
import org.apache.iotdb.confignode.exception.conf.RepeatConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * ConfigNodeConfCheck checks parameters in iotdb-confignode.properties when started, and the
 * consistency of some specific parameters on restart
 */
public class ConfigNodeConfCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeConfCheck.class);

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();

  private Properties currentProperties;
  private Properties specialProperties;

  public void checkConfig()
      throws ConfigurationException, RepeatConfigurationException, IOException {

    String propsDir = ConfigNodeDescriptor.getInstance().getPropsDir();
    File currentPropertiesFile = new File(propsDir + File.separator + ConfigNodeConstant.CONF_NAME);

    currentProperties = new Properties();
    FileInputStream inputStream = new FileInputStream(currentPropertiesFile);
    currentProperties.load(inputStream);

    File specialPropertiesFile =
        new File(propsDir + File.separator + ConfigNodeConstant.SPECIAL_CONF_NAME);
    if (!specialPropertiesFile.exists()) {
      if (specialPropertiesFile.createNewFile()) {
        LOGGER.info(
            "Special configuration file {} for ConfigNode is created.",
            specialPropertiesFile.getAbsolutePath());
        writeSpecialProperties();
        return;
      } else {
        LOGGER.error(
            "Can't create special configuration file {} for ConfigNode. IoTDB-ConfigNode is shutdown.",
            specialPropertiesFile.getAbsolutePath());
        System.exit(-1);
      }
    }

    inputStream = new FileInputStream(currentPropertiesFile);
    specialProperties.load(inputStream);
    checkSpecialProperties();
  }

  private void writeSpecialProperties() {}

  private void checkSpecialProperties() throws RepeatConfigurationException {
    int deviceGroupCount =
        Integer.parseInt(
            specialProperties.getProperty(
                "device_group_count", String.valueOf(conf.getDeviceGroupCount())));
    int currentDeviceGroupCount =
        Integer.parseInt(
            currentProperties.getProperty(
                "device_group_count", String.valueOf(conf.getDeviceGroupCount())));
    if (deviceGroupCount != currentDeviceGroupCount) {
      throw new RepeatConfigurationException(
          "device_group_count",
          String.valueOf(currentDeviceGroupCount),
          String.valueOf(deviceGroupCount));
    }

    String deviceGroupHashExecutorClass =
        specialProperties.getProperty(
            "device_group_hash_executor_class", conf.getDeviceGroupHashExecutorClass());
    String currentDeviceGroupHashExecutorClass =
        currentProperties.getProperty(
            "device_group_hash_executor_class", conf.getDeviceGroupHashExecutorClass());
    if (!Objects.equals(deviceGroupHashExecutorClass, currentDeviceGroupHashExecutorClass)) {
      throw new RepeatConfigurationException(
          "device_group_hash_executor_class",
          currentDeviceGroupHashExecutorClass,
          deviceGroupHashExecutorClass);
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

  private ConfigNodeConfCheck() {
    LOGGER.info("Starting IoTDB Cluster ConfigNode " + ConfigNodeConstant.VERSION);
  }
}
