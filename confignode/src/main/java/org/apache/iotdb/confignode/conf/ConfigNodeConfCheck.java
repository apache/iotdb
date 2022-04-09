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
    createDir(systemDir);

    // If consensusDir does not exist, create consensusDir
    File consensusDir = new File(conf.getConsensusDir());
    createDir(consensusDir);

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

  private void createDir(File dir) throws IOException {
    if (!dir.exists()) {
      if (dir.mkdirs()) {
        LOGGER.info("Make dirs: {}", dir);
      } else {
        throw new IOException(
            String.format(
                "Start ConfigNode failed, because couldn't make system dirs: %s.",
                dir.getAbsolutePath()));
      }
    }
  }

  /**
   * There are some special parameters that can't be changed after once we start ConfigNode.
   * Therefore, store them in iotdb-confignode-special.properties at the first startup
   */
  private void writeSpecialProperties(File specialPropertiesFile) {
    specialProperties.setProperty(
        "series_partition_slot_num", String.valueOf(conf.getSeriesPartitionSlotNum()));
    specialProperties.setProperty(
        "series_partition_slot_executor_class", conf.getSeriesPartitionSlotExecutorClass());
    try {
      specialProperties.store(new FileOutputStream(specialPropertiesFile), "");
    } catch (IOException e) {
      LOGGER.error(
          "Can't store special properties file {}.", specialPropertiesFile.getAbsolutePath());
    }
  }

  /** Ensure that special parameters are consistent with each startup except the first one */
  private void checkSpecialProperties() throws ConfigurationException {
    int specialSeriesPartitionSlotNum =
        Integer.parseInt(
            specialProperties.getProperty(
                "series_partition_slot_num", String.valueOf(conf.getSeriesPartitionSlotNum())));
    if (specialSeriesPartitionSlotNum != conf.getSeriesPartitionSlotNum()) {
      throw new ConfigurationException(
          "series_partition_slot_num",
          String.valueOf(conf.getSeriesPartitionSlotNum()),
          String.valueOf(specialSeriesPartitionSlotNum));
    }

    String specialSeriesPartitionSlotExecutorClass =
        specialProperties.getProperty(
            "series_partition_slot_executor_class", conf.getSeriesPartitionSlotExecutorClass());
    if (!Objects.equals(
        specialSeriesPartitionSlotExecutorClass, conf.getSeriesPartitionSlotExecutorClass())) {
      throw new ConfigurationException(
          "series_partition_slot_executor_class",
          conf.getSeriesPartitionSlotExecutorClass(),
          specialSeriesPartitionSlotExecutorClass);
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
