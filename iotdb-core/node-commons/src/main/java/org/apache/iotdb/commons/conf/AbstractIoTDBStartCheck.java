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
package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.commons.i18n.CommonMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;

public abstract class AbstractIoTDBStartCheck {

  protected static final Logger logger = LoggerFactory.getLogger(AbstractIoTDBStartCheck.class);

  // this file is located in data/system/system.properties
  // If user delete folder "data", system.properties can reset.
  public static final String PROPERTIES_FILE_NAME = "system.properties";

  protected Properties properties = new Properties();

  protected final Map<String, Supplier<String>> systemProperties = new HashMap<>();
  protected final SystemPropertiesHandler systemPropertiesHandler;

  protected static final String IOTDB_VERSION_STRING = "iotdb_version";
  protected static final String COMMIT_ID_STRING = "commit_id";
  protected static final String CLUSTER_ID = "cluster_id";

  protected AbstractIoTDBStartCheck() {
    systemPropertiesHandler = getSystemPropertiesHandler();

    systemProperties.put(IOTDB_VERSION_STRING, () -> IoTDBConstant.VERSION);
    systemProperties.put(COMMIT_ID_STRING, () -> IoTDBConstant.BUILD_INFO);
    for (String param : getVariableParamValueTable().keySet()) {
      systemProperties.put(param, () -> getVal(param));
    }
  }

  protected abstract SystemPropertiesHandler getSystemPropertiesHandler();

  protected abstract Map<String, Supplier<String>> getVariableParamValueTable();

  protected abstract String getNodeIdKey();

  protected abstract void loadNodeId(int nodeId);

  protected abstract void loadClusterId(String clusterId);

  /** check and create directory before start the node. */
  public abstract void checkDirectory() throws ConfigurationException, IOException;

  /**
   * check configuration in system.properties when starting the node
   *
   * <p>When init: create system.properties directly
   *
   * <p>When upgrading the system.properties: (1) create system.properties.tmp (2) delete
   * system.properties (3) rename system.properties.tmp to system.properties
   */
  public void checkSystemConfig() throws ConfigurationException, IOException {
    // read properties from system.properties
    properties = systemPropertiesHandler.read();

    if (systemPropertiesHandler.isFirstStart()) {
      validateFirstStart();
    } else {
      // check whether upgrading from <=v0.9
      if (!properties.containsKey(IOTDB_VERSION_STRING)) {
        logger.error(
            CommonMessages
                .MISC_LOG_DO_NOT_UPGRADE_IOTDB_FROM_V0_9_OR_LOWER_VERSION_TO_V1_0_9878EC88);
        System.exit(-1);
      }
      String versionString = properties.getProperty(IOTDB_VERSION_STRING);
      if (versionString.startsWith("0.")) {
        logger.error(CommonMessages.IOTDB_VERSION_TOO_OLD);
        System.exit(-1);
      }
      checkImmutableSystemProperties();
    }
  }

  /** Check all immutable properties */
  protected void checkImmutableSystemProperties() throws IOException {
    for (Entry<String, Supplier<String>> entry : systemProperties.entrySet()) {
      if (!properties.containsKey(entry.getKey())) {
        upgradePropertiesFileFromBrokenFile();
        logger.info(CommonMessages.REPAIR_SYSTEM_PROPERTIES, entry.getKey());
      }
    }

    if (properties.containsKey(getNodeIdKey())) {
      loadNodeId(Integer.parseInt(properties.getProperty(getNodeIdKey())));
    }
    if (properties.containsKey(CLUSTER_ID)) {
      loadClusterId(properties.getProperty(CLUSTER_ID));
    }
    checkExtraImmutableProperties();
  }

  /** Node-specific validation on first start, no-op by default. */
  protected void validateFirstStart() throws ConfigurationException {}

  /** Node-specific immutable properties, no-op by default. */
  protected void checkExtraImmutableProperties() throws IOException {}

  /** repair broken properties */
  protected void upgradePropertiesFileFromBrokenFile() throws IOException {
    systemProperties.forEach(
        (k, v) -> {
          if (!properties.containsKey(k)) {
            properties.setProperty(k, v.get());
          }
        });
    properties.setProperty(IOTDB_VERSION_STRING, IoTDBConstant.VERSION);
    properties.setProperty(COMMIT_ID_STRING, IoTDBConstant.BUILD_INFO);
    systemPropertiesHandler.overwrite(properties);
  }

  protected void throwException(String parameter, Object badValue) throws ConfigurationException {
    throw new ConfigurationException(
        parameter,
        String.valueOf(badValue),
        properties.getProperty(parameter),
        String.format(
            CommonMessages.PARAMETER_CANNOT_BE_MODIFIED_AFTER_FIRST_STARTUP_FMT, parameter));
  }

  protected String getVal(String paramName) {
    if (getVariableParamValueTable().containsKey(paramName)) {
      return getVariableParamValueTable().get(paramName).get();
    } else {
      return null;
    }
  }

  public void serializeNodeId(int nodeId) throws IOException {
    systemPropertiesHandler.put(getNodeIdKey(), String.valueOf(nodeId));
  }

  public void serializeClusterID(String clusterId) throws IOException {
    systemPropertiesHandler.put(CLUSTER_ID, clusterId);
  }

  public void serializeMutableSystemPropertiesIfNecessary() throws IOException {
    long startTime = System.currentTimeMillis();
    boolean needsSerialize = false;
    for (String param : getVariableParamValueTable().keySet()) {
      if (!properties.getProperty(param).equals(getVal(param))) {
        needsSerialize = true;
      }
    }

    if (needsSerialize) {
      generateOrOverwriteSystemPropertiesFile();
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        CommonMessages
            .MISC_LOG_SERIALIZE_MUTABLE_SYSTEM_PROPERTIES_SUCCESSFULLY_WHICH_TAKES_4656A206,
        (endTime - startTime));
  }

  public void generateOrOverwriteSystemPropertiesFile() throws IOException {
    systemProperties.forEach((k, v) -> properties.setProperty(k, v.get()));
    systemPropertiesHandler.overwrite(properties);
  }

  public Properties getProperties() {
    return properties;
  }
}
