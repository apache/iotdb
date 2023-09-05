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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.rescon.disk.DirectoryChecker;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;

public class IoTDBStartCheck {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBStartCheck.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  // this file is located in data/system/schema/system.properties
  // If user delete folder "data", system.properties can reset.
  public static final String PROPERTIES_FILE_NAME = "system.properties";
  private static final String SCHEMA_DIR = config.getSchemaDir();

  private boolean isFirstStart = false;

  private final File propertiesFile;
  private final File tmpPropertiesFile;

  private final Properties properties = new Properties();

  private final Map<String, Supplier<String>> systemProperties = new HashMap<>();

  // region params need checking, determined when first start
  private static final String SYSTEM_PROPERTIES_STRING = "System properties:";
  private static final String DATA_REGION_NUM = "data_region_num";

  // endregion
  // region params don't need checking and can be updated
  private static final String INTERNAL_ADDRESS = "dn_internal_address";
  private static final String INTERNAL_PORT = "dn_internal_port";
  private static final String RPC_ADDRESS = "dn_rpc_address";
  private static final String RPC_PORT = "dn_rpc_port";
  private static final String MPP_DATA_EXCHANGE_PORT = "dn_mpp_data_exchange_port";
  private static final String SCHEMA_REGION_CONSENSUS_PORT = "dn_schema_region_consensus_port";
  private static final String DATA_REGION_CONSENSUS_PORT = "dn_data_region_consensus_port";
  // Mutable system parameters
  private static final Map<String, Supplier<String>> variableParamValueTable = new HashMap<>();

  static {
    variableParamValueTable.put(
        INTERNAL_ADDRESS, () -> String.valueOf(config.getInternalAddress()));
    variableParamValueTable.put(INTERNAL_PORT, () -> String.valueOf(config.getInternalPort()));
    variableParamValueTable.put(RPC_ADDRESS, () -> String.valueOf(config.getRpcAddress()));
    variableParamValueTable.put(RPC_PORT, () -> String.valueOf(config.getRpcPort()));
    variableParamValueTable.put(
        MPP_DATA_EXCHANGE_PORT, () -> String.valueOf(config.getMppDataExchangePort()));
    variableParamValueTable.put(
        SCHEMA_REGION_CONSENSUS_PORT, () -> String.valueOf(config.getSchemaRegionConsensusPort()));
    variableParamValueTable.put(
        DATA_REGION_CONSENSUS_PORT, () -> String.valueOf(config.getDataRegionConsensusPort()));
  }
  // endregion
  // region params don't need checking, determined by the system
  private static final String IOTDB_VERSION_STRING = "iotdb_version";
  private static final String COMMIT_ID_STRING = "commit_id";
  private static final String DATA_NODE_ID = "data_node_id";
  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL = "schema_region_consensus_protocol";
  private static final String DATA_REGION_CONSENSUS_PROTOCOL = "data_region_consensus_protocol";
  // endregion
  // region params of old versions
  private static final String VIRTUAL_STORAGE_GROUP_NUM = "virtual_storage_group_num";
  // endregion

  public static IoTDBStartCheck getInstance() {
    return IoTDBConfigCheckHolder.INSTANCE;
  }

  private static class IoTDBConfigCheckHolder {

    private static final IoTDBStartCheck INSTANCE = new IoTDBStartCheck();
  }

  private String getVal(String paramName) {
    if (variableParamValueTable.containsKey(paramName)) {
      return variableParamValueTable.get(paramName).get();
    } else {
      return null;
    }
  }

  private IoTDBStartCheck() {
    logger.info("Starting IoTDB {}", IoTDBConstant.VERSION_WITH_BUILD);

    // check whether SCHEMA_DIR exists, create if not exists
    File dir = SystemFileFactory.INSTANCE.getFile(SCHEMA_DIR);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        logger.error("Can not create schema dir: {}", SCHEMA_DIR);
        System.exit(-1);
      } else {
        logger.info(" {} dir has been created.", SCHEMA_DIR);
      }
    }

    propertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBStartCheck.SCHEMA_DIR + File.separator + PROPERTIES_FILE_NAME);
    tmpPropertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBStartCheck.SCHEMA_DIR + File.separator + PROPERTIES_FILE_NAME + ".tmp");

    systemProperties.put(IOTDB_VERSION_STRING, () -> IoTDBConstant.VERSION);
    systemProperties.put(COMMIT_ID_STRING, () -> IoTDBConstant.BUILD_INFO);
    for (String param : variableParamValueTable.keySet()) {
      systemProperties.put(param, () -> getVal(param));
    }
  }

  /** check configuration in system.properties when starting IoTDB */
  public boolean checkIsFirstStart() throws IOException {
    // system init first time, no need to check, write system.properties and return
    if (!propertiesFile.exists() && !tmpPropertiesFile.exists()) {
      // create system.properties
      if (propertiesFile.createNewFile()) {
        logger.info(" {} has been created.", propertiesFile.getAbsolutePath());
      } else {
        logger.error("can not create {}", propertiesFile.getAbsolutePath());
        System.exit(-1);
      }

      // write properties to system.properties
      try (FileOutputStream outputStream = new FileOutputStream(propertiesFile)) {
        systemProperties.forEach((k, v) -> properties.setProperty(k, v.get()));
        properties.store(outputStream, SYSTEM_PROPERTIES_STRING);
      }
      isFirstStart = true;
      return true;
    }

    if (!propertiesFile.exists() && tmpPropertiesFile.exists()) {
      // rename tmp file to system.properties, no need to check
      FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
      logger.info("rename {} to {}", tmpPropertiesFile, propertiesFile);
      isFirstStart = false;
      return false;
    } else if (propertiesFile.exists() && tmpPropertiesFile.exists()) {
      // both files exist, remove tmp file
      FileUtils.forceDelete(tmpPropertiesFile);
      logger.info("remove {}", tmpPropertiesFile);
    }
    isFirstStart = false;
    return false;
  }

  /**
   * check and create directory before start IoTDB.
   *
   * <p>(1) try to create directory, avoid the inability to create directory at runtime due to lack
   * of permissions. (2) try to check if the directory is occupied, avoid multiple IoTDB processes
   * accessing same director.
   */
  public void checkDirectory() throws ConfigurationException, IOException {
    // check data dirs TODO(zhm) only check local directories
    for (String dataDir : config.getLocalDataDirs()) {
      DirectoryChecker.getInstance().registerDirectory(new File(dataDir));
    }
    if (config.isClusterMode()
        && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      if (DirectoryChecker.getInstance().isCrossDisk(config.getDataDirs())) {
        throw new ConfigurationException(
            "Configuring the data directories as cross-disk directories is not supported under RatisConsensus(it will be supported in a later version).");
      }
    }
    // check system dir
    DirectoryChecker.getInstance().registerDirectory(new File(config.getSystemDir()));
    // check WAL dir
    if (!(config.isClusterMode()
            && config
                .getDataRegionConsensusProtocolClass()
                .equals(ConsensusFactory.RATIS_CONSENSUS))
        && !config.getWalMode().equals(WALMode.DISABLE)) {
      for (String walDir : commonConfig.getWalDirs()) {
        DirectoryChecker.getInstance().registerDirectory(new File(walDir));
      }
    }
    // in cluster mode, check consensus dir
    if (config.isClusterMode()) {
      DirectoryChecker.getInstance().registerDirectory(new File(config.getConsensusDir()));
    }
  }

  /**
   * check configuration in system.properties when starting IoTDB
   *
   * <p>When init: create system.properties directly
   *
   * <p>When upgrading the system.properties: (1) create system.properties.tmp (2) delete
   * system.properties (3) rename system.properties.tmp to system.properties
   */
  public void checkSystemConfig() throws ConfigurationException, IOException {
    // read properties from system.properties
    try (FileInputStream inputStream = new FileInputStream(propertiesFile);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET)) {
      properties.load(inputStreamReader);
    }

    if (isFirstStart) {
      // overwrite system.properties when first start
      try (FileOutputStream outputStream = new FileOutputStream(propertiesFile)) {
        systemProperties.forEach((k, v) -> properties.setProperty(k, v.get()));
        properties.store(outputStream, SYSTEM_PROPERTIES_STRING);
      }
      if (config.isClusterMode()
          && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
          && config.getWalMode().equals(WALMode.DISABLE)) {
        throw new ConfigurationException(
            "Configuring the WALMode as disable is not supported under IoTConsensus");
      }
    } else {
      // check whether upgrading from <=v0.9
      if (!properties.containsKey(IOTDB_VERSION_STRING)) {
        logger.error(
            "DO NOT UPGRADE IoTDB from v0.9 or lower version to v1.0!"
                + " Please upgrade to v0.10 first");
        System.exit(-1);
      }
      String versionString = properties.getProperty(IOTDB_VERSION_STRING);
      if (versionString.startsWith("0.")) {
        logger.error("IoTDB version is too old");
        System.exit(-1);
      }
      checkImmutableSystemProperties();
    }
  }

  /** repair broken properties */
  private void upgradePropertiesFileFromBrokenFile() throws IOException {
    // create an empty tmpPropertiesFile
    if (tmpPropertiesFile.createNewFile()) {
      logger.info("Create system.properties.tmp {}.", tmpPropertiesFile);
    } else {
      logger.error("Create system.properties.tmp {} failed.", tmpPropertiesFile);
      System.exit(-1);
    }

    try (FileOutputStream tmpFOS = new FileOutputStream(tmpPropertiesFile.toString())) {
      systemProperties.forEach(
          (k, v) -> {
            if (!properties.containsKey(k)) {
              properties.setProperty(k, v.get());
            }
          });
      properties.setProperty(IOTDB_VERSION_STRING, IoTDBConstant.VERSION);
      properties.setProperty(COMMIT_ID_STRING, IoTDBConstant.BUILD_INFO);
      properties.store(tmpFOS, SYSTEM_PROPERTIES_STRING);
      // upgrade finished, delete old system.properties file
      if (propertiesFile.exists()) {
        Files.delete(propertiesFile.toPath());
      }
    }
    // rename system.properties.tmp to system.properties
    FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
  }

  /** Check all immutable properties */
  private void checkImmutableSystemProperties() throws IOException {
    for (Entry<String, Supplier<String>> entry : systemProperties.entrySet()) {
      if (!properties.containsKey(entry.getKey())) {
        upgradePropertiesFileFromBrokenFile();
        logger.info("repair system.properties, lack {}", entry.getKey());
      }
    }

    // load configuration from system properties only when start as Data node
    if (properties.containsKey(IoTDBConstant.CLUSTER_NAME)) {
      config.setClusterName(properties.getProperty(IoTDBConstant.CLUSTER_NAME));
    }
    if (properties.containsKey(DATA_NODE_ID)) {
      config.setDataNodeId(Integer.parseInt(properties.getProperty(DATA_NODE_ID)));
    }

    if (properties.containsKey(SCHEMA_REGION_CONSENSUS_PROTOCOL)) {
      config.setSchemaRegionConsensusProtocolClass(
          properties.getProperty(SCHEMA_REGION_CONSENSUS_PROTOCOL));
    }

    if (properties.containsKey(DATA_REGION_CONSENSUS_PROTOCOL)) {
      config.setDataRegionConsensusProtocolClass(
          properties.getProperty(DATA_REGION_CONSENSUS_PROTOCOL));
    }
  }

  private void throwException(String parameter, Object badValue) throws ConfigurationException {
    throw new ConfigurationException(
        parameter,
        String.valueOf(badValue),
        properties.getProperty(parameter),
        parameter + "can't be modified after first startup");
  }

  // reload properties from system.properties
  private void reloadProperties() throws IOException {
    try (FileInputStream inputStream = new FileInputStream(propertiesFile);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET)) {
      properties.load(inputStreamReader);
    }
  }

  /** call this method to serialize ClusterName and DataNodeId */
  public void serializeClusterNameAndDataNodeId(String clusterName, int dataNodeId)
      throws IOException {
    // create an empty tmpPropertiesFile
    if (tmpPropertiesFile.createNewFile()) {
      logger.info("Create system.properties.tmp {}.", tmpPropertiesFile);
    } else {
      logger.error("Create system.properties.tmp {} failed.", tmpPropertiesFile);
      System.exit(-1);
    }

    reloadProperties();

    try (FileOutputStream tmpFOS = new FileOutputStream(tmpPropertiesFile.toString())) {
      properties.setProperty(IoTDBConstant.CLUSTER_NAME, clusterName);
      properties.setProperty(DATA_NODE_ID, String.valueOf(dataNodeId));
      properties.store(tmpFOS, SYSTEM_PROPERTIES_STRING);
      // serialize finished, delete old system.properties file
      if (propertiesFile.exists()) {
        Files.delete(propertiesFile.toPath());
      }
    }
    // rename system.properties.tmp to system.properties
    FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
  }

  public boolean checkConsensusProtocolExists(TConsensusGroupType type) {
    if (type == TConsensusGroupType.DataRegion) {
      return properties.containsKey(DATA_REGION_CONSENSUS_PROTOCOL);
    } else if (type == TConsensusGroupType.SchemaRegion) {
      return properties.containsKey(SCHEMA_REGION_CONSENSUS_PROTOCOL);
    }

    logger.error("Unexpected consensus group type");
    return false;
  }

  public void serializeMutableSystemPropertiesIfNecessary() throws IOException {
    boolean needsSerialize = false;
    for (String param : variableParamValueTable.keySet()) {
      if (!(properties.getProperty(param).equals(getVal(param)))) {
        needsSerialize = true;
      }
    }

    if (needsSerialize) {
      try (FileOutputStream outputStream = new FileOutputStream(propertiesFile)) {
        systemProperties.forEach((k, v) -> properties.setProperty(k, v.get()));
        properties.store(outputStream, SYSTEM_PROPERTIES_STRING);
      }
    }
  }
}
