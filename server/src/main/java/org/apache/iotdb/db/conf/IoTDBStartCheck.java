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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.directories.DirectoryChecker;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

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
import java.util.Objects;
import java.util.Properties;

public class IoTDBStartCheck {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBStartCheck.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  // this file is located in data/system/schema/system.properties
  // If user delete folder "data", system.properties can reset.
  private static final String PROPERTIES_FILE_NAME = "system.properties";
  private static final String SCHEMA_DIR = config.getSchemaDir();
  private static final String[] WAL_DIRS = commonConfig.getWalDirs();

  private File propertiesFile;
  private File tmpPropertiesFile;

  private final Properties properties = new Properties();

  private final Map<String, String> systemProperties = new HashMap<>();

  private static final String SYSTEM_PROPERTIES_STRING = "System properties:";

  private static final String TIMESTAMP_PRECISION_STRING = "timestamp_precision";
  private static final String timestampPrecision = config.getTimestampPrecision();

  private static final String PARTITION_INTERVAL_STRING = "time_partition_interval";
  private static final long timePartitionInterval = config.getTimePartitionInterval();

  private static final String TSFILE_FILE_SYSTEM_STRING = "tsfile_storage_fs";
  private static final String tsfileFileSystem = config.getTsFileStorageFs().toString();

  private static final String TAG_ATTRIBUTE_SIZE_STRING = "tag_attribute_total_size";
  private static final String tagAttributeTotalSize =
      String.valueOf(config.getTagAttributeTotalSize());

  private static final String TAG_ATTRIBUTE_FLUSH_INTERVAL = "tag_attribute_flush_interval";
  private static final String tagAttributeFlushInterval =
      String.valueOf(config.getTagAttributeFlushInterval());

  private static final String MAX_DEGREE_OF_INDEX_STRING = "max_degree_of_index_node";
  private static final String maxDegreeOfIndexNode =
      String.valueOf(TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode());

  private static final String DATA_REGION_NUM = "data_region_num";
  // for upgrading from old file
  private static final String VIRTUAL_STORAGE_GROUP_NUM = "virtual_storage_group_num";
  private static final String dataRegionNum = String.valueOf(config.getDataRegionNum());

  private static final String ENABLE_ID_TABLE = "enable_id_table";
  private static final String enableIDTable = String.valueOf(config.isEnableIDTable());

  private static final String ENABLE_ID_TABLE_LOG_FILE = "enable_id_table_log_file";
  private static final String enableIdTableLogFile =
      String.valueOf(config.isEnableIDTableLogFile());

  private static final String SCHEMA_ENGINE_MODE = "schema_engine_mode";
  private static final String schemaEngineMode = String.valueOf(config.getSchemaEngineMode());

  private static final String TIME_ENCODER_KEY = "time_encoder";
  private static final String timeEncoderValue =
      String.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());

  private static final String DATA_NODE_ID = "data_node_id";

  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL = "schema_region_consensus_protocol";

  private static final String DATA_REGION_CONSENSUS_PROTOCOL = "data_region_consensus_protocol";

  private static final String IOTDB_VERSION_STRING = "iotdb_version";

  private static final String INTERNAL_ADDRESS = "dn_internal_address";
  private static final String internalAddress = config.getInternalAddress();

  private static final String INTERNAL_PORT = "dn_internal_port";
  private static final String internalPort = String.valueOf(config.getInternalPort());

  private static final String RPC_ADDRESS = "dn_rpc_address";
  private static final String rpcAddress = config.getRpcAddress();

  private static final String RPC_PORT = "dn_rpc_port";
  private static final String rpcPort = String.valueOf(config.getRpcPort());

  private static final String MPP_DATA_EXCHANGE_PORT = "dn_mpp_data_exchange_port";
  private static final String mppDataExchangePort = String.valueOf(config.getMppDataExchangePort());

  private static final String SCHEMA_REGION_CONSENSUS_PORT = "dn_schema_region_consensus_port";
  private static final String schemaRegionConsensusPort =
      String.valueOf(config.getSchemaRegionConsensusPort());

  private static final String DATA_REGION_CONSENSUS_PORT = "dn_data_region_consensus_port";
  private static final String dataRegionConsensusPort =
      String.valueOf(config.getDataRegionConsensusPort());

  public static IoTDBStartCheck getInstance() {
    return IoTDBConfigCheckHolder.INSTANCE;
  }

  private static class IoTDBConfigCheckHolder {

    private static final IoTDBStartCheck INSTANCE = new IoTDBStartCheck();
  }

  private IoTDBStartCheck() {
    logger.info("Starting IoTDB " + IoTDBConstant.VERSION_WITH_BUILD);

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

    // Check time stamp precision
    if (!("ms".equals(timestampPrecision)
        || "us".equals(timestampPrecision)
        || "ns".equals(timestampPrecision))) {
      logger.error(
          "Wrong {}, please set as: ms, us or ns ! Current is: {}",
          TIMESTAMP_PRECISION_STRING,
          timestampPrecision);
      System.exit(-1);
    }

    systemProperties.put(IOTDB_VERSION_STRING, IoTDBConstant.VERSION);
    systemProperties.put(TIMESTAMP_PRECISION_STRING, timestampPrecision);
    systemProperties.put(PARTITION_INTERVAL_STRING, String.valueOf(timePartitionInterval));
    systemProperties.put(TSFILE_FILE_SYSTEM_STRING, tsfileFileSystem);
    systemProperties.put(TAG_ATTRIBUTE_SIZE_STRING, tagAttributeTotalSize);
    systemProperties.put(TAG_ATTRIBUTE_FLUSH_INTERVAL, tagAttributeFlushInterval);
    systemProperties.put(MAX_DEGREE_OF_INDEX_STRING, maxDegreeOfIndexNode);
    systemProperties.put(DATA_REGION_NUM, dataRegionNum);
    systemProperties.put(TIME_ENCODER_KEY, timeEncoderValue);
    systemProperties.put(ENABLE_ID_TABLE, enableIDTable);
    systemProperties.put(ENABLE_ID_TABLE_LOG_FILE, enableIdTableLogFile);
    systemProperties.put(SCHEMA_ENGINE_MODE, schemaEngineMode);
    systemProperties.put(INTERNAL_ADDRESS, internalAddress);
    systemProperties.put(INTERNAL_PORT, internalPort);
    systemProperties.put(RPC_ADDRESS, rpcAddress);
    systemProperties.put(RPC_PORT, rpcPort);
    systemProperties.put(MPP_DATA_EXCHANGE_PORT, mppDataExchangePort);
    systemProperties.put(SCHEMA_REGION_CONSENSUS_PORT, schemaRegionConsensusPort);
    systemProperties.put(DATA_REGION_CONSENSUS_PORT, dataRegionConsensusPort);
  }

  /**
   * check and create directory before start IoTDB.
   *
   * <p>(1) try to create directory, avoid the inability to create directory at runtime due to lack
   * of permissions. (2) try to check if the directory is occupied, avoid multiple IoTDB processes
   * accessing same director.
   */
  public void checkDirectory() throws ConfigurationException, IOException {
    // check data dirs
    for (String dataDir : config.getDataDirs()) {
      DirectoryChecker.getInstance().registerDirectory(new File(dataDir));
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
  public void checkConfig() throws ConfigurationException, IOException {
    propertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBStartCheck.SCHEMA_DIR + File.separator + PROPERTIES_FILE_NAME);
    tmpPropertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBStartCheck.SCHEMA_DIR + File.separator + PROPERTIES_FILE_NAME + ".tmp");

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
        systemProperties.forEach(properties::setProperty);
        properties.store(outputStream, SYSTEM_PROPERTIES_STRING);
      }
      return;
    }

    if (!propertiesFile.exists() && tmpPropertiesFile.exists()) {
      // rename tmp file to system.properties, no need to check
      FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
      logger.info("rename {} to {}", tmpPropertiesFile, propertiesFile);
      return;
    } else if (propertiesFile.exists() && tmpPropertiesFile.exists()) {
      // both files exist, remove tmp file
      FileUtils.forceDelete(tmpPropertiesFile);
      logger.info("remove {}", tmpPropertiesFile);
    }

    // no tmp file, read properties from system.properties
    try (FileInputStream inputStream = new FileInputStream(propertiesFile);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET)) {
      properties.load(inputStreamReader);
    }
    // check whether upgrading from <=v0.9
    if (!properties.containsKey(IOTDB_VERSION_STRING)) {
      logger.error(
          "DO NOT UPGRADE IoTDB from v0.9 or lower version to v0.12!"
              + " Please upgrade to v0.10 first");
      System.exit(-1);
    }
    // check whether upgrading from [v0.10, v.12]
    String versionString = properties.getProperty(IOTDB_VERSION_STRING);
    if (versionString.startsWith("0.10") || versionString.startsWith("0.11")) {
      logger.error("IoTDB version is too old, please upgrade to 0.12 firstly.");
      System.exit(-1);
    } else if (versionString.startsWith("0.12") || versionString.startsWith("0.13")) {
      checkWALNotExists();
      upgradePropertiesFile();
    }
    checkProperties();
  }

  private void checkWALNotExists() {
    for (String walDir : WAL_DIRS) {
      if (SystemFileFactory.INSTANCE.getFile(walDir).isDirectory()) {
        File[] sgWALs = SystemFileFactory.INSTANCE.getFile(walDir).listFiles();
        if (sgWALs != null) {
          for (File sgWAL : sgWALs) {
            // make sure wal directory of each sg is empty
            if (sgWAL.isDirectory() && Objects.requireNonNull(sgWAL.list()).length != 0) {
              logger.error(
                  "WAL detected, please stop insertion and run 'SET SYSTEM TO READONLY', then run 'flush' on IoTDB {} before upgrading to {}.",
                  properties.getProperty(IOTDB_VERSION_STRING),
                  IoTDBConstant.VERSION);
              System.exit(-1);
            }
          }
        }
      }
    }
  }

  /** upgrade 0.12 or 0.13 properties to 0.14 properties */
  private void upgradePropertiesFile() throws IOException {
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
              properties.setProperty(k, v);
            }
          });
      properties.setProperty(IOTDB_VERSION_STRING, IoTDBConstant.VERSION);
      // rename virtual_storage_group_num to data_region_num
      properties.setProperty(DATA_REGION_NUM, properties.getProperty(VIRTUAL_STORAGE_GROUP_NUM));
      properties.remove(VIRTUAL_STORAGE_GROUP_NUM);
      properties.store(tmpFOS, SYSTEM_PROPERTIES_STRING);

      // upgrade finished, delete old system.properties file
      if (propertiesFile.exists()) {
        Files.delete(propertiesFile.toPath());
      }
    }
    // rename system.properties.tmp to system.properties
    FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
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
              properties.setProperty(k, v);
            }
          });
      properties.setProperty(IOTDB_VERSION_STRING, IoTDBConstant.VERSION);
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
  private void checkProperties() throws ConfigurationException, IOException {
    for (Entry<String, String> entry : systemProperties.entrySet()) {
      if (!properties.containsKey(entry.getKey())) {
        upgradePropertiesFileFromBrokenFile();
        logger.info("repair system.properties, lack {}", entry.getKey());
      }
    }

    if (!properties.getProperty(TIMESTAMP_PRECISION_STRING).equals(timestampPrecision)) {
      throwException(TIMESTAMP_PRECISION_STRING, timestampPrecision);
    }

    if (!(properties.getProperty(TSFILE_FILE_SYSTEM_STRING).equals(tsfileFileSystem))) {
      throwException(TSFILE_FILE_SYSTEM_STRING, tsfileFileSystem);
    }

    if (!(properties.getProperty(TAG_ATTRIBUTE_SIZE_STRING).equals(tagAttributeTotalSize))) {
      throwException(TAG_ATTRIBUTE_SIZE_STRING, tagAttributeTotalSize);
    }

    if (!(properties.getProperty(TAG_ATTRIBUTE_FLUSH_INTERVAL).equals(tagAttributeFlushInterval))) {
      throwException(TAG_ATTRIBUTE_FLUSH_INTERVAL, tagAttributeFlushInterval);
    }

    if (!(properties.getProperty(MAX_DEGREE_OF_INDEX_STRING).equals(maxDegreeOfIndexNode))) {
      throwException(MAX_DEGREE_OF_INDEX_STRING, maxDegreeOfIndexNode);
    }

    if (!(properties.getProperty(DATA_REGION_NUM).equals(dataRegionNum))) {
      throwException(DATA_REGION_NUM, dataRegionNum);
    }

    if (!(properties.getProperty(TIME_ENCODER_KEY).equals(timeEncoderValue))) {
      throwException(TIME_ENCODER_KEY, timeEncoderValue);
    }

    if (!(properties.getProperty(ENABLE_ID_TABLE).equals(enableIDTable))) {
      throwException(ENABLE_ID_TABLE, enableIDTable);
    }

    if (!(properties.getProperty(ENABLE_ID_TABLE_LOG_FILE).equals(enableIdTableLogFile))) {
      throwException(ENABLE_ID_TABLE_LOG_FILE, enableIdTableLogFile);
    }

    if (!(properties.getProperty(SCHEMA_ENGINE_MODE).equals(schemaEngineMode))) {
      throwException(SCHEMA_ENGINE_MODE, schemaEngineMode);
    }

    // load configuration from system properties only when start as Data node
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
        parameter, String.valueOf(badValue), properties.getProperty(parameter));
  }

  // reload properties from system.properties
  private void reloadProperties() throws IOException {
    try (FileInputStream inputStream = new FileInputStream(propertiesFile);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET)) {
      properties.load(inputStreamReader);
    }
  }

  /** call this method to serialize DataNodeId */
  public void serializeDataNodeId(int dataNodeId) throws IOException {
    // create an empty tmpPropertiesFile
    if (tmpPropertiesFile.createNewFile()) {
      logger.info("Create system.properties.tmp {}.", tmpPropertiesFile);
    } else {
      logger.error("Create system.properties.tmp {} failed.", tmpPropertiesFile);
      System.exit(-1);
    }

    reloadProperties();

    try (FileOutputStream tmpFOS = new FileOutputStream(tmpPropertiesFile.toString())) {
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

  /** call this method to serialize consensus protocol */
  public void serializeConsensusProtocol(String regionConsensusProtocol, TConsensusGroupType type)
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
      if (type == TConsensusGroupType.DataRegion) {
        properties.setProperty(DATA_REGION_CONSENSUS_PROTOCOL, regionConsensusProtocol);
      } else if (type == TConsensusGroupType.SchemaRegion) {
        properties.setProperty(SCHEMA_REGION_CONSENSUS_PROTOCOL, regionConsensusProtocol);
      }
      properties.store(tmpFOS, SYSTEM_PROPERTIES_STRING);
      // serialize finished, delete old system.properties file
      if (propertiesFile.exists()) {
        Files.delete(propertiesFile.toPath());
      }
    }
    // rename system.properties.tmp to system.properties
    FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
  }

  public void serializeGlobalConfig(TGlobalConfig globalConfig) throws IOException {
    // create an empty tmpPropertiesFile
    if (tmpPropertiesFile.createNewFile()) {
      logger.info("Create system.properties.tmp {}.", tmpPropertiesFile);
    } else {
      logger.error("Create system.properties.tmp {} failed.", tmpPropertiesFile);
      System.exit(-1);
    }

    reloadProperties();

    try (FileOutputStream tmpFOS = new FileOutputStream(tmpPropertiesFile.toString())) {

      if (!checkConsensusProtocolExists(TConsensusGroupType.DataRegion)) {
        properties.setProperty(
            DATA_REGION_CONSENSUS_PROTOCOL, globalConfig.getDataRegionConsensusProtocolClass());
      }
      if (!checkConsensusProtocolExists(TConsensusGroupType.SchemaRegion)) {
        properties.setProperty(
            SCHEMA_REGION_CONSENSUS_PROTOCOL, globalConfig.getSchemaRegionConsensusProtocolClass());
      }
      properties.store(tmpFOS, SYSTEM_PROPERTIES_STRING);
      // serialize finished, delete old system.properties file
      if (propertiesFile.exists()) {
        Files.delete(propertiesFile.toPath());
      }
    }
    // rename system.properties.tmp to system.properties
    FileUtils.moveFile(tmpPropertiesFile, propertiesFile);
  }

  public void serializeNewDataNode(TDataNodeLocation dataNodeLocation) throws IOException {
    reloadProperties();

    try (FileOutputStream fileOutputStream = new FileOutputStream(propertiesFile)) {
      properties.setProperty(INTERNAL_ADDRESS, dataNodeLocation.getInternalEndPoint().getIp());
      properties.setProperty(
          INTERNAL_PORT, String.valueOf(dataNodeLocation.getInternalEndPoint().getPort()));
      properties.setProperty(
          RPC_ADDRESS, String.valueOf(dataNodeLocation.getClientRpcEndPoint().getIp()));
      properties.setProperty(
          RPC_PORT, String.valueOf(dataNodeLocation.getClientRpcEndPoint().getPort()));
      properties.setProperty(
          MPP_DATA_EXCHANGE_PORT,
          String.valueOf(dataNodeLocation.getMPPDataExchangeEndPoint().getPort()));
      properties.setProperty(
          SCHEMA_REGION_CONSENSUS_PORT,
          String.valueOf(dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort()));
      properties.setProperty(
          DATA_REGION_CONSENSUS_PORT,
          String.valueOf(dataNodeLocation.getDataRegionConsensusEndPoint().getPort()));
      properties.store(fileOutputStream, SYSTEM_PROPERTIES_STRING);
    }
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

  public boolean isIpPortUpdated() {
    boolean isUpdated = false;
    // check the modifiable parts of configuration
    if (!(properties.getProperty(INTERNAL_PORT).equals(internalPort))) {
      isUpdated = true;
      logger.info(
          "Internal port is updated from {} to {}",
          properties.getProperty(INTERNAL_PORT),
          internalPort);
    }
    if (!(properties.getProperty(RPC_ADDRESS).equals(rpcAddress))) {
      isUpdated = true;
      logger.info(
          "RPC address is updated from {} to {}", properties.getProperty(RPC_ADDRESS), rpcAddress);
    }
    if (!(properties.getProperty(RPC_PORT).equals(rpcPort))) {
      isUpdated = true;
      logger.info("RPC port is updated from {} to {}", properties.getProperty(RPC_PORT), rpcPort);
    }
    if (!(properties.getProperty(MPP_DATA_EXCHANGE_PORT).equals(mppDataExchangePort))) {
      isUpdated = true;
      logger.info(
          "MPP data exchange port is updated from {} to {}",
          properties.getProperty(MPP_DATA_EXCHANGE_PORT),
          mppDataExchangePort);
    }
    return isUpdated;
  }

  public boolean checkNonModifiableConfiguration() {
    // check the non-modifiable parts of configuration
    if (!(properties.getProperty(INTERNAL_ADDRESS).equals(internalAddress))) {
      logger.error("Internal address is not allowed to be updated");
      return true;
    }
    if (!(properties.getProperty(SCHEMA_REGION_CONSENSUS_PORT).equals(schemaRegionConsensusPort))) {
      logger.error("Schema region consensus port is not allowed to be updated");
      return true;
    }
    if (!(properties.getProperty(DATA_REGION_CONSENSUS_PORT).equals(dataRegionConsensusPort))) {
      logger.error("Data region consensus port is not allowed to be updated");
      return true;
    }
    return false;
  }
}
