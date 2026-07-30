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
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.rescon.disk.DirectoryChecker;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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

  private Properties properties = new Properties();

  private final Map<String, Supplier<String>> systemProperties = new HashMap<>();
  private final SystemPropertiesHandler systemPropertiesHandler;

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
  private static final String ENCRYPT_MAGIC_STRING = "encrypt_magic_string";
  private static final String ENCRYPT_SALT = "encrypt_salt";
  private static final String ENCRYPT_TOKEN_HINT = "encrypt_token_hint";
  private static final String magicString = "thisisusedfortsfileencrypt";

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
  private static final String CLUSTER_ID = "cluster_id";
  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL = "schema_region_consensus_protocol";
  private static final String DATA_REGION_CONSENSUS_PROTOCOL = "data_region_consensus_protocol";
  // endregion
  // region params of old versions
  private static final String VIRTUAL_STORAGE_GROUP_NUM = "virtual_storage_group_num";

  // endregion

  public static IoTDBStartCheck getInstance() {
    return IoTDBConfigCheckHolder.INSTANCE;
  }

  public static void reinitializeStatics() {
    IoTDBConfigCheckHolder.INSTANCE = new IoTDBStartCheck();
  }

  private static class IoTDBConfigCheckHolder {

    private static IoTDBStartCheck INSTANCE = new IoTDBStartCheck();
  }

  private String getVal(String paramName) {
    if (variableParamValueTable.containsKey(paramName)) {
      return variableParamValueTable.get(paramName).get();
    } else {
      return null;
    }
  }

  private IoTDBStartCheck() {
    logger.info(DataNodeMiscMessages.STARTING_IOTDB, IoTDBConstant.VERSION_WITH_BUILD);

    // check whether SCHEMA_DIR exists, create if not exists
    File dir = SystemFileFactory.INSTANCE.getFile(SCHEMA_DIR);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        logger.error(DataNodeMiscMessages.CANNOT_CREATE_SCHEMA_DIR, SCHEMA_DIR);
        System.exit(-1);
      } else {
        logger.info(DataNodeMiscMessages.SCHEMA_DIR_CREATED, SCHEMA_DIR);
      }
    }

    systemPropertiesHandler = DataNodeSystemPropertiesHandler.getInstance();

    systemProperties.put(IOTDB_VERSION_STRING, () -> IoTDBConstant.VERSION);
    systemProperties.put(COMMIT_ID_STRING, () -> IoTDBConstant.BUILD_INFO);
    for (String param : variableParamValueTable.keySet()) {
      systemProperties.put(param, () -> getVal(param));
    }
  }

  /**
   * check and create directory before start IoTDB.
   *
   * <p>(1) try to create directory, avoid the inability to create directory at runtime due to lack
   * of permissions. (2) try to check if the directory is occupied, avoid multiple IoTDB processes
   * accessing same director.
   */
  public void checkDirectory() throws ConfigurationException, IOException {
    for (String dataDir : config.getLocalDataDirs()) {
      DirectoryChecker.getInstance().registerDirectory(new File(dataDir));
    }
    if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      if (DirectoryChecker.getInstance().isCrossDisk(config.getDataDirs())) {
        throw new ConfigurationException(
            DataNodeMiscMessages
                .MISC_EXCEPTION_CONFIGURING_THE_DATA_DIRECTORIES_AS_CROSS_DISK_DIRECTORIES_FC0A3875);
      }
    }
    // check system dir
    DirectoryChecker.getInstance().registerDirectory(new File(config.getSystemDir()));
    // check WAL dir
    if (!(config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS))
        && !config.getWalMode().equals(WALMode.DISABLE)) {
      for (String walDir : commonConfig.getWalDirs()) {
        DirectoryChecker.getInstance().registerDirectory(new File(walDir));
      }
    }
    // check consensus dir
    DirectoryChecker.getInstance().registerDirectory(new File(config.getConsensusDir()));
  }

  /**
   * The location of system.properties has been adjusted from SHCEMA_DIR to the system directory.
   * During a restart, it is necessary to check if the file exists in the old location. If it does,
   * move the file to the new location.
   *
   * @throws IOException If copy fail or delete fail
   */
  public static void checkOldSystemConfig() throws IOException {
    File oldPropertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBStartCheck.SCHEMA_DIR + File.separator + PROPERTIES_FILE_NAME);
    if (oldPropertiesFile.exists()) {
      File correctPropertiesFile =
          SystemFileFactory.INSTANCE.getFile(
              config.getSystemDir() + File.separator + PROPERTIES_FILE_NAME);
      FileUtils.copyFile(oldPropertiesFile, correctPropertiesFile);
      FileUtils.delete(oldPropertiesFile);
      logger.info(
          DataNodeMiscMessages.MISC_LOG_SYSTEM_PROPERTIES_FILE_HAS_BEEN_MOVED_SUCCESSFULLY_4445A448,
          oldPropertiesFile.getAbsolutePath(),
          correctPropertiesFile.getAbsolutePath());
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
    properties = systemPropertiesHandler.read();

    if (systemPropertiesHandler.isFirstStart()) {
      if ((config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
              || (config
                      .getDataRegionConsensusProtocolClass()
                      .equals(ConsensusFactory.IOT_CONSENSUS_V2)
                  && config
                      .getIotConsensusV2Mode()
                      .equals(ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE)))
          && config.getWalMode().equals(WALMode.DISABLE)) {
        throw new ConfigurationException(
            DataNodeMiscMessages
                .MISC_EXCEPTION_CONFIGURING_THE_WALMODE_AS_DISABLE_IS_NOT_SUPPORTED_UNDER_49298819);
      }
    } else {
      // check whether upgrading from <=v0.9
      if (!properties.containsKey(IOTDB_VERSION_STRING)) {
        logger.error(
            DataNodeMiscMessages
                .MISC_LOG_DO_NOT_UPGRADE_IOTDB_FROM_V0_9_OR_LOWER_VERSION_TO_V1_0_9878EC88);
        System.exit(-1);
      }
      String versionString = properties.getProperty(IOTDB_VERSION_STRING);
      if (versionString.startsWith("0.")) {
        logger.error(DataNodeMiscMessages.IOTDB_VERSION_TOO_OLD);
        System.exit(-1);
      }
      checkImmutableSystemProperties();
    }
  }

  /** repair broken properties */
  private void upgradePropertiesFileFromBrokenFile() throws IOException {
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

  /** Check all immutable properties */
  private void checkImmutableSystemProperties() throws IOException {
    for (Entry<String, Supplier<String>> entry : systemProperties.entrySet()) {
      if (!properties.containsKey(entry.getKey())) {
        upgradePropertiesFileFromBrokenFile();
        logger.info(DataNodeMiscMessages.REPAIR_SYSTEM_PROPERTIES, entry.getKey());
      }
    }

    // load configuration from system properties only when start as Data node
    if (properties.containsKey(DATA_NODE_ID)) {
      config.setDataNodeId(Integer.parseInt(properties.getProperty(DATA_NODE_ID)));
    }
    if (properties.containsKey(CLUSTER_ID)) {
      config.setClusterId(properties.getProperty(CLUSTER_ID));
    }
    // Only the data region protocol could have been persisted as the old PipeConsensus name
    // during a jar-only upgrade, so only that field needs compatibility normalization.
    boolean needRewriteConsensusProtocol = false;
    if (properties.containsKey(SCHEMA_REGION_CONSENSUS_PROTOCOL)) {
      config.setSchemaRegionConsensusProtocolClass(
          properties.getProperty(SCHEMA_REGION_CONSENSUS_PROTOCOL));
    }
    if (properties.containsKey(DATA_REGION_CONSENSUS_PROTOCOL)) {
      final String persistedDataRegionConsensusProtocolClass =
          properties.getProperty(DATA_REGION_CONSENSUS_PROTOCOL);
      final String dataRegionConsensusProtocolClass =
          ConsensusFactory.normalizeConsensusProtocolClass(
              persistedDataRegionConsensusProtocolClass);
      if (!Objects.equals(
          persistedDataRegionConsensusProtocolClass, dataRegionConsensusProtocolClass)) {
        properties.setProperty(DATA_REGION_CONSENSUS_PROTOCOL, dataRegionConsensusProtocolClass);
        needRewriteConsensusProtocol = true;
        logger.warn(
            DataNodeMiscMessages
                .MISC_LOG_SYSTEMPROPERTIES_NORMALIZE_FROM_TO_FOR_COMPATIBILITY_BE1C725F,
            DATA_REGION_CONSENSUS_PROTOCOL,
            persistedDataRegionConsensusProtocolClass,
            dataRegionConsensusProtocolClass);
      }
      config.setDataRegionConsensusProtocolClass(dataRegionConsensusProtocolClass);
    }
    if (needRewriteConsensusProtocol) {
      systemPropertiesHandler.overwrite(properties);
    }
  }

  private void throwException(String parameter, Object badValue) throws ConfigurationException {
    throw new ConfigurationException(
        parameter,
        String.valueOf(badValue),
        properties.getProperty(parameter),
        String.format(
            DataNodeMiscMessages.PARAMETER_CANNOT_BE_MODIFIED_AFTER_FIRST_STARTUP_FMT, parameter));
  }

  public void serializeDataNodeId(int dataNodeId) throws IOException {
    systemPropertiesHandler.put(DATA_NODE_ID, String.valueOf(dataNodeId));
  }

  public void serializeClusterID(String clusterId) throws IOException {
    systemPropertiesHandler.put(CLUSTER_ID, clusterId);
  }

  public void serializeEncryptMagicString() throws IOException {
    if (!Objects.equals(TSFileDescriptor.getInstance().getConfig().getEncryptType(), "UNENCRYPTED")
        && !Objects.equals(
            TSFileDescriptor.getInstance().getConfig().getEncryptType(),
            "org.apache.tsfile.encrypt.UNENCRYPTED")) {
      String token = System.getenv("user_encrypt_token");
      if (token == null || token.trim().isEmpty()) {
        throw new EncryptException(
            DataNodeMiscMessages
                .MISC_EXCEPTION_ENCRYPTTYPE_IS_NOT_UNENCRYPTED_BUT_USER_ENCRYPT_TOKEN_IS_F828C20B);
      }
      String tokenHint = System.getenv("user_encrypt_token_hint");
      if (tokenHint != null && !tokenHint.trim().isEmpty()) {
        // If user_encrypt_token_hint is set, it should follow some rules.
        // For example, it could not include user_encrypt_token.
        if (tokenHint.toLowerCase().contains(token.toLowerCase())) {
          throw new EncryptException(
              DataNodeMiscMessages
                  .MISC_EXCEPTION_USER_ENCRYPT_TOKEN_HINT_SHOULD_NOT_INCLUDE_USER_ENCRYPT_50531D40);
        }
        if (tokenHint
            .toLowerCase()
            .contains(new StringBuilder(token.toLowerCase()).reverse().toString())) {
          throw new EncryptException(
              DataNodeMiscMessages
                  .MISC_EXCEPTION_USER_ENCRYPT_TOKEN_HINT_SHOULD_NOT_INCLUDE_THE_REVERSE_OF_39B2D35C);
        }
      }
    }
    String encryptMagicString =
        EncryptUtils.byteArrayToHexString(
            EncryptUtils.getEncrypt().getEncryptor().encrypt(magicString.getBytes()));
    systemProperties.put(ENCRYPT_MAGIC_STRING, () -> encryptMagicString);
    String encryptSalt =
        EncryptUtils.byteArrayToHexString(
            TSFileDescriptor.getInstance().getConfig().getEncryptSalt());
    systemProperties.put(ENCRYPT_SALT, () -> encryptSalt);
    String encryptTokenHint = CommonDescriptor.getInstance().getConfig().getUserEncryptTokenHint();
    systemProperties.put(ENCRYPT_TOKEN_HINT, () -> encryptTokenHint);
    generateOrOverwriteSystemPropertiesFile();
  }

  public boolean checkConsensusProtocolExists(TConsensusGroupType type) {
    if (type == TConsensusGroupType.DataRegion) {
      return properties.containsKey(DATA_REGION_CONSENSUS_PROTOCOL);
    } else if (type == TConsensusGroupType.SchemaRegion) {
      return properties.containsKey(SCHEMA_REGION_CONSENSUS_PROTOCOL);
    }

    logger.error(DataNodeMiscMessages.UNEXPECTED_CONSENSUS_GROUP_TYPE);
    return false;
  }

  public void serializeMutableSystemPropertiesIfNecessary() throws IOException {
    long startTime = System.currentTimeMillis();
    boolean needsSerialize = false;
    for (String param : variableParamValueTable.keySet()) {
      if (!properties.getProperty(param).equals(getVal(param))) {
        needsSerialize = true;
      }
    }

    if (needsSerialize) {
      generateOrOverwriteSystemPropertiesFile();
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        DataNodeMiscMessages
            .MISC_LOG_SERIALIZE_MUTABLE_SYSTEM_PROPERTIES_SUCCESSFULLY_WHICH_TAKES_4656A206,
        (endTime - startTime));
  }

  public void generateOrOverwriteSystemPropertiesFile() throws IOException {
    systemProperties.forEach((k, v) -> properties.setProperty(k, v.get()));
    systemPropertiesHandler.overwrite(properties);
  }

  public void checkEncryptMagicString() throws IOException, ConfigurationException {
    if (!Objects.equals(TSFileDescriptor.getInstance().getConfig().getEncryptType(), "UNENCRYPTED")
        && !Objects.equals(
            TSFileDescriptor.getInstance().getConfig().getEncryptType(),
            "org.apache.tsfile.encrypt.UNENCRYPTED")) {
      properties = systemPropertiesHandler.read();
      CommonDescriptor.getInstance()
          .getConfig()
          .setUserEncryptTokenHint(properties.getProperty(ENCRYPT_TOKEN_HINT));
      String encryptSalt = properties.getProperty(ENCRYPT_SALT);
      byte[] saltBytes = EncryptUtils.hexStringToByteArray(encryptSalt);
      TSFileDescriptor.getInstance().getConfig().setEncryptSalt(saltBytes);

      String token = System.getenv("user_encrypt_token");
      if (token == null || token.trim().isEmpty()) {
        throw new EncryptException(
            String.format(
                DataNodeMiscMessages
                    .MISC_EXCEPTION_RESTART_SYSTEM_AFTER_NOT_STORING_KEY_BUT_USER_ENCRYPT_TOKEN_61CCF9A2,
                CommonDescriptor.getInstance().getConfig().getUserEncryptTokenHint()));
      }
      TSFileDescriptor.getInstance().getConfig().setEncryptKeyFromToken(token);
      String encryptMagicString = properties.getProperty(ENCRYPT_MAGIC_STRING);
      byte[] magicStringBytes = EncryptUtils.hexStringToByteArray(encryptMagicString);
      String decryptedMagicString =
          new String(
              EncryptUtils.getEncrypt().getDecryptor().decrypt(magicStringBytes),
              TSFileConfig.STRING_CHARSET);
      if (!Objects.equals(decryptedMagicString, magicString)) {
        logger.error(DataNodeMiscMessages.ENCRYPT_MAGIC_STRING_NOT_MATCHED);
        throw new ConfigurationException(
            String.format(
                DataNodeMiscMessages
                    .MISC_EXCEPTION_CHANGING_ENCRYPT_TYPE_OR_KEY_FOR_TSFILE_ENCRYPTION_AFTER_0668F74E,
                CommonDescriptor.getInstance().getConfig().getUserEncryptTokenHint()));
      }
    }
  }

  public Properties getProperties() {
    return properties;
  }
}
