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
package com.timecho.iotdb;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;

import com.timecho.iotdb.auth.StrictAccessControlImpl;
import com.timecho.iotdb.auth.StrictTreeAccessCheckVisitor;
import com.timecho.iotdb.commons.secret.SecretKey;
import com.timecho.iotdb.commons.utils.OSUtils;
import com.timecho.iotdb.dataregion.migration.MigrationTaskManager;
import com.timecho.iotdb.db.protocol.session.TimechoSessionManager;
import com.timecho.iotdb.rpc.IPFilter;
import com.timecho.iotdb.schemaregion.EnterpriseSchemaConstant;
import com.timecho.iotdb.schemaregion.mtree.EnterpriseCachedMNodeFactory;
import com.timecho.iotdb.schemaregion.mtree.EnterpriseMemMNodeFactory;
import com.timecho.iotdb.service.ClientRPCServiceImplNew;
import com.timecho.iotdb.service.DataNodeInternalRPCServiceNew;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class DataNode extends org.apache.iotdb.db.service.DataNode {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  public static void main(String[] args) {
    startUp(args, DataNode::new);
  }

  public static void prepare() {
    SessionManager.sessionManagerSupplier = TimechoSessionManager::new;

    // set up environment for schema region
    MNodeFactoryLoader.getInstance().addNodeFactory(EnterpriseMemMNodeFactory.class);
    MNodeFactoryLoader.getInstance().addNodeFactory(EnterpriseCachedMNodeFactory.class);
    MNodeFactoryLoader.getInstance().setEnv(EnterpriseSchemaConstant.ENTERPRISE_MNODE_FACTORY_ENV);

    // just to init the class
    //noinspection ResultOfMethodCallIgnored
    IPFilter.getAllowListPatterns();
  }

  protected static void startUp(String[] args, Supplier<DataNode> dataNodeSupplier) {
    prepare();
    // set up environment for object storage
    TSFileDescriptor.getInstance()
        .getConfig()
        .setObjectStorageFile("com.timecho.iotdb.os.fileSystem.OSFile");
    TSFileDescriptor.getInstance()
        .getConfig()
        .setObjectStorageTsFileInput("com.timecho.iotdb.os.fileSystem.OSTsFileInput");
    TSFileDescriptor.getInstance()
        .getConfig()
        .setObjectStorageTsFileOutput("com.timecho.iotdb.os.fileSystem.OSTsFileOutput");

    logger.info(
        TimechoServerMessages.IOTDB_DATANODE_ENVIRONMENT_VARIABLES,
        IoTDBConfig.getEnvironmentVariables());
    logger.info(
        TimechoServerMessages.IOTDB_DATANODE_DEFAULT_CHARSET,
        Charset.defaultCharset().displayName());
    DataNode dataNode = dataNodeSupplier.get();
    int returnCode = dataNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  @Override
  protected void registerInternalRPCService() throws StartupException {
    // Start InternalRPCService to indicate that the current DataNode can accept cluster scheduling
    DataNodeInternalRPCServiceNew instance = DataNodeInternalRPCServiceNew.getInstance();
    instance.setDataNodeContext(context);
    registerManager.register(instance);
    registerManager.register(MigrationTaskManager.getInstance());
  }

  @Override
  protected String getClientRPCServiceImplClassName() {
    return ClientRPCServiceImplNew.class.getName();
  }

  @Override
  protected void versionCheck(TSystemConfigurationResp configurationResp) throws StartupException {
    if (!configurationResp.globalConfig.isEnterprise) {
      final String message =
          "TimechoDB DataNode can only be used with TimechoDB ConfigNode and cannot be used with IoTDB ConfigNode.";
      logger.error(message);
      throw new StartupException(message);
    }
  }

  @Override
  protected void storeRuntimeConfigurations(
      List<TConfigNodeLocation> configNodeLocations, TRuntimeConfiguration runtimeConfiguration)
      throws StartupException {
    super.storeRuntimeConfigurations(configNodeLocations, runtimeConfiguration);
    if (runtimeConfiguration.isSetEnableSeparationOfAdminPowers()
        && runtimeConfiguration.isEnableSeparationOfAdminPowers()) {
      AuthorityChecker.setAccessControl(
          new StrictAccessControlImpl(
              new ITableAuthCheckerImpl(), new StrictTreeAccessCheckVisitor()));
    }
  }

  @Override
  protected void saveSecretKey() {
    SecretKey.getInstance()
        .initSecretKeyFile(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            String.valueOf(UUID.randomUUID()));
  }

  @Override
  protected void saveHardwareCode() {
    try {
      String hardwareCode = OSUtils.generateSystemInfoContentWithVersion();
      SecretKey.getInstance()
          .initHardwareCodeFile(
              IoTDBDescriptor.getInstance().getConfig().getSystemDir(), hardwareCode);
    } catch (Exception e) {
      logger.error(TimechoServerMessages.HARDWARE_GENERATION_FAILED);
    }
  }

  @Override
  protected void loadSecretKey() throws IOException {
    SecretKey.getInstance()
        .loadSecretKeyFromFile(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
  }

  @Override
  protected void loadHardwareCode() throws IOException {
    SecretKey.getInstance()
        .loadHardwareCodeFromFile(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
  }

  @Override
  protected void initEncryptProps() {
    SecretKey.getInstance()
        .initEncryptProps(CommonDescriptor.getInstance().getConfig().getFileEncryptType());
  }

  // Scan all files in the scope if exist the encrypted files.
  @Override
  protected void initSecretKey() throws IOException {
    URL configFileUrl =
        IoTDBDescriptor.getPropsUrl(
            SecretKey.DN_FILE_ENCRYPTED_PREFIX
                + CommonConfig.SYSTEM_CONFIG_NAME
                + SecretKey.FILE_ENCRYPTED_SUFFIX);
    if ((configFileUrl == null || !(new File(configFileUrl.getPath()).exists()))
        && !WALManager.existsEncryptedFile()) {
      saveSecretKey();
      saveHardwareCode();
    } else {
      throw new IOException(
          "Can't start DataNode, because encrypted file is found, no way to resolve that secret key file is lost.");
    }
  }

  @Override
  protected void encryptConfigFile() {
    // Encrypt config file first time
    if (CommonDescriptor.getInstance().getConfig().isEnableEncryptConfigFile()) {
      URL systemConfigUrl = IoTDBDescriptor.getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
      if (systemConfigUrl != null) {
        File systemConfigFile = new File(systemConfigUrl.getPath());
        if (systemConfigFile.exists()) {
          ConfigurationFileUtils.encryptConfigFile(
              systemConfigUrl,
              systemConfigFile.getParentFile().getPath()
                  + File.separator
                  + SecretKey.DN_FILE_ENCRYPTED_PREFIX
                  + CommonConfig.SYSTEM_CONFIG_NAME
                  + SecretKey.FILE_ENCRYPTED_SUFFIX);
        }
      }
    }
  }

  private static class DataNodeNewHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeNewHolder() {}
  }

  public static DataNode getInstance() {
    return DataNode.DataNodeNewHolder.INSTANCE;
  }
}
