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

import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class CommonDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonDescriptor.class);

  private final CommonConfig config = new CommonConfig();

  private CommonDescriptor() {}

  public static CommonDescriptor getInstance() {
    return CommonDescriptorHolder.INSTANCE;
  }

  private static class CommonDescriptorHolder {

    private static final CommonDescriptor INSTANCE = new CommonDescriptor();

    private CommonDescriptorHolder() {
      // empty constructor
    }
  }

  public CommonConfig getConfig() {
    return config;
  }

  public void initCommonConfigDir(String systemDir) {
    config.setUserFolder(systemDir + File.separator + "users");
    config.setRoleFolder(systemDir + File.separator + "roles");
    config.setProcedureWalFolder(systemDir + File.separator + "procedure");
  }

  public void loadCommonProps(Properties properties) {
    config.setAuthorizerProvider(
        properties.getProperty("authorizer_provider_class", config.getAuthorizerProvider()).trim());
    // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
    config.setOpenIdProviderUrl(
        properties.getProperty("openID_url", config.getOpenIdProviderUrl()).trim());
    config.setAdminName(properties.getProperty("admin_name", config.getAdminName()).trim());

    config.setAdminPassword(
        properties.getProperty("admin_password", config.getAdminPassword()).trim());
    config.setEncryptDecryptProvider(
        properties
            .getProperty(
                "iotdb_server_encrypt_decrypt_provider", config.getEncryptDecryptProvider())
            .trim());

    config.setEncryptDecryptProviderParameter(
        properties.getProperty(
            "iotdb_server_encrypt_decrypt_provider_parameter",
            config.getEncryptDecryptProviderParameter()));

    config.setDefaultTTLInMs(
        Long.parseLong(
            properties
                .getProperty("default_ttl_in_ms", String.valueOf(config.getDefaultTTLInMs()))
                .trim()));
    config.setSyncDir(properties.getProperty("dn_sync_dir", config.getSyncDir()).trim());

    config.setWalDirs(
        properties.getProperty("dn_wal_dirs", config.getWalDirs()[0]).trim().split(","));

    config.setCnRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "cn_rpc_thrift_compression_enable",
                    String.valueOf(config.isCnRpcThriftCompressionEnabled()))
                .trim()));

    config.setCnConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_connection_timeout_ms", String.valueOf(config.getCnConnectionTimeoutInMS()))
                .trim()));

    config.setCnSelectorNumOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_selector_thread_nums_of_client_manager",
                    String.valueOf(config.getCnSelectorNumOfClientManager()))
                .trim()));

    config.setHandleSystemErrorStrategy(
        HandleSystemErrorStrategy.valueOf(
            properties
                .getProperty(
                    "handle_system_error", String.valueOf(config.getHandleSystemErrorStrategy()))
                .trim()));

    config.setDiskSpaceWarningThreshold(
        Double.parseDouble(
            properties
                .getProperty(
                    "disk_space_warning_threshold",
                    String.valueOf(config.getDiskSpaceWarningThreshold()))
                .trim()));
  }

  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    config.setDiskSpaceWarningThreshold(globalConfig.getDiskSpaceWarningThreshold());
  }
}
