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

package org.apache.iotdb.procedure.conf;

import org.apache.iotdb.commons.exception.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class ProcedureNodeConfigDescriptor {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureNodeConfigDescriptor.class);

  private final ProcedureNodeConfig conf = new ProcedureNodeConfig();

  private ProcedureNodeConfigDescriptor() {
    loadProps();
  }

  public ProcedureNodeConfig getConf() {
    return conf;
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public URL getPropsUrl() {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(ProcedureNodeConstant.PROCEDURE_CONF_DIR, null);
    // If it wasn't, check if a home directory was provided
    if (urlString == null) {
      urlString = System.getProperty(ProcedureNodeConstant.PROCEDURENODE_HOME, null);
      if (urlString != null) {
        urlString =
            urlString
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + ProcedureNodeConstant.CONF_NAME;
      } else {
        // When start ProcedureNode with the script, the environment variables ProcedureNode_CONF
        // and ProcedureNode_HOME will be set. But we didn't set these two in developer mode.
        // Thus, just return null and use default Configuration in developer mode.
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + ProcedureNodeConstant.CONF_NAME);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      return null;
    }
  }

  private void loadProps() {
    URL url = getPropsUrl();
    if (url == null) {
      LOG.warn(
          "Couldn't load the ProcedureNode configuration from any of the known sources. Use default configuration.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {

      LOG.info("start reading ProcedureNode conf file: {}", url);

      Properties properties = new Properties();
      properties.load(inputStream);

      conf.setRpcAddress(
          properties.getProperty("procedure_node_address", String.valueOf(conf.getRpcAddress())));

      conf.setRpcPort(
          Integer.parseInt(
              properties.getProperty("config_node_rpc_port", String.valueOf(conf.getRpcPort()))));

      conf.setConfignodePort(
          Integer.parseInt(
              properties.getProperty(
                  "config_node_port", String.valueOf(conf.getConfignodePort()))));

      conf.setDatanodePort(
          Integer.parseInt(
              properties.getProperty("date_node_port", String.valueOf(conf.getDatanodePort()))));

      conf.setRpcAdvancedCompressionEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_advanced_compression_enable",
                  String.valueOf(conf.isRpcAdvancedCompressionEnable()))));

      conf.setRpcThriftCompressionEnabled(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_thrift_compression_enable",
                  String.valueOf(conf.isRpcThriftCompressionEnabled()))));

      conf.setRpcMaxConcurrentClientNum(
          Integer.parseInt(
              properties.getProperty(
                  "rpc_max_concurrent_client_num",
                  String.valueOf(conf.getRpcMaxConcurrentClientNum()))));

      conf.setThriftDefaultBufferSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))));

      conf.setThriftMaxFrameSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

      conf.setProcedureWalDir(properties.getProperty("proc_wal_dir", conf.getProcedureWalDir()));

      conf.setCompletedEvictTTL(
          Integer.parseInt(
              properties.getProperty(
                  "completed_evict_ttl", String.valueOf(conf.getCompletedEvictTTL()))));

      conf.setCompletedCleanInterval(
          Integer.parseInt(
              properties.getProperty(
                  "completed_clean_interval", String.valueOf(conf.getCompletedCleanInterval()))));

      conf.setWorkerThreadsCoreSize(
          Integer.parseInt(
              properties.getProperty(
                  "workerthreads_core_size", String.valueOf(conf.getWorkerThreadsCoreSize()))));
    } catch (IOException e) {
      LOG.warn("Couldn't load ProcedureNode conf file, use default config", e);
    } finally {
      updatePath();
    }
  }

  private void updatePath() {
    formulateFolders();
  }

  private void formulateFolders() {
    conf.setProcedureWalDir(addHomeDir(conf.getProcedureWalDir()));
  }

  private String addHomeDir(String dir) {
    String homeDir = System.getProperty(ProcedureNodeConstant.PROCEDURENODE_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
  }

  public static ProcedureNodeConfigDescriptor getInstance() {
    return ProcedureNodeDescriptorHolder.INSTANCE;
  }

  public void checkConfig() throws StartupException {
    File walDir = new File(conf.getProcedureWalDir());
    if (!walDir.exists()) {
      if (walDir.mkdirs()) {
        LOG.info("Make  procedure  wall  dirs:{}", walDir);
      } else {
        throw new StartupException(
            String.format(
                "Start procedure node  failed,  because can  not  make  wal dirs:%s.",
                walDir.getAbsolutePath()));
      }
    }
  }

  private static class ProcedureNodeDescriptorHolder {

    private static final ProcedureNodeConfigDescriptor INSTANCE =
        new ProcedureNodeConfigDescriptor();

    private ProcedureNodeDescriptorHolder() {
      // empty constructor
    }
  }
}
