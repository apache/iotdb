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

package org.apache.iotdb.cluster.config;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.cluster.exception.BadSeedUrlFormatException;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterDescriptor.class);
  private static final ClusterDescriptor INSTANCE = new ClusterDescriptor();

  private static final String OPTION_INTERVAL_META_PORT = "internal_meta_port";
  private static final String OPTION_INTERVAL_DATA_PORT = "internal_data_port";
  private static final String OPTION_CLUSTER_RPC_PORT = "cluster_rpc_port";
  private static final String OPTION_SEED_NODES = "seed_nodes";


  private ClusterConfig config = new ClusterConfig();
  private static CommandLine commandLine;

  private ClusterDescriptor() {
    loadProps();
  }

  public ClusterConfig getConfig() {
    return config;
  }

  public static ClusterDescriptor getInstance() {
    return INSTANCE;
  }

  public String getPropsUrl() {
    String url = System.getProperty(ClusterConstant.CLUSTER_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + ClusterConfig.CONFIG_NAME;
      } else {
        logger.warn(
            "Cannot find IOTDB_HOME or CLUSTER_CONF environment variable when loading "
                + "config file {}, use default configuration",
            ClusterConfig.CONFIG_NAME);
        // update all data seriesPath
        return null;
      }
    } else {
      url += (File.separatorChar + ClusterConfig.CONFIG_NAME);
    }
    return url;
  }

  public void replaceProps(String[] params) {
    Options options = new Options();

    Option metaPort = new Option(OPTION_INTERVAL_META_PORT, OPTION_INTERVAL_META_PORT, true,
        "port for metadata service");
    metaPort.setRequired(false);
    options.addOption(metaPort);

    Option dataPort = new Option(OPTION_INTERVAL_DATA_PORT, OPTION_INTERVAL_DATA_PORT, true,
        "port for data service");
    dataPort.setRequired(false);
    options.addOption(dataPort);

    Option clusterRpcPort = new Option(OPTION_CLUSTER_RPC_PORT, OPTION_CLUSTER_RPC_PORT, true,
        "port for client service");
    clusterRpcPort.setRequired(false);
    options.addOption(clusterRpcPort);

    Option seedNodes = new Option(OPTION_SEED_NODES, OPTION_SEED_NODES, true,
        "comma-separated {IP/DOMAIN}:meta_port:data_port:client_port pairs");
    seedNodes.setRequired(false);
    options.addOption(seedNodes);

    boolean ok = parseCommandLine(options, params);
    if (!ok) {
      logger.error("replaces properties failed, use default conf params");
    } else {
      if (commandLine.hasOption(OPTION_INTERVAL_META_PORT)) {
        config.setInternalMetaPort(Integer.parseInt(commandLine.getOptionValue(
            OPTION_INTERVAL_META_PORT)));
        logger.debug("replace local meta port with={}", config.getInternalMetaPort());
      }

      if (commandLine.hasOption(OPTION_INTERVAL_DATA_PORT)) {
        config.setInternalDataPort(Integer.parseInt(commandLine.getOptionValue(
            OPTION_INTERVAL_DATA_PORT)));
        logger.debug("replace local data port with={}", config.getInternalDataPort());
      }

      if (commandLine.hasOption(OPTION_CLUSTER_RPC_PORT)) {
        config.setClusterRpcPort(Integer.parseInt(commandLine.getOptionValue(
            OPTION_CLUSTER_RPC_PORT)));
        logger.debug("replace local cluster rpc port with={}", config.getClusterRpcPort());
      }

      if (commandLine.hasOption(OPTION_SEED_NODES)) {
        String seedNodeUrls = commandLine.getOptionValue(OPTION_SEED_NODES);
        config.setSeedNodeUrls(getSeedUrlList(seedNodeUrls));
        logger.debug("replace seed nodes with={}", config.getSeedNodeUrls());
      }
    }
  }

  public void replaceHostnameWithIp() throws UnknownHostException, BadSeedUrlFormatException {
    boolean isInvalidClusterRpcIp = InetAddresses.isInetAddress(config.getClusterRpcIp());
    if (!isInvalidClusterRpcIp) {
      String clusterRpcIp = hostnameToIP(config.getClusterRpcIp());
      config.setClusterRpcIp(clusterRpcIp);
    }

    List<String> newSeedUrls = new ArrayList<>();
    for (String seedUrl : config.getSeedNodeUrls()) {
      String[] splits = seedUrl.split(":");
      if (splits.length != 4) {
        throw new BadSeedUrlFormatException(seedUrl);
      }
      String seedIP = splits[0];
      boolean isInvalidSeedIp = InetAddresses.isInetAddress(seedIP);
      if (!isInvalidSeedIp) {
        String newSeedIP = hostnameToIP(seedIP);
        newSeedUrls.add(newSeedIP + ":" + splits[1] + ":" + splits[2] + ":" + splits[3]);
      } else {
        newSeedUrls.add(seedUrl);
      }
    }
    config.setSeedNodeUrls(newSeedUrls);
    logger.debug("after replace, the clusterRpcIP={}, seedUrls={}", config.getClusterRpcIp(),
        config.getSeedNodeUrls());
  }

  private static boolean parseCommandLine(Options options, String[] params) {
    try {
      CommandLineParser parser = new DefaultParser();
      commandLine = parser.parse(options, params);
    } catch (ParseException e) {
      logger.error("parse conf params failed", e);
      return false;
    }
    return true;
  }

  /**
   * load an property file and set TsfileDBConfig variables.
   */
  private void loadProps() {

    String url = getPropsUrl();
    Properties properties = System.getProperties();
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(new File(url))) {
        logger.info("Start to read config file {}", url);
        properties.load(inputStream);
      } catch (IOException e) {
        logger.warn("Fail to find config file {}", url, e);
      }
    }
    config.setClusterRpcIp(properties.getProperty("cluster_rpc_ip", config.getClusterRpcIp()));

    config.setInternalMetaPort(Integer.parseInt(properties.getProperty(OPTION_INTERVAL_META_PORT,
        String.valueOf(config.getInternalMetaPort()))));

    config.setInternalDataPort(Integer.parseInt(properties.getProperty(OPTION_INTERVAL_DATA_PORT,
        Integer.toString(config.getInternalDataPort()))));

    config.setClusterRpcPort(Integer.parseInt(properties.getProperty(OPTION_CLUSTER_RPC_PORT,
        Integer.toString(config.getClusterRpcPort()))));

    config.setMaxConcurrentClientNum(Integer.parseInt(properties.getProperty(
        "max_concurrent_client_num", String.valueOf(config.getMaxConcurrentClientNum()))));

    config.setReplicationNum(Integer.parseInt(properties.getProperty(
        "default_replica_num", String.valueOf(config.getReplicationNum()))));

    config.setClusterName(properties.getProperty("cluster_name", config.getClusterName()));

    config.setRpcThriftCompressionEnabled(Boolean.parseBoolean(properties.getProperty(
        "rpc_thrift_compression_enable", String.valueOf(config.isRpcThriftCompressionEnabled()))));

    config.setConnectionTimeoutInMS(Integer.parseInt(properties
        .getProperty("connection_timeout_ms", String.valueOf(config.getConnectionTimeoutInMS()))));

    config.setReadOperationTimeoutMS(Integer.parseInt(properties
        .getProperty("read_operation_timeout_ms",
            String.valueOf(config.getReadOperationTimeoutMS()))));

    config.setCatchUpTimeoutMS(Integer.parseInt(properties
        .getProperty("catch_up_timeout_ms",
            String.valueOf(config.getCatchUpTimeoutMS()))));

    config.setWriteOperationTimeoutMS(Integer.parseInt(properties
        .getProperty("write_operation_timeout_ms",
            String.valueOf(config.getWriteOperationTimeoutMS()))));

    config.setUseBatchInLogCatchUp(Boolean.parseBoolean(properties.getProperty(
        "use_batch_in_catch_up", String.valueOf(config.isUseBatchInLogCatchUp()))));

    config.setMinNumOfLogsInMem(Integer.parseInt(properties
        .getProperty("min_num_of_logs_in_mem", String.valueOf(config.getMinNumOfLogsInMem()))));

    config.setMaxNumOfLogsInMem(Integer.parseInt(properties
        .getProperty("max_num_of_logs_in_mem", String.valueOf(config.getMaxNumOfLogsInMem()))));

    config.setLogDeleteCheckIntervalSecond(Integer.parseInt(properties
        .getProperty("log_deletion_check_interval_second",
            String.valueOf(config.getLogDeleteCheckIntervalSecond()))));

    config.setEnableAutoCreateSchema(Boolean.parseBoolean(properties
        .getProperty("enable_auto_create_schema",
            String.valueOf(config.isEnableAutoCreateSchema()))));

    config.setUseAsyncServer(
        Boolean.parseBoolean(properties.getProperty("is_use_async_server",
            String.valueOf(config.isUseAsyncServer()))));

    config.setUseAsyncApplier(
        Boolean.parseBoolean(properties.getProperty("is_use_async_applier",
            String.valueOf(config.isUseAsyncApplier()))));

    config.setEnableRaftLogPersistence(
        Boolean.parseBoolean(properties.getProperty("is_enable_raft_log_persistence",
            String.valueOf(config.isEnableRaftLogPersistence()))));

    config.setFlushRaftLogThreshold(Integer.parseInt(properties
        .getProperty("flush_raft_log_threshold", String.valueOf(config.getFlushRaftLogThreshold())))
    );

    config.setRaftLogBufferSize(Integer.parseInt(properties
        .getProperty("raft_log_buffer_size", String.valueOf(config.getRaftLogBufferSize())))
    );

    config.setMaxRaftLogIndexSizeInMemory(Integer
        .parseInt(properties.getProperty("max_raft_log_index_size_in_memory",
            String.valueOf(config.getMaxRaftLogIndexSizeInMemory()))));

    config.setMaxRaftLogPersistDataSizePerFile(Integer
        .parseInt(properties.getProperty("max_raft_log_persist_data_size_per_file",
            String.valueOf(config.getMaxRaftLogPersistDataSizePerFile()))));

    config.setMaxNumberOfPersistRaftLogFiles(Integer
        .parseInt(properties.getProperty("max_number_of_persist_raft_log_files",
            String.valueOf(config.getMaxNumberOfPersistRaftLogFiles()))));

    config.setMaxPersistRaftLogNumberOnDisk(
        Integer.parseInt(properties.getProperty("max_persist_raft_log_number_on_disk",
            String.valueOf(config.getMaxPersistRaftLogNumberOnDisk()))));

    config.setMaxNumberOfLogsPerFetchOnDisk(
        Integer.parseInt(properties.getProperty("max_number_of_logs_per_fetch_on_disk",
            String.valueOf(config.getMaxNumberOfLogsPerFetchOnDisk()))));

    config.setEnableUsePersistLogOnDiskToCatchUp(
        Boolean.parseBoolean(properties.getProperty("enable_use_persist_log_on_disk_to_catch_up",
            String.valueOf(config.isEnableUsePersistLogOnDiskToCatchUp()))));

    String consistencyLevel = properties.getProperty("consistency_level");
    if (consistencyLevel != null) {
      config.setConsistencyLevel(ConsistencyLevel.getConsistencyLevel(consistencyLevel));
    }

    String seedUrls = properties.getProperty(OPTION_SEED_NODES);
    if (seedUrls != null) {
      List<String> urlList = getSeedUrlList(seedUrls);
      config.setSeedNodeUrls(urlList);
    }
  }

  private List<String> getSeedUrlList(String seedUrls) {
    if (seedUrls == null) {
      return Collections.emptyList();
    }
    List<String> urlList = new ArrayList<>();
    String[] split = seedUrls.split(",");
    for (String nodeUrl : split) {
      nodeUrl = nodeUrl.trim();
      if ("".equals(nodeUrl)) {
        continue;
      }
      urlList.add(nodeUrl);
    }
    return urlList;
  }

  public void loadHotModifiedProps() throws QueryProcessException {
    Properties properties = getProperties();
    if (properties != null) {
      loadHotModifiedProps(properties);
    }
  }

  private Properties getProperties() throws QueryProcessException {
    String url = getPropsUrl();
    if (url == null) {
      return null;
    }
    Properties properties;
    try (InputStream inputStream = new FileInputStream(new File(url))) {
      logger.info("Start to reload config file {}", url);
      properties = new Properties();
      properties.load(inputStream);
    } catch (Exception e) {
      throw new QueryProcessException(
          String.format("Fail to reload config file %s because %s", url, e.getMessage()));
    }
    return properties;
  }

  /**
   * This method is for setting hot modified properties of the cluster. Currently, we support
   * max_concurrent_client_num, connection_timeout_ms, max_resolved_log_size
   *
   * @param properties
   * @throws QueryProcessException
   */
  public void loadHotModifiedProps(Properties properties) {

    config.setMaxConcurrentClientNum(Integer.parseInt(properties
        .getProperty("max_concurrent_client_num",
            String.valueOf(config.getMaxConcurrentClientNum()))));

    config.setConnectionTimeoutInMS(Integer.parseInt(properties
        .getProperty("connection_timeout_ms", String.valueOf(config.getConnectionTimeoutInMS()))));

    logger.info("Set cluster configuration {}", properties);
  }

  private String hostnameToIP(String hostname) throws UnknownHostException {
    InetAddress address = InetAddress.getByName(hostname);
    return address.getHostAddress();
  }

}