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

package org.apache.iotdb.it.env.cluster.node;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.CLUSTER_CONFIGURATIONS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_CONFIG_NODE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_DATA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_DATA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_SCHEMA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_SCHEMA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HYPHEN;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.INFLUXDB_RPC_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.IOT_CONSENSUS_V2_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.JAVA_CMD;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_DATA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_DATA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_SCHEMA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_SCHEMA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.MQTT_HOST;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.MQTT_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_CONFIG_NODE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_DATA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_DATA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_SCHEMA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_SCHEMA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_CONFIG_NODE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_DATA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_DATA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_SCHEMA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_SCHEMA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_LIB_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.REST_SERVICE_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_CONFIG_NODE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_DATA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_DATA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_SCHEMA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_SCHEMA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_CONFIG_NODE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_DATA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_DATA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_SCHEMA_REGION_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_SCHEMA_REGION_REPLICA_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TAB;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TARGET;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TEMPLATE_NODE_LIB_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TEMPLATE_NODE_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TRIGGER_LIB_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.UDF_LIB_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;
import static org.apache.iotdb.it.env.cluster.EnvUtils.fromConsensusAbbrToFullName;
import static org.apache.iotdb.it.env.cluster.EnvUtils.getTimeForLogDirectory;
import static org.apache.iotdb.it.env.cluster.EnvUtils.getValueOfIndex;

public abstract class AbstractNodeWrapper implements BaseNodeWrapper {
  private static final Logger logger = IoTDBTestLogger.logger;

  protected final String testClassName;
  protected final String testMethodName;
  protected final int[] portList;
  protected final int jmxPort;
  protected final MppJVMConfig jvmConfig;
  protected final int clusterIndex;
  protected final boolean isMultiCluster;
  protected Process instance;
  private final String nodeAddress;
  private int nodePort;
  private final int metricPort;
  private final long startTime;
  private List<String> killPoints = new ArrayList<>();

  /**
   * Mutable properties are always hardcoded default values to make the cluster be set up
   * successfully. Their lifecycles are the same with the whole test runner.
   */
  protected final Properties mutableNodeProperties = new Properties();

  protected final Properties mutableCommonProperties = new Properties();

  /**
   * Specified by -DClusterConfigurations in commandLine. A name of ClusterConfigurations can be
   * interpreted to a set of configurations below.
   */
  protected final Properties clusterConfigProperties = new Properties();

  /**
   * Immutable values are connection configurations, such as ip, ports which are generated randomly
   * during cluster initialization. Their lifecycles are the same with this node wrapper instance.
   */
  protected final Properties immutableNodeProperties = new Properties();

  protected final Properties immutableCommonProperties = new Properties();

  protected final MppCommonConfig outputCommonConfig = new MppCommonConfig();

  protected AbstractNodeWrapper(
      String testClassName,
      String testMethodName,
      int[] portList,
      int clusterIndex,
      boolean isMultiCluster,
      long startTime) {
    this.testClassName = testClassName;
    this.testMethodName = testMethodName;
    this.portList = portList;
    this.nodeAddress = "127.0.0.1";
    this.nodePort = portList[0];
    this.metricPort = portList[portList.length - 2];
    jmxPort = this.portList[portList.length - 1];
    // these properties can't be mutated.
    immutableCommonProperties.setProperty(UDF_LIB_DIR, MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty(TRIGGER_LIB_DIR, MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty(PIPE_LIB_DIR, MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty(MQTT_HOST, MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty(MQTT_PORT, MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty(REST_SERVICE_PORT, MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty(INFLUXDB_RPC_PORT, MppBaseConfig.NULL_VALUE);
    this.jvmConfig = initVMConfig();
    this.clusterIndex = clusterIndex;
    this.isMultiCluster = isMultiCluster;
    this.startTime = startTime;
  }

  /** CreateNodeDir must be called before changeConfig for persistent. */
  @Override
  public void createNodeDir() {
    // Copy templateNodePath to nodePath
    String destPath = getNodePath();
    try {
      try {
        if (new File(destPath).exists()) {
          PathUtils.deleteDirectory(Paths.get(destPath));
        }
      } catch (NoSuchFileException e) {
        // ignored
      }
      // Here we need to copy without follow symbolic links, so we can't use FileUtils directly.
      try (Stream<Path> s = Files.walk(Paths.get(TEMPLATE_NODE_PATH))) {
        s.forEach(
            source -> {
              Path destination =
                  Paths.get(destPath, source.toString().substring(TEMPLATE_NODE_PATH.length()));
              try {
                Files.copy(
                    source,
                    destination,
                    LinkOption.NOFOLLOW_LINKS,
                    StandardCopyOption.COPY_ATTRIBUTES);
              } catch (IOException e) {
                logger.error("Got error copying files to node dest dir", e);
                throw new RuntimeException(e);
              }
            });
      }
    } catch (IOException ex) {
      logger.error("Copy node dir failed", ex);
      throw new AssertionError();
    }
  }

  /** CreateLogDir must be called after changeConfig for correct log directory. */
  @Override
  public void createLogDir() {
    try {
      // Make sure the log dir exist, as the first file is output by starting script directly.
      FileUtils.createParentDirectories(new File(getLogPath()));
    } catch (IOException ex) {
      logger.error("Copy node dir failed", ex);
      throw new AssertionError();
    }
  }

  @Override
  public void destroyDir() {
    Exception lastException = null;
    for (int i = 0; i < 10; i++) {
      try {
        // DO NOT use FileUtils.forceDelete, as it will follow the symbolic link to make libs
        // read-only, which causes permission denied in deletion.
        PathUtils.deleteDirectory(Paths.get(getNodePath()));
        return;
      } catch (IOException ex) {
        lastException = ex;
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Delete node dir failed. " + e);
        }
      }
    }
    lastException.printStackTrace();
    throw new AssertionError("Delete node dir failed.");
  }

  /**
   * Change the config of this node. Here's the order of applied config source. The latter one will
   * override the former one if a key is duplicated.
   *
   * <ul>
   *   <li>1. Reading directly from assembled property files of other modules.
   *   <li>2. Default values which are hardcoded in mutable properties fields.
   *   <li>3. Values read from the path specified by system variables.
   *   <li>4. Values mutated by developers through {@link EnvFactory}.
   *   <li>5. Make sure immutable properties are not changed.
   * </ul>
   *
   * @param nodeConfig the values mutated through {@link EnvFactory}
   * @param commonConfig the values mutated through {@link EnvFactory}.
   * @param jvmConfig the JVM configurations need to be changed. If it's null, then nothing will be
   *     happened.
   */
  public final void changeConfig(
      MppBaseConfig nodeConfig, MppCommonConfig commonConfig, @Nullable MppJVMConfig jvmConfig) {
    try {
      // 1. Read directly from assembled property files
      // In a config-node, the files should be iotdb-confignode.properties and
      // iotdb-common.properties.
      MppBaseConfig outputNodeConfig = nodeConfig.emptyClone();

      // 2. Override by values which are hardcoded in mutable properties fields.
      reloadMutableFields();
      outputCommonConfig.updateProperties(mutableCommonProperties);
      outputNodeConfig.updateProperties(mutableNodeProperties);

      // 3. Override by values read from the path specified by system variables. The path is a
      // relative one to integration-test module dir.
      outputCommonConfig.updateProperties(getDefaultCommonConfigPath());
      outputNodeConfig.updateProperties(getDefaultNodeConfigPath());

      // 4. Override by values specified from the command line option -DClusterConfigProperties.
      // Mainly used for streaming module to simplify testing process.
      reloadClusterConfigurations();
      outputCommonConfig.updateProperties(clusterConfigProperties);

      // 5. Override by values mutated by developers
      outputCommonConfig.updateProperties(commonConfig);
      outputNodeConfig.updateProperties(nodeConfig);

      // 6. Restore immutable properties
      outputCommonConfig.updateProperties(immutableCommonProperties);
      outputNodeConfig.updateProperties(immutableNodeProperties);

      // Persistent
      outputCommonConfig.persistent(getSystemConfigPath());
      outputNodeConfig.persistent(getSystemConfigPath());
    } catch (IOException ex) {
      throw new AssertionError("Change the config of node failed. " + ex);
    }
    this.jvmConfig.override(jvmConfig);
  }

  private void reloadClusterConfigurations() {
    String valueStr = System.getProperty(CLUSTER_CONFIGURATIONS);
    if (valueStr == null) {
      return;
    }

    try {
      switch (getValueOfIndex(valueStr, clusterIndex)) {
        case LIGHT_WEIGHT_STANDALONE_MODE:
          clusterConfigProperties.setProperty(
              CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_SCHEMA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_DATA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REPLICATION_FACTOR,
              System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_SCHEMA_REGION_REPLICA_NUM));
          clusterConfigProperties.setProperty(
              DATA_REPLICATION_FACTOR,
              System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_DATA_REGION_REPLICA_NUM));
          break;
        case SCALABLE_SINGLE_NODE_MODE:
          clusterConfigProperties.setProperty(
              CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(SCALABLE_SINGLE_NODE_MODE_CONFIG_NODE_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(SCALABLE_SINGLE_NODE_MODE_SCHEMA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(SCALABLE_SINGLE_NODE_MODE_DATA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REPLICATION_FACTOR,
              System.getProperty(SCALABLE_SINGLE_NODE_MODE_SCHEMA_REGION_REPLICA_NUM));
          clusterConfigProperties.setProperty(
              DATA_REPLICATION_FACTOR,
              System.getProperty(SCALABLE_SINGLE_NODE_MODE_DATA_REGION_REPLICA_NUM));
          break;
        case HIGH_PERFORMANCE_MODE:
          clusterConfigProperties.setProperty(
              CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(HIGH_PERFORMANCE_MODE_CONFIG_NODE_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(HIGH_PERFORMANCE_MODE_SCHEMA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(HIGH_PERFORMANCE_MODE_DATA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REPLICATION_FACTOR,
              System.getProperty(HIGH_PERFORMANCE_MODE_SCHEMA_REGION_REPLICA_NUM));
          clusterConfigProperties.setProperty(
              DATA_REPLICATION_FACTOR,
              System.getProperty(HIGH_PERFORMANCE_MODE_DATA_REGION_REPLICA_NUM));
          break;
        case STRONG_CONSISTENCY_CLUSTER_MODE:
          clusterConfigProperties.setProperty(
              CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_CONFIG_NODE_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_SCHEMA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_DATA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REPLICATION_FACTOR,
              System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_SCHEMA_REGION_REPLICA_NUM));
          clusterConfigProperties.setProperty(
              DATA_REPLICATION_FACTOR,
              System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_DATA_REGION_REPLICA_NUM));
          break;
        case PIPE_CONSENSUS_BATCH_MODE:
          clusterConfigProperties.setProperty(
              CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(PIPE_CONSENSUS_BATCH_MODE_CONFIG_NODE_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(PIPE_CONSENSUS_BATCH_MODE_SCHEMA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(PIPE_CONSENSUS_BATCH_MODE_DATA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REPLICATION_FACTOR,
              System.getProperty(PIPE_CONSENSUS_BATCH_MODE_SCHEMA_REGION_REPLICA_NUM));
          clusterConfigProperties.setProperty(
              DATA_REPLICATION_FACTOR,
              System.getProperty(PIPE_CONSENSUS_BATCH_MODE_DATA_REGION_REPLICA_NUM));
          // set mode
          clusterConfigProperties.setProperty(
              IOT_CONSENSUS_V2_MODE, ConsensusFactory.IOT_CONSENSUS_V2_BATCH_MODE);
          break;
        case PIPE_CONSENSUS_STREAM_MODE:
          clusterConfigProperties.setProperty(
              CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(PIPE_CONSENSUS_STREAM_MODE_CONFIG_NODE_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(PIPE_CONSENSUS_STREAM_MODE_SCHEMA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              DATA_REGION_CONSENSUS_PROTOCOL_CLASS,
              fromConsensusAbbrToFullName(
                  System.getProperty(PIPE_CONSENSUS_STREAM_MODE_DATA_REGION_CONSENSUS)));
          clusterConfigProperties.setProperty(
              SCHEMA_REPLICATION_FACTOR,
              System.getProperty(PIPE_CONSENSUS_STREAM_MODE_SCHEMA_REGION_REPLICA_NUM));
          clusterConfigProperties.setProperty(
              DATA_REPLICATION_FACTOR,
              System.getProperty(PIPE_CONSENSUS_STREAM_MODE_DATA_REGION_REPLICA_NUM));
          // set mode
          clusterConfigProperties.setProperty(
              IOT_CONSENSUS_V2_MODE, ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE);
          break;
        default:
          // Print nothing to avoid polluting test outputs
      }
    } catch (NumberFormatException ignore) {
      // Ignore the exception
    }
  }

  @Override
  public void start() {
    try {
      File stdoutFile = new File(getLogPath());
      List<String> startCmd = new ArrayList<>();
      startCmd.add(JAVA_CMD);
      if (!SystemUtils.IS_JAVA_1_8) {
        startCmd.add("--add-opens=java.base/java.util.concurrent=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.util=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.io=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.net=ALL-UNNAMED");
      }

      String libPath =
          System.getProperty("user.dir")
              + File.separator
              + "target"
              + File.separator
              + "template-node-share"
              + File.separator
              + "lib"
              + File.separator;
      File directory = new File(libPath);
      String osName = System.getProperty("os.name").toLowerCase();
      String server_node_lib_path = "";
      if (directory.exists() && directory.isDirectory() && !osName.contains("win")) {
        File[] files = directory.listFiles();
        for (File file : files) {
          if (file.getName().startsWith("iotdb-server")) {
            server_node_lib_path = libPath + file.getName() + ":" + TEMPLATE_NODE_LIB_PATH;
            break;
          }
        }
      } else {
        server_node_lib_path = TEMPLATE_NODE_LIB_PATH;
      }
      startCmd.addAll(
          Arrays.asList(
              "-Dcom.sun.management.jmxremote.port=" + jmxPort,
              "-Dcom.sun.management.jmxremote.rmi.port=" + jmxPort,
              "-Djava.rmi.server.randomIDs=true",
              "-Dcom.sun.management.jmxremote.ssl=false",
              "-Dcom.sun.management.jmxremote.authenticate=false",
              "-Djava.rmi.server.hostname=" + getIp(),
              "-Xms" + jvmConfig.getInitHeapSize() + "m",
              "-Xmx" + jvmConfig.getMaxHeapSize() + "m",
              "-Duser.timezone=" + jvmConfig.getTimezone(),
              "-XX:MaxDirectMemorySize=" + jvmConfig.getMaxDirectMemorySize() + "m",
              "-Djdk.nio.maxCachedBufferSize=262144",
              "-D" + IoTDBConstant.INTEGRATION_TEST_KILL_POINTS + "=" + killPoints.toString(),
              "-cp",
              server_node_lib_path));
      addStartCmdParams(startCmd);
      FileUtils.write(
          stdoutFile, String.join(" ", startCmd) + "\n\n", StandardCharsets.UTF_8, true);
      ProcessBuilder processBuilder =
          new ProcessBuilder(startCmd)
              .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
              .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
      processBuilder.environment().put("CLASSPATH", TEMPLATE_NODE_LIB_PATH);
      this.instance = processBuilder.start();
      logger.info("In test {} {} started.", getTestLogDirName(), getId());
    } catch (IOException ex) {
      throw new AssertionError("Start node failed. " + ex);
    }
  }

  @Override
  public void stop() {
    if (this.instance == null) {
      return;
    }
    this.instance.destroy();
    try {
      if (!this.instance.waitFor(20, TimeUnit.SECONDS)) {
        this.instance.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Waiting node to shutdown error.", e);
    }
  }

  @Override
  public void stopForcibly() {
    if (this.instance == null) {
      return;
    }
    try {
      this.instance.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Waiting node to shutdown error.", e);
    }
  }

  @Override
  public boolean isAlive() {
    return this.instance != null && this.instance.isAlive();
  }

  @Override
  public final String getIp() {
    return this.nodeAddress;
  }

  @Override
  public final int getPort() {
    return this.nodePort;
  }

  @Override
  public int getMetricPort() {
    return this.metricPort;
  }

  public void setPort(int port) {
    this.nodePort = port;
  }

  @Override
  public final String getIpAndPortString() {
    return this.getIp() + ":" + this.getPort();
  }

  protected String workDirFilePath(String dirName, String fileName) {
    return getNodePath() + File.separator + dirName + File.separator + fileName;
  }

  protected String getLogPath() {
    return getLogDirPath() + File.separator + getId() + ".log";
  }

  public String getLogDirPath() {
    String baseDir =
        System.getProperty(USER_DIR)
            + File.separator
            + TARGET
            + File.separator
            + "cluster-logs"
            + File.separator
            + getTestLogDirName()
            + File.separator
            + getTimeForLogDirectory(startTime);
    if (!isMultiCluster) {
      return baseDir;
    } else {
      return baseDir + File.separator + getClusterIdStr();
    }
  }

  private String getClusterIdStr() {
    return clusterIndex + HYPHEN + outputCommonConfig.getClusterConfigStr();
  }

  public String getNodePath() {
    return System.getProperty(USER_DIR) + File.separator + TARGET + File.separator + getId();
  }

  public void dumpJVMSnapshot(String testCaseName) {
    JMXServiceURL url;
    try {
      url =
          new JMXServiceURL(
              String.format("service:jmx:rmi:///jndi/rmi://127.0.0.1:%d/jmxrmi", jmxPort));
    } catch (MalformedURLException e) {
      logger.error("Construct JMX URL failed", e);
      return;
    }

    try (JMXConnector connector = JMXConnectorFactory.connect(url)) {
      MBeanServerConnection mbsc = connector.getMBeanServerConnection();
      ThreadMXBean tmbean =
          ManagementFactory.newPlatformMXBeanProxy(
              mbsc, ManagementFactory.THREAD_MXBEAN_NAME, ThreadMXBean.class);
      ThreadInfo[] threadInfos = tmbean.dumpAllThreads(true, true);
      long[] deadlockIds = tmbean.findDeadlockedThreads();
      LocalDateTime currentTime = LocalDateTime.now();
      try (PrintWriter output =
          new PrintWriter(
              getLogDirPath() + File.separator + testCaseName + "_" + getId() + "-threads.dump")) {
        output.printf("# Captured at %s%n", currentTime);
        output.println("==================\n");
        if (deadlockIds != null && deadlockIds.length > 0) {
          output.printf("Detect DEADLOCK threads!%n");
          for (long deadlockId : deadlockIds) {
            ThreadInfo ti = tmbean.getThreadInfo(deadlockId);
            output.printf("%s #%d%n", ti.getThreadName(), ti.getThreadId());
          }
          output.println("==================\n");
        }
        for (ThreadInfo threadInfo : threadInfos) {
          dumpThread(output, threadInfo);
        }
      }
    } catch (IOException e) {
      logger.error("Connect with MBeanServer {} failed", url.getURLPath(), e);
    }
  }

  private void dumpThread(PrintWriter output, ThreadInfo ti) {
    StringBuilder sb = new StringBuilder("\"" + ti.getThreadName() + "\"");
    sb.append(" #").append(ti.getThreadId());
    sb.append(" ").append(ti.getThreadState()).append("\n");
    if (ti.getLockName() != null) {
      sb.append("Waiting on: ").append(ti.getLockName()).append("\n");
    }
    if (ti.getLockOwnerName() != null) {
      sb.append("Locked by: ")
          .append(ti.getLockOwnerName())
          .append(" #")
          .append(ti.getLockOwnerId())
          .append("\n");
    }
    Map<StackTraceElement, MonitorInfo> lockInfoMap = new HashMap<>();
    for (MonitorInfo monitor : ti.getLockedMonitors()) {
      lockInfoMap.put(monitor.getLockedStackFrame(), monitor);
    }
    for (StackTraceElement ele : ti.getStackTrace()) {
      if (lockInfoMap.containsKey(ele)) {
        MonitorInfo monitorInfo = lockInfoMap.get(ele);
        sb.append(TAB)
            .append("- lock ")
            .append(monitorInfo.getClassName())
            .append("@")
            .append(Integer.toHexString(monitorInfo.getIdentityHashCode()))
            .append("\n");
      }
      sb.append(TAB).append(ele).append("\n");
    }
    sb.append("\n");
    output.print(sb);
  }

  protected String getTestLogDirName() {
    if (testMethodName == null) {
      return testClassName;
    }
    return testClassName + File.separator + testMethodName;
  }

  public void setKillPoints(List<String> killPoints) {
    this.killPoints = killPoints;
  }

  private String getKillPoints() {
    return killPoints.toString();
  }

  /* Abstract methods, which must be implemented in ConfigNode and DataNode. */
  protected abstract void reloadMutableFields();

  protected abstract void renameFile();

  protected abstract String getSystemConfigPath();

  /** Return the node config file path specified through system variable */
  protected abstract String getDefaultNodeConfigPath();

  /** Return the common config file path specified through system variable */
  protected abstract String getDefaultCommonConfigPath();

  protected abstract void addStartCmdParams(List<String> params);

  public abstract String getSystemPropertiesPath();

  protected abstract MppJVMConfig initVMConfig();

  @Override
  public void executeJstack() {
    executeJstack(logger::info);
  }

  @Override
  public void executeJstack(final String testCaseName) {
    final String fileName =
        getLogDirPath() + File.separator + testCaseName + "_" + getId() + "-threads.jstack";
    try (final PrintWriter output = new PrintWriter(fileName)) {
      executeJstack(output::println);
    } catch (final IOException e) {
      logger.warn("IOException occurred when executing Jstack for {}", this.getId(), e);
    }
    logger.info("Jstack execution output can be found at {}", fileName);
  }

  private void executeJstack(final Consumer<String> consumer) {
    final long pid = this.getPid();
    if (pid == -1) {
      logger.warn("Failed to get pid for {} before executing Jstack", this.getId());
      return;
    }
    final String command = "jstack -l " + pid;
    logger.info("Executing command {} for {}", command, this.getId());
    try {
      final Process process = Runtime.getRuntime().exec(command);
      try (final BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          consumer.accept(line);
        }
      }
      final int exitCode = process.waitFor();
      logger.info("Command {} exited with code {}", command, exitCode);
    } catch (final IOException e) {
      logger.warn("IOException occurred when executing Jstack for {}", this.getId(), e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("InterruptedException occurred when executing Jstack for {}", this.getId(), e);
    }
  }

  /**
   * @return The native process ID of the process, -1 if failure.
   */
  @Override
  public long getPid() {
    final JMXServiceURL url;
    try {
      url =
          new JMXServiceURL(
              String.format("service:jmx:rmi:///jndi/rmi://127.0.0.1:%d/jmxrmi", jmxPort));
    } catch (final MalformedURLException ignored) {
      return -1;
    }
    try (final JMXConnector connector = JMXConnectorFactory.connect(url)) {
      final MBeanServerConnection mbsc = connector.getMBeanServerConnection();
      final RuntimeMXBean rmbean =
          ManagementFactory.newPlatformMXBeanProxy(
              mbsc, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
      return Long.parseLong(rmbean.getName().split("@")[0]);
    } catch (final Throwable ignored) {
      return -1;
    }
  }
}
