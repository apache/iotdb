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

package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.it.env.EnvFactory;
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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
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
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public abstract class AbstractNodeWrapper implements BaseNodeWrapper {
  private static final Logger logger = IoTDBTestLogger.logger;
  private static final String javaCmd =
      System.getProperty("java.home")
          + File.separator
          + "bin"
          + File.separator
          + (SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");
  public static final String templateNodePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "template-node";
  public static final String templateNodeLibPath =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "template-node-share"
          + File.separator
          + "lib"
          + File.separator
          + "*";

  protected static final String propertyKeyConfigNodeConsensusProtocolClass =
      "config_node_consensus_protocol_class";
  protected static final String propertyKeySchemaRegionConsensusProtocolClass =
      "schema_region_consensus_protocol_class";
  protected static final String propertyKeyDataRegionConsensusProtocolClass =
      "data_region_consensus_protocol_class";
  protected static final String propertyKeySchemaReplicationFactor = "schema_replication_factor";
  protected static final String propertyKeyDataReplicationFactor = "data_replication_factor";

  protected final String testClassName;
  protected final String testMethodName;
  protected final int[] portList;
  protected final int jmxPort;
  protected final MppJVMConfig jvmConfig;
  private final String TAB = "  ";
  private Process instance;
  private final String nodeAddress;
  private int nodePort;

  /**
   * Mutable properties are always hardcoded default values to make the cluster be set up
   * successfully. Their lifecycles are the same with the whole test runner.
   */
  protected final Properties mutableNodeProperties = new Properties();

  protected final Properties mutableCommonProperties = new Properties();

  /**
   * Immutable values are connection configurations, such as ip, ports which are generated randomly
   * during cluster initialization. Their lifecycles are the same with this node wrapper instance.
   */
  protected final Properties immutableNodeProperties = new Properties();

  protected final Properties immutableCommonProperties = new Properties();

  public AbstractNodeWrapper(String testClassName, String testMethodName, int[] portList) {
    this.testClassName = testClassName;
    this.testMethodName = testMethodName;
    this.portList = portList;
    this.nodeAddress = "127.0.0.1";
    this.nodePort = portList[0];
    jmxPort = this.portList[portList.length - 1];
    // these properties can't be mutated.
    immutableCommonProperties.setProperty("udf_lib_dir", MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty("trigger_lib_dir", MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty("mqtt_host", MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty("mqtt_port", MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty("rest_service_port", MppBaseConfig.NULL_VALUE);
    immutableCommonProperties.setProperty("influxdb_rpc_port", MppBaseConfig.NULL_VALUE);
    this.jvmConfig = initVMConfig();
  }

  @Override
  public void createDir() {
    // Copy templateNodePath to nodePath
    String destPath = getNodePath();
    try {
      try {
        PathUtils.deleteDirectory(Paths.get(destPath));
      } catch (NoSuchFileException e) {
        // ignored
      }
      // Here we need to copy without follow symbolic links, so we can't use FileUtils directly.
      try (Stream<Path> s = Files.walk(Paths.get(templateNodePath))) {
        s.forEach(
            source -> {
              Path destination =
                  Paths.get(destPath, source.toString().substring(templateNodePath.length()));
              try {
                Files.copy(
                    source,
                    destination,
                    LinkOption.NOFOLLOW_LINKS,
                    StandardCopyOption.COPY_ATTRIBUTES);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
      // Make sure the log dir exist, as the first file is output by starting script directly.
      FileUtils.createParentDirectories(new File(getLogPath()));
    } catch (IOException ex) {
      logger.error("Copy node dir failed", ex);
      fail();
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
          fail("Delete node dir failed. " + e);
        }
      }
    }
    lastException.printStackTrace();
    fail("Delete node dir failed.");
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
      MppBaseConfig outputCommonConfig = commonConfig.emptyClone();
      MppBaseConfig outputNodeConfig = nodeConfig.emptyClone();

      // 2. Override by values which are hardcoded in mutable properties fields.
      reloadMutableFields();
      outputCommonConfig.updateProperties(mutableCommonProperties);
      outputNodeConfig.updateProperties(mutableNodeProperties);

      // 3. Override by values read from the path specified by system variables. The path is a
      // relative one to integration-test module dir.
      outputCommonConfig.updateProperties(getDefaultCommonConfigPath());
      outputNodeConfig.updateProperties(getDefaultNodeConfigPath());

      // 4. Override by values mutated by developers
      outputCommonConfig.updateProperties(commonConfig);
      outputNodeConfig.updateProperties(nodeConfig);

      // 5. Restore immutable properties
      outputCommonConfig.updateProperties(immutableCommonProperties);
      outputNodeConfig.updateProperties(immutableNodeProperties);

      // Persistent
      outputCommonConfig.persistent(getTargetCommonConfigPath());
      outputNodeConfig.persistent(getTargetNodeConfigPath());
    } catch (IOException ex) {
      fail("Change the config of node failed. " + ex);
    }
    this.jvmConfig.override(jvmConfig);
  }

  @Override
  public void start() {
    try {
      File stdoutFile = new File(getLogPath());
      List<String> startCmd = new ArrayList<>();
      startCmd.add(javaCmd);
      if (!SystemUtils.IS_JAVA_1_8) {
        startCmd.add("--add-opens=java.base/java.util.concurrent=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.util=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.io=ALL-UNNAMED");
        startCmd.add("--add-opens=java.base/java.net=ALL-UNNAMED");
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
              "-XX:MaxDirectMemorySize=" + jvmConfig.getMaxDirectMemorySize() + "m",
              "-Djdk.nio.maxCachedBufferSize=262144",
              "-cp",
              templateNodeLibPath));
      addStartCmdParams(startCmd);
      FileUtils.write(
          stdoutFile, String.join(" ", startCmd) + "\n\n", StandardCharsets.UTF_8, true);
      ProcessBuilder processBuilder =
          new ProcessBuilder(startCmd)
              .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
              .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
      processBuilder.environment().put("CLASSPATH", templateNodeLibPath);
      this.instance = processBuilder.start();
      logger.info("In test {} {} started.", getTestLogDirName(), getId());
    } catch (IOException ex) {
      fail("Start node failed. " + ex);
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
      logger.error("Waiting node to shutdown error." + e);
    }
  }

  @Override
  public final String getIp() {
    return this.nodeAddress;
  }

  @Override
  public final int getPort() {
    return this.nodePort;
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

  private String getLogPath() {
    return getLogDirPath() + File.separator + getId() + ".log";
  }

  protected String getLogDirPath() {
    return System.getProperty("user.dir")
        + File.separator
        + "target"
        + File.separator
        + "cluster-logs"
        + File.separator
        + getTestLogDirName();
  }

  protected String getNodePath() {
    return System.getProperty("user.dir") + File.separator + "target" + File.separator + getId();
  }

  @Override
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
        output.printf("# Captured at %s\n", currentTime);
        output.println("==================\n");
        if (deadlockIds != null && deadlockIds.length > 0) {
          output.printf("Detect DEADLOCK threads!\n");
          for (long deadlockId : deadlockIds) {
            ThreadInfo ti = tmbean.getThreadInfo(deadlockId);
            output.printf("%s #%d\n", ti.getThreadName(), ti.getThreadId());
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

  private String getTestLogDirName() {
    if (testMethodName == null) {
      return testClassName;
    }
    return testClassName + "_" + testMethodName;
  }

  /* Abstract methods, which must be implemented in ConfigNode and DataNode. */
  protected abstract void reloadMutableFields();

  protected abstract void renameFile();

  protected abstract String getTargetNodeConfigPath();

  protected abstract String getTargetCommonConfigPath();

  /** Return the node config file path specified through system variable */
  protected abstract String getDefaultNodeConfigPath();

  /** Return the common config file path specified through system variable */
  protected abstract String getDefaultCommonConfigPath();

  protected abstract void addStartCmdParams(List<String> params);

  public abstract String getSystemPropertiesPath();

  protected abstract MppJVMConfig initVMConfig();
}
