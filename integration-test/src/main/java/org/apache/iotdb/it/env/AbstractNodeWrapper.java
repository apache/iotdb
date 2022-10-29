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
package org.apache.iotdb.it.env;

import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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
  private final String templateNodePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "template-node";
  protected static final String templateNodeLibPath =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "template-node-share"
          + File.separator
          + "lib"
          + File.separator
          + "*";
  protected final String testClassName;
  protected final String testMethodName;
  protected final int[] portList;
  protected final int jmxPort;
  private final String TAB = "  ";
  private Process instance;
  private String node_address;
  private int node_port;

  public AbstractNodeWrapper(String testClassName, String testMethodName, int[] portList) {
    this.testClassName = testClassName;
    this.testMethodName = testMethodName;
    this.portList = portList;
    this.node_address = "127.0.0.1";
    this.node_port = portList[0];
    jmxPort = this.portList[portList.length - 1];
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
      try (Stream<Path> s = Files.walk(Paths.get(this.templateNodePath))) {
        s.forEach(
            source -> {
              Path destination =
                  Paths.get(destPath, source.toString().substring(this.templateNodePath.length()));
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
    for (int i = 0; i < 3; i++) {
      try {
        // DO NOT use FileUtils.forceDelete, as it will follow the symbolic link to make libs
        // read-only, which causes permission denied in deletion.
        PathUtils.deleteDirectory(Paths.get(getNodePath()));
        return;
      } catch (IOException ex) {
        logger.warn("Delete node dir failed. RetryTimes={}", i + 1, ex);
        try {
          TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
          fail("Delete node dir failed. " + e);
        }
      }
    }
    fail("Delete node dir failed.");
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
              "-Xms200m",
              "-Xmx200m",
              "-XX:MaxDirectMemorySize=200m",
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
  }

  @Override
  public void waitingToShutDown() {
    while (this.instance.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.error("Waiting node to shutdown." + e);
      }
    }
  }

  @Override
  public void changeConfig(Properties properties) {
    try {
      String commonConfigPath = getCommonConfigPath();
      Properties commonConfigProperties = new Properties();
      try (InputStream confInput = Files.newInputStream(Paths.get(commonConfigPath))) {
        commonConfigProperties.load(confInput);
      }
      String configPath = getConfigPath();
      Properties configProperties = new Properties();
      try (InputStream confInput = Files.newInputStream(Paths.get(configPath))) {
        configProperties.load(confInput);
      }
      commonConfigProperties.putAll(configProperties);
      updateConfig(commonConfigProperties);
      if (properties != null && !properties.isEmpty()) {
        commonConfigProperties.putAll(properties);
      }
      try (FileWriter confOutput = new FileWriter(configPath)) {
        commonConfigProperties.store(confOutput, null);
      }
    } catch (IOException ex) {
      fail("Change the config of data node failed. " + ex);
    }
  }

  @Override
  public final String getIp() {
    return this.node_address;
  }

  public void setIp(String ip) {
    this.node_address = ip;
  }

  @Override
  public final int getPort() {
    return this.node_port;
  }

  public void setPort(int port) {
    this.node_port = port;
  }

  @Override
  public final String getIpAndPortString() {
    return this.getIp() + ":" + this.getPort();
  }

  protected String workDirFilePath(String dirName, String fileName) {
    return getNodePath() + File.separator + dirName + File.separator + fileName;
  }

  protected abstract String getConfigPath();

  protected abstract String getCommonConfigPath();

  protected abstract void updateConfig(Properties properties);

  protected abstract void addStartCmdParams(List<String> params);

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

  protected abstract void renameFile();
}
