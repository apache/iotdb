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

import org.apache.iotdb.itbase.env.BaseNodeWrapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public abstract class AbstractNodeWrapper implements BaseNodeWrapper {
  private static final Logger logger = LoggerFactory.getLogger(AbstractNodeWrapper.class);
  private final String templateNodePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "template-node";
  private final String templateNodeLibPath =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "template-node-share"
          + File.separator
          + "lib";
  private final File NULL_FILE =
      SystemUtils.IS_OS_WINDOWS ? new File("nul") : new File("/dev/null");
  protected final String testClassName;
  protected final String testMethodName;
  protected final int[] portList;
  private final int jmxPort;
  private final String jmxUserName = "root";
  private final String jmxPassword = "passw!d";
  private final String TAB = "  ";
  private Process instance;

  public AbstractNodeWrapper(String testClassName, String testMethodName, int[] portList) {
    this.testClassName = testClassName;
    this.testMethodName = testMethodName;
    this.portList = portList;
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
      Path destLibPath = Paths.get(destPath, "lib");
      FileUtils.forceMkdir(destLibPath.toFile());
      // Create hard link for libs to decrease copy size
      try (Stream<Path> s = Files.walk(Paths.get(this.templateNodeLibPath))) {
        s.forEach(
            source -> {
              if (source.toFile().isFile()) {
                Path destination =
                    Paths.get(
                        destLibPath.toString(),
                        source.toString().substring(this.templateNodeLibPath.length()));
                try {
                  Files.createLink(destination, source);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            });
      }
      String startScriptPath = getStartScriptPath();
      String stopScriptPath = getStopScriptPath();
      if (!new File(startScriptPath).setExecutable(true)) {
        logger.error("Change {} to executable failed.", startScriptPath);
      }
      if (!new File(stopScriptPath).setExecutable(true)) {
        logger.error("Change {} to executable failed.", stopScriptPath);
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
      ProcessBuilder processBuilder =
          new ProcessBuilder(getStartScriptPath())
              .redirectOutput(stdoutFile)
              .redirectError(stdoutFile);
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
    // In Windows, the IoTDB process is started as a subprocess of the original batch script with a
    // new pid, so we need to kill the new subprocess as well.
    if (SystemUtils.IS_OS_WINDOWS) {
      ProcessBuilder processBuilder =
          new ProcessBuilder(getStopScriptPath())
              .redirectOutput(NULL_FILE)
              .redirectError(NULL_FILE);
      processBuilder.environment().put("CONSOLE_LOG_LEVEL", "DEBUG");
      Process p = null;
      try {
        p = processBuilder.start();
        p.waitFor(5, TimeUnit.SECONDS);
      } catch (IOException | InterruptedException e) {
        logger.error("Stop instance in Windows failed", e);
        if (p != null) {
          p.destroyForcibly();
        }
      }
    }
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
      String configPath = getConfigPath();
      Properties configProperties = new Properties();
      try (InputStream confInput = Files.newInputStream(Paths.get(configPath))) {
        configProperties.load(confInput);
      }
      updateConfig(configProperties);
      if (properties != null && !properties.isEmpty()) {
        configProperties.putAll(properties);
      }
      try (FileWriter confOutput = new FileWriter(configPath)) {
        configProperties.store(confOutput, null);
      }
      // Change JMX config
      Path path = Paths.get(getEnvConfigPath());
      String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
      content = content.replaceAll("JMX_LOCAL=\"true\"", "JMX_LOCAL=\"false\"");
      content = content.replaceAll("JMX_PORT=\"\\d+\"", String.format("JMX_PORT=\"%d\"", jmxPort));
      Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException ex) {
      fail("Change the config of data node failed. " + ex);
    }
  }

  @Override
  public final String getIp() {
    return "127.0.0.1";
  }

  @Override
  public final int getPort() {
    return portList[0];
  }

  @Override
  public final String getIpAndPortString() {
    return this.getIp() + ":" + this.getPort();
  }

  protected String workDirFilePath(String dirName, String fileName) {
    return getNodePath() + File.separator + dirName + File.separator + fileName;
  }

  protected abstract String getConfigPath();

  protected abstract String getEnvConfigPath();

  protected abstract void updateConfig(Properties properties);

  protected abstract String getStartScriptPath();

  protected abstract String getStopScriptPath();

  private String getLogPath() {
    return getLogDirPath() + File.separator + getId() + ".log";
  }

  private String getLogDirPath() {
    return System.getProperty("user.dir")
        + File.separator
        + "target"
        + File.separator
        + "cluster-logs"
        + File.separator
        + getTestLogDirName();
  }

  private String getNodePath() {
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
    Map<String, Object> environment =
        Collections.singletonMap(JMXConnector.CREDENTIALS, new String[] {jmxUserName, jmxPassword});

    try (JMXConnector connector = JMXConnectorFactory.connect(url, environment)) {
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
}
