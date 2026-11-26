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

import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.env.IntegrationTestEnvMessages;
import org.apache.iotdb.it.framework.IoTDBTestLogger;

import org.apache.tsfile.external.commons.io.file.PathUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.AI_NODE_NAME;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TARGET;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;
import static org.apache.iotdb.it.env.cluster.EnvUtils.getTimeForLogDirectory;

public class AINodeWrapper extends AbstractNodeWrapper {

  private static final Logger logger = IoTDBTestLogger.logger;
  private final long startTime;
  private final String seedConfigNode;
  private final int clusterIngressPort;

  private static final String START_SCRIPT_FILE = "start-ainode.sh";
  private static final String STOP_SCRIPT_FILE = "stop-ainode.sh";

  private static final String SHELL_COMMAND = "bash";

  private static final String PROPERTIES_FILE = "iotdb-ainode.properties";
  public static final String CONFIG_PATH = "conf";
  public static final String SCRIPT_PATH = "sbin";
  public static final String BUILT_IN_MODEL_PATH = "data/ainode/models/builtin";
  public static final String CACHE_BUILT_IN_MODEL_PATH = "/data/ainode/models";

  private static final long GRACEFUL_STOP_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(20);
  private static final long FORCE_STOP_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
  private static final long STOP_SCRIPT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long FORCE_STOP_SCRIPT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long PROCESS_EXIT_POLL_INTERVAL_MILLIS = 100L;

  private final Set<ProcessHandle> survivingProcessHandles = ConcurrentHashMap.newKeySet();

  private void replaceAttribute(String[] keys, String[] values, String filePath) {
    Properties props = new Properties();
    try (FileInputStream in = new FileInputStream(filePath)) {
      props.load(in);
    } catch (IOException e) {
      logger.warn("Failed to load existing AINode properties from {}, because: ", filePath, e);
    }
    for (int i = 0; i < keys.length; i++) {
      props.setProperty(keys[i], values[i]);
    }
    try (FileOutputStream out = new FileOutputStream(filePath)) {
      props.store(out, "Updated by AINode integration-test env");
    } catch (IOException e) {
      logger.error("Failed to save properties to {}, because:", filePath, e);
    }
  }

  public AINodeWrapper(
      String seedConfigNode,
      int clusterIngressPort,
      String testClassName,
      String testMethodName,
      int clusterIndex,
      int[] port,
      long startTime) {
    super(testClassName, testMethodName, port, clusterIndex, false, startTime);
    this.seedConfigNode = seedConfigNode;
    this.clusterIngressPort = clusterIngressPort;
    this.startTime = startTime;
  }

  @Override
  public String getLogDirPath() {
    return System.getProperty(USER_DIR)
        + File.separator
        + TARGET
        + File.separator
        + "ainode-logs"
        + File.separator
        + getTestLogDirName()
        + File.separator
        + getTimeForLogDirectory(startTime);
  }

  @Override
  String getNodeType() {
    return "ainode";
  }

  @Override
  public void start() {
    try {
      File stdoutFile = new File(getLogPath());
      String filePrefix = getNodePath();
      String propertiesFile =
          filePrefix + File.separator + CONFIG_PATH + File.separator + PROPERTIES_FILE;

      // set attribute
      replaceAttribute(
          new String[] {"ain_seed_config_node", "ain_rpc_port", "ain_cluster_ingress_port"},
          new String[] {
            this.seedConfigNode,
            Integer.toString(getPort()),
            Integer.toString(this.clusterIngressPort)
          },
          propertiesFile);

      // Link built-in LTSM weights from the runner-wide cache. These can be hundreds of MB to
      // multiple GB; copying them per fork dominates IT startup. Symlinks share read-only weights
      // across forks; we fall back to a copy on platforms / filesystems that reject symlinks.
      String builtInModelPath = filePrefix + File.separator + BUILT_IN_MODEL_PATH;
      File builtInModelDir = new File(builtInModelPath);
      try {
        if (builtInModelDir.exists()) {
          PathUtils.deleteDirectory(builtInModelDir.toPath());
        }
      } catch (NoSuchFileException e) {
        // ignored
      }
      Path cacheRoot = Paths.get(CACHE_BUILT_IN_MODEL_PATH);
      Path destRoot = builtInModelDir.toPath();
      builtInModelDir.getParentFile().mkdirs();
      try {
        Files.createSymbolicLink(destRoot, cacheRoot);
        logger.info("AINode symlinked model weights {} -> {}", destRoot, cacheRoot);
      } catch (UnsupportedOperationException | IOException symlinkErr) {
        logger.warn(
            "AINode failed to symlink {} -> {} ({}), falling back to copy",
            destRoot,
            cacheRoot,
            symlinkErr.toString());
        builtInModelDir.mkdirs();
        try (Stream<Path> s = Files.walk(cacheRoot)) {
          s.forEach(
              source -> {
                Path destination =
                    Paths.get(
                        builtInModelPath,
                        source.toString().substring(CACHE_BUILT_IN_MODEL_PATH.length()));
                logger.info("AINode copying model weights from {} to {}", source, destination);
                try {
                  Files.copy(
                      source,
                      destination,
                      LinkOption.NOFOLLOW_LINKS,
                      StandardCopyOption.COPY_ATTRIBUTES);
                } catch (IOException e) {
                  logger.error("AINode got error copying model weights", e);
                  throw new RuntimeException(e);
                }
              });
        } catch (Exception e) {
          logger.error("AINode got error copying model weights", e);
        }
      }

      // start AINode
      List<String> startCommand = new ArrayList<>();
      startCommand.add(SHELL_COMMAND);
      startCommand.add(
          filePrefix + File.separator + SCRIPT_PATH + File.separator + START_SCRIPT_FILE);

      ProcessBuilder processBuilder =
          new ProcessBuilder(startCommand)
              .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
              .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
      this.survivingProcessHandles.clear();
      this.instance = processBuilder.start();
      logger.info("In test {} {} started.", getTestLogDirName(), getId());
    } catch (Exception e) {
      throw new AssertionError("Start AI Node failed. " + e + Paths.get(""));
    }
  }

  @Override
  public void stop() {
    stop(GRACEFUL_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop(final long timeout, final TimeUnit unit) {
    stopAndConfirmExit(unit.toMillis(timeout), FORCE_STOP_TIMEOUT_MILLIS);
  }

  @Override
  public void stopForcibly() {
    stop();
  }

  @Override
  public boolean isAlive() {
    if (super.isAlive()) {
      return true;
    }
    return survivingProcessHandles.stream().anyMatch(ProcessHandle::isAlive);
  }

  public List<Long> getSurvivingPids() {
    Set<Long> pids = new LinkedHashSet<>();
    Process process = this.instance;
    if (process != null && process.isAlive()) {
      pids.add(process.pid());
    }
    for (ProcessHandle handle : survivingProcessHandles) {
      if (handle.isAlive()) {
        pids.add(handle.pid());
      }
    }
    return new ArrayList<>(pids);
  }

  protected boolean stopAndConfirmExit(
      final long graceTimeoutMillis, final long forceTimeoutMillis) {
    final Process process = this.instance;
    if (process == null && survivingProcessHandles.isEmpty()) {
      return true;
    }

    final Set<ProcessHandle> trackedHandles = new LinkedHashSet<>(survivingProcessHandles);
    refreshTrackedHandles(process, trackedHandles);
    if (!hasAliveProcesses(process, trackedHandles)) {
      survivingProcessHandles.clear();
      logger.info(
          IntegrationTestEnvMessages.LOG_IN_TEST_ARG_ARG_STOPPED_57DB922B,
          getTestLogDirName(),
          getId());
      return true;
    }

    final AtomicBoolean interruptedFlag = new AtomicBoolean(Thread.interrupted());
    try {
      final long normalizedGraceMillis = Math.max(0L, graceTimeoutMillis);
      final long graceDeadlineNanos =
          System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(normalizedGraceMillis);

      if (normalizedGraceMillis > 0L) {
        final long stopScriptTimeoutMillis =
            Math.min(remainingMillis(graceDeadlineNanos), STOP_SCRIPT_TIMEOUT_MILLIS);
        runStopScript(stopScriptTimeoutMillis, interruptedFlag);
        refreshTrackedHandles(process, trackedHandles);

        if (process != null && process.isAlive()) {
          process.destroy();
        }
        final Set<ProcessHandle> gracefullySignalled = new HashSet<>();
        while (hasAliveProcesses(process, trackedHandles)) {
          final long remainingGraceMillis = remainingMillis(graceDeadlineNanos);
          if (remainingGraceMillis <= 0L) {
            break;
          }
          refreshTrackedHandles(process, trackedHandles);
          if (process == null || !process.isAlive()) {
            for (ProcessHandle handle : trackedHandles) {
              if (handle.isAlive() && gracefullySignalled.add(handle)) {
                handle.destroy();
              }
            }
          }
          waitForAnyProgress(
              process,
              trackedHandles,
              Math.min(PROCESS_EXIT_POLL_INTERVAL_MILLIS, remainingGraceMillis),
              interruptedFlag);
        }
      }

      refreshTrackedHandles(process, trackedHandles);
      if (hasAliveProcesses(process, trackedHandles)) {
        logger.warn(
            IntegrationTestEnvMessages
                .LOG_NODE_ARG_DID_NOT_EXIT_WITHIN_ARG_MS_FORCIBLY_TERMINATING_PROCESS_TREE_65FA74DB,
            getId(),
            normalizedGraceMillis);
        final Set<ProcessHandle> forciblySignalled = new HashSet<>();
        if (process != null && process.isAlive()) {
          process.destroyForcibly();
          forciblySignalled.add(process.toHandle());
        }
        for (ProcessHandle handle : new ArrayList<>(trackedHandles)) {
          if (handle.isAlive() && forciblySignalled.add(handle)) {
            handle.destroyForcibly();
          }
        }

        final long forceDeadlineNanos =
            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0L, forceTimeoutMillis));
        while (hasAliveProcesses(process, trackedHandles)) {
          final long remainingForceMillis = remainingMillis(forceDeadlineNanos);
          if (remainingForceMillis <= 0L) {
            break;
          }
          refreshTrackedHandles(process, trackedHandles);
          for (ProcessHandle handle : new ArrayList<>(trackedHandles)) {
            if (handle.isAlive() && forciblySignalled.add(handle)) {
              handle.destroyForcibly();
            }
          }
          waitForAnyProgress(
              process,
              trackedHandles,
              Math.min(PROCESS_EXIT_POLL_INTERVAL_MILLIS, remainingForceMillis),
              interruptedFlag);
        }
      }

      refreshTrackedHandles(process, trackedHandles);
      final Set<ProcessHandle> stillAlive = new LinkedHashSet<>();
      if (process != null && process.isAlive()) {
        stillAlive.add(process.toHandle());
      }
      for (ProcessHandle handle : trackedHandles) {
        if (handle.isAlive()) {
          stillAlive.add(handle);
        }
      }
      survivingProcessHandles.addAll(stillAlive);
      survivingProcessHandles.retainAll(stillAlive);

      if (stillAlive.isEmpty() && (process == null || !process.isAlive())) {
        logger.info(
            IntegrationTestEnvMessages.LOG_IN_TEST_ARG_ARG_STOPPED_57DB922B,
            getTestLogDirName(),
            getId());
        return true;
      }

      logger.error(
          IntegrationTestEnvMessages
              .LOG_FAILED_TO_CONFIRM_EXIT_OF_NODE_ARG_SURVIVING_PIDS_ARG_CBF3E211,
          getId(),
          getSurvivingPids());
      return false;
    } finally {
      if (interruptedFlag.get()) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void runStopScript(final long timeoutMillis, final AtomicBoolean interruptedFlag) {
    if (timeoutMillis <= 0L) {
      return;
    }
    final File stdoutFile = new File(getLogPath());
    final File parentDir = stdoutFile.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      parentDir.mkdirs();
    }
    final String filePrefix = getNodePath();
    final List<String> stopCommand = new ArrayList<>();
    stopCommand.add(SHELL_COMMAND);
    stopCommand.add(filePrefix + File.separator + SCRIPT_PATH + File.separator + STOP_SCRIPT_FILE);
    final ProcessBuilder processBuilder =
        new ProcessBuilder(stopCommand)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
            .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
    try {
      final Process stopProcess = processBuilder.start();
      if (!waitForProcessUninterruptibly(stopProcess, timeoutMillis, interruptedFlag)) {
        logger.warn(
            IntegrationTestEnvMessages
                .LOG_NODE_ARG_S_STOP_SCRIPT_DID_NOT_EXIT_WITHIN_ARG_MS_KILLING_STOP_SCRIPT_9393F866,
            getId(),
            timeoutMillis);
        try (Stream<ProcessHandle> descendants = stopProcess.toHandle().descendants()) {
          descendants.forEach(ProcessHandle::destroyForcibly);
        }
        stopProcess.destroyForcibly();
        waitForProcessUninterruptibly(
            stopProcess, FORCE_STOP_SCRIPT_TIMEOUT_MILLIS, interruptedFlag);
        return;
      }
      final int exitCode = stopProcess.exitValue();
      if (exitCode != 0) {
        logger.warn(
            IntegrationTestEnvMessages.LOG_NODE_ARG_S_STOP_SCRIPT_EXITED_WITH_CODE_ARG_E5C267CF,
            getId(),
            exitCode);
      }
    } catch (IOException e) {
      logger.warn(
          IntegrationTestEnvMessages
              .LOG_FAILED_TO_RUN_STOP_SCRIPT_FOR_NODE_ARG_FALLING_BACK_TO_DIRECT_PROCESS_TERMINATION_13E3FD59,
          getId(),
          e);
    }
  }

  private static void refreshTrackedHandles(
      final Process process, final Set<ProcessHandle> trackedHandles) {
    if (process != null) {
      trackedHandles.add(process.toHandle());
    }
    for (ProcessHandle handle : new ArrayList<>(trackedHandles)) {
      if (handle.isAlive()) {
        try (Stream<ProcessHandle> descendants = handle.descendants()) {
          descendants.forEach(trackedHandles::add);
        }
      }
    }
  }

  private static boolean hasAliveProcesses(
      final Process process, final Set<ProcessHandle> trackedHandles) {
    if (process != null && process.isAlive()) {
      return true;
    }
    for (ProcessHandle handle : trackedHandles) {
      if (handle.isAlive()) {
        return true;
      }
    }
    return false;
  }

  private static void waitForAnyProgress(
      final Process process,
      final Set<ProcessHandle> trackedHandles,
      final long waitMillis,
      final AtomicBoolean interruptedFlag) {
    if (waitMillis <= 0L) {
      return;
    }
    try {
      if (process != null && process.isAlive()) {
        process.waitFor(waitMillis, TimeUnit.MILLISECONDS);
        return;
      }
      for (ProcessHandle handle : trackedHandles) {
        if (handle.isAlive()) {
          try {
            handle.onExit().get(waitMillis, TimeUnit.MILLISECONDS);
          } catch (TimeoutException | ExecutionException ignored) {
            // Re-checked by caller loop.
          }
          return;
        }
      }
    } catch (InterruptedException e) {
      interruptedFlag.set(true);
    }
  }

  private static boolean waitForProcessUninterruptibly(
      final Process process, final long timeoutMillis, final AtomicBoolean interruptedFlag) {
    final long deadlineNanos =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMillis));
    while (process.isAlive()) {
      final long remaining = remainingMillis(deadlineNanos);
      if (remaining <= 0L) {
        return !process.isAlive();
      }
      try {
        if (process.waitFor(remaining, TimeUnit.MILLISECONDS)) {
          return true;
        }
      } catch (InterruptedException e) {
        interruptedFlag.set(true);
      }
    }
    return true;
  }

  private static long remainingMillis(final long deadlineNanos) {
    final long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0L) {
      return 0L;
    }
    final long oneMilliNanos = TimeUnit.MILLISECONDS.toNanos(1);
    return (remainingNanos + oneMilliNanos - 1) / oneMilliNanos;
  }

  @Override
  public int getMetricPort() {
    // no metric currently
    return -1;
  }

  @Override
  public String getId() {
    return AI_NODE_NAME + getPort();
  }

  /* Abstract methods, which must be implemented in ConfigNode and DataNode. */
  public void reloadMutableFields() {}
  ;

  public void renameFile() {}
  ;

  public String getSystemConfigPath() {
    return "";
  }
  ;

  /** Return the node config file path specified through system variable */
  public String getDefaultNodeConfigPath() {
    return "";
  }
  ;

  /** Return the common config file path specified through system variable */
  public String getDefaultCommonConfigPath() {
    return "";
  }
  ;

  public void addStartCmdParams(List<String> params) {}
  ;

  public String getSystemPropertiesPath() {
    return "";
  }
  ;

  public MppJVMConfig initVMConfig() {
    return null;
  }
  ;
}
