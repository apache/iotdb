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

package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.persistence.schema.ConfignodeSnapshotParser;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.confignode.writelog.io.SingleFileLogReader;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.utils.writelog.LogWriter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@link IStateMachine} for ConfigRegion. */
public class ConfigRegionStateMachine implements IStateMachine, IStateMachine.EventApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRegionStateMachine.class);

  private static final ExecutorService threadPool =
      IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.CONFIG_NODE_RECOVER.getName());
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private final ConfigPlanExecutor executor;
  private ConfigManager configManager;

  /** Variables for {@link ConfigNode} Simple Consensus. */
  private LogWriter simpleLogWriter;

  private File simpleLogFile;
  private int startIndex;
  private int endIndex;

  private static final String CURRENT_FILE_DIR =
      ConsensusManager.getConfigRegionDir() + File.separator + "current";
  private static final String PROGRESS_FILE_PATH =
      CURRENT_FILE_DIR + File.separator + "log_inprogress_";
  private static final String FILE_PATH = CURRENT_FILE_DIR + File.separator + "log_";
  private static final long LOG_FILE_MAX_SIZE =
      CONF.getConfigNodeSimpleConsensusLogSegmentSizeMax();
  private final TEndPoint currentNodeTEndPoint;
  private static Pattern LOG_INPROGRESS_PATTERN = Pattern.compile("\\d+");
  private static Pattern LOG_PATTERN = Pattern.compile("(?<=_)(\\d+)$");

  public ConfigRegionStateMachine(ConfigManager configManager, ConfigPlanExecutor executor) {
    this.executor = executor;
    this.configManager = configManager;
    this.currentNodeTEndPoint =
        new TEndPoint()
            .setIp(ConfigNodeDescriptor.getInstance().getConf().getInternalAddress())
            .setPort(ConfigNodeDescriptor.getInstance().getConf().getConsensusPort());
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  public void setConfigManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    return Optional.ofNullable(request)
        .map(o -> write((ConfigPhysicalPlan) request))
        .orElseGet(() -> new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()));
  }

  /** Transmit {@link ConfigPhysicalPlan} to {@link ConfigPlanExecutor} */
  protected TSStatus write(ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executeNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException e) {
      LOGGER.error("Execute non-query plan failed", e);
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      writeLogForSimpleConsensus(plan);
    }

    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      PipeConfigNodeAgent.runtime().listener().tryListenToPlan(plan, false);
    }

    return result;
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    IConsensusRequest result;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        result = ConfigPhysicalPlan.Factory.create(request.serializeToByteBuffer());
      } catch (Exception e) {
        LOGGER.error(
            "Deserialization error for write plan, request: {}, bytebuffer: {}",
            request,
            request.serializeToByteBuffer(),
            e);
        return null;
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      result = request;
    } else {
      LOGGER.error(
          "Unexpected write plan, request: {}, bytebuffer: {}",
          request,
          request.serializeToByteBuffer());
      return null;
    }
    return result;
  }

  @Override
  public DataSet read(final IConsensusRequest request) {
    final ConfigPhysicalReadPlan plan;
    if (request instanceof ConfigPhysicalReadPlan) {
      plan = (ConfigPhysicalReadPlan) request;
    } else {
      LOGGER.error("Unexpected read plan : {}", request);
      return null;
    }
    return read(plan);
  }

  /** Transmit {@link ConfigPhysicalReadPlan} to {@link ConfigPlanExecutor} */
  protected DataSet read(final ConfigPhysicalReadPlan plan) {
    DataSet result;
    try {
      result = executor.executeQueryPlan(plan);
    } catch (final UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error("Execute query plan failed", e);
      result = null;
    }
    return result;
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    if (executor.takeSnapshot(snapshotDir)) {
      try {
        PipeConfigNodeAgent.runtime()
            .listener()
            .tryListenToSnapshots(ConfignodeSnapshotParser.getSnapshots());
        return true;
      } catch (IOException e) {
        if (PipeConfigNodeAgent.runtime().listener().isOpened()) {
          LOGGER.warn(
              "Config Region Listening Queue Listen to snapshot failed, the historical data may not be transferred.",
              e);
        }
      }
    }
    return false;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    try {
      executor.loadSnapshot(latestSnapshotRootDir);
      // We recompute the snapshot for pipe listener when loading snapshot
      // to recover the newest snapshot in cache
      PipeConfigNodeAgent.runtime()
          .listener()
          .tryListenToSnapshots(ConfignodeSnapshotParser.getSnapshots());
    } catch (IOException e) {
      if (PipeConfigNodeAgent.runtime().listener().isOpened()) {
        LOGGER.warn(
            "Config Region Listening Queue Listen to snapshot failed when startup, snapshot will be tried again when starting schema transferring pipes",
            e);
      }
    }
  }

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the ConfigRegionStateMachine
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    if (currentNodeId != newLeaderId) {
      LOGGER.info(
          "Current node [nodeId:{}, ip:port: {}] is no longer the leader, "
              + "the new leader is [nodeId:{}]",
          currentNodeId,
          currentNodeTEndPoint,
          newLeaderId);
    }
  }

  @Override
  public void notifyNotLeader() {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the ConfigRegionStateMachine
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    LOGGER.info(
        "Current node [nodeId:{}, ip:port: {}] is no longer the leader, "
            + "start cleaning up related services",
        currentNodeId,
        currentNodeTEndPoint);
    // Stop leader scheduling services
    configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeMetaSync();
    configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeHeartbeat();
    configManager.getSubscriptionManager().getSubscriptionCoordinator().stopSubscriptionMetaSync();
    configManager.getLoadManager().stopLoadServices();
    configManager.getProcedureManager().stopExecutor();
    configManager.getRetryFailedTasksThread().stopRetryFailedTasksService();
    configManager.getPartitionManager().stopRegionCleaner();
    configManager.getCQManager().stopCQScheduler();
    configManager.getClusterSchemaManager().clearSchemaQuotaCache();
    // Remove Metric after leader change
    configManager.removeMetrics();

    // Shutdown leader related service for config pipe
    PipeConfigNodeAgent.runtime().notifyLeaderUnavailable();

    // Clean receiver file dir
    PipeConfigNodeAgent.receiver().cleanPipeReceiverDir();

    LOGGER.info(
        "Current node [nodeId:{}, ip:port: {}] is no longer the leader, "
            + "all services on old leader are unavailable now.",
        currentNodeId,
        currentNodeTEndPoint);
  }

  @Override
  public void notifyLeaderReady() {
    LOGGER.info(
        "Current node [nodeId: {}, ip:port: {}] becomes config region leader",
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        currentNodeTEndPoint);

    // Always start load services first
    configManager.getLoadManager().startLoadServices();

    // Start leader scheduling services
    configManager.getProcedureManager().startExecutor();
    threadPool.submit(
        () -> configManager.getProcedureManager().getStore().getProcedureInfo().upgrade());
    configManager.getRetryFailedTasksThread().startRetryFailedTasksService();
    configManager.getPartitionManager().startRegionCleaner();
    configManager.checkUserPathPrivilege();
    // Add Metric after leader ready
    configManager.addMetrics();

    // Activate leader related service for config pipe
    PipeConfigNodeAgent.runtime().notifyLeaderReady();

    // we do cq recovery async for performance:
    // cq recovery may be time-consuming, we use another thread to do it in
    // make notifyLeaderChanged not blocked by it
    threadPool.submit(() -> configManager.getCQManager().startCQScheduler());

    threadPool.submit(
        () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeMetaSync());
    threadPool.submit(
        () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeHeartbeat());
    threadPool.submit(
        () ->
            configManager
                .getPipeManager()
                .getPipeRuntimeCoordinator()
                .onConfigRegionGroupLeaderChanged());

    threadPool.submit(
        () ->
            configManager
                .getSubscriptionManager()
                .getSubscriptionCoordinator()
                .startSubscriptionMetaSync());

    // To adapt old version, we check cluster ID after state machine has been fully recovered.
    // Do check async because sync will be slow and block every other things.
    threadPool.submit(() -> configManager.getClusterManager().checkClusterId());

    LOGGER.info(
        "Current node [nodeId: {}, ip:port: {}] as config region leader is ready to work",
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        currentNodeTEndPoint);
  }

  @Override
  public void start() {
    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      initStandAloneConfigNode();
    }
  }

  @Override
  public void stop() {
    // Shutdown leader related service for config pipe
    PipeConfigNodeAgent.runtime().notifyLeaderUnavailable();
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  private void writeLogForSimpleConsensus(ConfigPhysicalPlan plan) {
    if (simpleLogFile.length() > LOG_FILE_MAX_SIZE) {
      try {
        simpleLogWriter.force();
        File completedFilePath = new File(FILE_PATH + startIndex + "_" + endIndex);
        Files.move(
            simpleLogFile.toPath(), completedFilePath.toPath(), StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException e) {
        LOGGER.error("Can't force logWriter for ConfigNode SimpleConsensus mode", e);
      }
      for (int retry = 0; retry < 5; retry++) {
        try {
          simpleLogWriter.close();
        } catch (IOException e) {
          LOGGER.warn(
              "Can't close StandAloneLog for ConfigNode SimpleConsensus mode, "
                  + "filePath: {}, retry: {}",
              simpleLogFile.getAbsolutePath(),
              retry);
          try {
            // Sleep 1s and retry
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Unexpected interruption during the close method of logWriter");
          }
          continue;
        }
        break;
      }
      startIndex = endIndex + 1;
      createLogFile(startIndex);
    }

    try {
      ByteBuffer buffer = plan.serializeToByteBuffer();
      buffer.position(buffer.limit());
      simpleLogWriter.write(buffer);

      endIndex = endIndex + 1;
    } catch (Exception e) {
      LOGGER.error(
          "Can't serialize current ConfigPhysicalPlan for ConfigNode SimpleConsensus mode", e);
    }
  }

  private void initStandAloneConfigNode() {
    File dir = new File(CURRENT_FILE_DIR);
    dir.mkdirs();
    String[] list = new File(CURRENT_FILE_DIR).list();
    if (list != null && list.length != 0) {
      Arrays.sort(list, new FileComparator());
      for (String logFileName : list) {
        File logFile =
            SystemFileFactory.INSTANCE.getFile(CURRENT_FILE_DIR + File.separator + logFileName);
        SingleFileLogReader logReader;
        try {
          logReader = new SingleFileLogReader(logFile);
        } catch (FileNotFoundException e) {
          LOGGER.error(
              "InitStandAloneConfigNode meets error, can't find standalone log files, filePath: {}",
              logFile.getAbsolutePath(),
              e);
          continue;
        }

        startIndex = endIndex;
        while (logReader.hasNext()) {
          endIndex++;
          // Read and re-serialize the PhysicalPlan
          ConfigPhysicalPlan nextPlan = logReader.next();
          try {
            TSStatus status = executor.executeNonQueryPlan(nextPlan);
            if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              // Recover the linked queue.
              // Note that the "nextPlan"s may contain create and drop pipe operations
              // and will affect whether the queue listen to the plans.
              PipeConfigNodeAgent.runtime().listener().tryListenToPlan(nextPlan, false);
            }
          } catch (UnknownPhysicalPlanTypeException e) {
            LOGGER.error("Try listen to plan failed", e);
          }
        }
        logReader.close();
      }
    } else {
      startIndex = 0;
      endIndex = 0;
    }
    startIndex = startIndex + 1;
    createLogFile(endIndex);

    ScheduledExecutorService simpleConsensusThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.CONFIG_NODE_SIMPLE_CONSENSUS_WAL_FLUSH.getName());
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        simpleConsensusThread,
        this::flushWALForSimpleConsensus,
        0,
        CONF.getForceWalPeriodForConfigNodeSimpleInMs(),
        TimeUnit.MILLISECONDS);
  }

  private void flushWALForSimpleConsensus() {
    if (simpleLogWriter != null) {
      try {
        simpleLogWriter.force();
      } catch (IOException e) {
        LOGGER.error("Can't force logWriter for ConfigNode flushWALForSimpleConsensus", e);
      }
    }
  }

  private void createLogFile(int startIndex) {
    simpleLogFile = SystemFileFactory.INSTANCE.getFile(PROGRESS_FILE_PATH + startIndex);
    try {
      if (!simpleLogFile.createNewFile()) {
        LOGGER.warn(
            "ConfigNode SimpleConsensusFile has existed，filePath:{}",
            simpleLogFile.getAbsolutePath());
      }
      simpleLogWriter = new LogWriter(simpleLogFile, false);
      LOGGER.info("Create ConfigNode SimpleConsensusFile: {}", simpleLogFile.getAbsolutePath());
    } catch (Exception e) {
      LOGGER.warn(
          "Create ConfigNode SimpleConsensusFile failed, filePath: {}",
          simpleLogFile.getAbsolutePath(),
          e);
    }
  }

  static class FileComparator implements Comparator<String> {

    @Override
    public int compare(String filename1, String filename2) {
      long id1 = parseEndIndex(filename1);
      long id2 = parseEndIndex(filename2);
      return Long.compare(id1, id2);
    }
  }

  static long parseEndIndex(String filename) {
    if (filename.startsWith("log_inprogress_")) {
      Matcher matcher = LOG_INPROGRESS_PATTERN.matcher(filename);
      if (matcher.find()) {
        return Long.parseLong(matcher.group());
      }
    } else {
      Matcher matcher = LOG_PATTERN.matcher(filename);
      if (matcher.find()) {
        return Long.parseLong(matcher.group());
      }
    }
    return 0;
  }
}
