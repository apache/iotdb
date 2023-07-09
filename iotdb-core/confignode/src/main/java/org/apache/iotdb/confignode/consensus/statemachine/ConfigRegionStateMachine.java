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
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** StateMachine for ConfigRegion. */
public class ConfigRegionStateMachine
    implements IStateMachine, IStateMachine.EventApi, IStateMachine.RetryPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRegionStateMachine.class);

  private static final ExecutorService threadPool =
      IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.CONFIG_NODE_RECOVER.getName());
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private final ConfigPlanExecutor executor;
  private ConfigManager configManager;

  /** Variables for ConfigNode Simple Consensus. */
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

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  protected TSStatus write(ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executeNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error(e.getMessage());
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      writeLogForSimpleConsensus(plan);
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
  public DataSet read(IConsensusRequest request) {
    ConfigPhysicalPlan plan;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        plan = ConfigPhysicalPlan.Factory.create(request.serializeToByteBuffer());
      } catch (Exception e) {
        LOGGER.error("Deserialization error for write plan : {}", request);
        return null;
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      plan = (ConfigPhysicalPlan) request;
    } else {
      LOGGER.error("Unexpected read plan : {}", request);
      return null;
    }
    return read(plan);
  }

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  protected DataSet read(ConfigPhysicalPlan plan) {
    DataSet result;
    try {
      result = executor.executeQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error(e.getMessage());
      result = null;
    }
    return result;
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return executor.takeSnapshot(snapshotDir);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    executor.loadSnapshot(latestSnapshotRootDir);
  }

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the ConfigRegionStateMachine
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();

    if (currentNodeId == newLeaderId) {
      LOGGER.info(
          "Current node [nodeId: {}, ip:port: {}] becomes Leader",
          newLeaderId,
          currentNodeTEndPoint);

      // Always start load services first
      configManager.getLoadManager().startLoadServices();

      // Start leader scheduling services
      configManager.getProcedureManager().shiftExecutor(true);
      configManager.getRetryFailedTasksThread().startRetryFailedTasksService();
      configManager.getPartitionManager().startRegionCleaner();

      // we do cq recovery async for two reasons:
      // 1. For performance: cq recovery may be time-consuming, we use another thread to do it in
      // make notifyLeaderChanged not blocked by it
      // 2. For correctness: in cq recovery processing, it will use ConsensusManager which may be
      // initialized after notifyLeaderChanged finished
      threadPool.submit(() -> configManager.getCQManager().startCQScheduler());

      threadPool.submit(
          () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeMetaSync());
      threadPool.submit(
          () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeHeartbeat());
    } else {
      LOGGER.info(
          "Current node [nodeId:{}, ip:port: {}] is not longer the leader, "
              + "the new leader is [nodeId:{}]",
          currentNodeId,
          currentNodeTEndPoint,
          newLeaderId);

      // Stop leader scheduling services
      configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeMetaSync();
      configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeHeartbeat();
      configManager.getLoadManager().stopLoadServices();
      configManager.getProcedureManager().shiftExecutor(false);
      configManager.getRetryFailedTasksThread().stopRetryFailedTasksService();
      configManager.getPartitionManager().stopRegionCleaner();
      configManager.getCQManager().stopCQScheduler();
      configManager.getClusterSchemaManager().clearSchemaQuotaCache();
    }
  }

  @Override
  public void start() {
    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      initStandAloneConfigNode();
    }
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  /** TODO optimize the lock usage. */
  private synchronized void writeLogForSimpleConsensus(ConfigPhysicalPlan plan) {
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
          // read and re-serialize the PhysicalPlan
          ConfigPhysicalPlan nextPlan = logReader.next();
          try {
            executor.executeNonQueryPlan(nextPlan);
          } catch (UnknownPhysicalPlanTypeException | AuthException e) {
            LOGGER.error(e.getMessage());
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

  private void createLogFile(int endIndex) {
    simpleLogFile = SystemFileFactory.INSTANCE.getFile(PROGRESS_FILE_PATH + endIndex);
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
}
