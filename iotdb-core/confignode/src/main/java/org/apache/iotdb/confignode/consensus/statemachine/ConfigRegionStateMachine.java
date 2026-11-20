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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** {@link IStateMachine} for ConfigRegion. */
public class ConfigRegionStateMachine implements IStateMachine, IStateMachine.EventApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRegionStateMachine.class);

  private static final ExecutorService leaderServicesTransitionExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.CONFIG_NODE_LEADER_SERVICES_TRANSITION.getName());
  private static final ExecutorService leaderServicesStartupPool =
      IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.CONFIG_NODE_RECOVER.getName());
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private final ConfigPlanExecutor executor;

  /**
   * Readiness of leader services in the {@link #leaderServicesEpoch current epoch}. Recovered
   * procedures can run once the Pipe callback has completed, but external requests must also wait
   * for worker startup to finish.
   */
  private volatile LeaderServicesReadiness leaderServicesReadiness =
      LeaderServicesReadiness.WARMING_UP;

  /**
   * Monotonically increasing leadership generation. Every become-leader / resign-leader transition
   * bumps it, so any work submitted for an older epoch can detect it is stale and bail out.
   */
  private final AtomicLong leaderServicesEpoch;

  /** Guards {@link #leaderServicesReadiness} and {@link #leaderServicesEpoch} as a unit. */
  private final Object leaderServicesLock;

  private enum LeaderServicesReadiness {
    WARMING_UP,
    READY_FOR_PROCEDURES,
    READY
  }

  private ConfigManager configManager;

  /** Variables for {@link ConfigNode} Simple Consensus. */
  private LogWriter simpleLogWriter;

  private File simpleLogFile;
  private int startIndex;
  private int endIndex;
  private boolean simpleConsensusWritesDisabled;

  private static final String CURRENT_FILE_DIR =
      ConsensusManager.getConfigRegionDir() + File.separator + "current";
  private static final String PROGRESS_FILE_PATH =
      CURRENT_FILE_DIR + File.separator + "log_inprogress_";
  private static final String FILE_PATH = CURRENT_FILE_DIR + File.separator + "log_";
  private static final String LOG_INPROGRESS_FILE_PREFIX = "log_inprogress_";
  private static final String LOG_FILE_PREFIX = "log_";
  private static final long LOG_FILE_MAX_SIZE =
      CONF.getConfigNodeSimpleConsensusLogSegmentSizeMax();
  private final TEndPoint currentNodeTEndPoint;

  public ConfigRegionStateMachine(ConfigManager configManager, ConfigPlanExecutor executor) {
    this.executor = executor;
    this.leaderServicesEpoch = new AtomicLong(0);
    this.leaderServicesLock = new Object();
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
    SimpleConsensusPersistResult persistResult = null;
    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      persistResult = persistPlanForSimpleConsensus(plan);
      final TSStatus persistStatus = persistResult.status;
      if (persistStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return persistStatus;
      }
    }

    TSStatus result;
    try {
      result = executor.executeNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | RuntimeException e) {
      LOGGER.error(ConfigNodeMessages.EXECUTE_NON_QUERY_PLAN_FAILED, e);
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    if (persistResult != null
        && result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && !rollbackFailedPlanForSimpleConsensus(plan, persistResult)) {
      final String rollbackFailureMessage =
          ConfigNodeMessages.FAILED_TO_ROLLBACK_PERSISTED_CONFIGNODE_SIMPLECONSENSUS_LOG;
      result.setMessage(
          Optional.ofNullable(result.getMessage())
              .map(message -> message + " " + rollbackFailureMessage)
              .orElse(rollbackFailureMessage));
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
      } catch (IOException e) {
        if (PipeConfigNodeAgent.runtime().listener().isOpened()) {
          LOGGER.warn(
              "Config Region Listening Queue Listen to snapshot failed, the historical data may not be transferred.",
              e);
        }
      }
      return true;
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
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    if (currentNodeId != newLeaderId) {
      LOGGER.info(
          ConfigNodeMessages.LOG_CONFIGNODE_ARG_AT_ARG_IS_NO_LONGER_LEADER_NEW_LEADER_ARG_5C41D7D7,
          currentNodeId,
          currentNodeTEndPoint,
          newLeaderId);
      resignLeaderAsync();
    }
  }

  @Override
  public void notifyNotLeader() {
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    LOGGER.info(
        ConfigNodeMessages.LOG_CONFIGNODE_ARG_AT_ARG_IS_NO_LONGER_LEADER_CLEANING_SERVICES_37CA9A1A,
        currentNodeId,
        currentNodeTEndPoint);
    resignLeaderAsync();
  }

  @Override
  public void notifyLeaderReady() {
    LOGGER.info(
        ConfigNodeMessages.LOG_CONFIGNODE_ARG_AT_ARG_BECOMES_CONFIG_REGION_LEADER_2EB9B9E2,
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        currentNodeTEndPoint);
    long epoch = nextLeaderServicesEpoch();
    leaderServicesTransitionExecutor.submit(() -> becomeLeader(epoch));
  }

  private void resignLeaderAsync() {
    invalidateLeaderServices();
    leaderServicesTransitionExecutor.submit(this::stopLeaderServices);
  }

  private void becomeLeader(final long epoch) {
    if (!isCurrentLeaderServicesEpoch(epoch)) {
      LOGGER.info(
          ConfigNodeMessages
              .LOG_SKIP_STARTING_CONFIGNODE_LEADER_SERVICES_BECAUSE_THE_LEADER_EPOCH_IS_STALE_9E4467F6);
      return;
    }

    configManager.getLoadManager().startLoadServices();

    CompletableFuture<?>[] startups =
        leaderServiceStartups().stream()
            .map(startup -> startInParallelIfEpochCurrent(epoch, startup))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(startups).join();

    if (!isCurrentLeaderServicesEpoch(epoch)) {
      return;
    }
    final boolean procedureExecutorWasRunning =
        configManager.getProcedureManager().getExecutor().isRunning();
    configManager
        .getProcedureManager()
        .startExecutor(
            () -> {
              startPipeOnLeaderChangedIfEpochCurrent(epoch);
              markLeaderServicesReadyIfEpochCurrent(
                  epoch, LeaderServicesReadiness.READY_FOR_PROCEDURES);
            });
    if (!isCurrentLeaderServicesEpoch(epoch)) {
      return;
    }
    if (procedureExecutorWasRunning) {
      startPipeOnLeaderChangedIfEpochCurrent(epoch);
    }
    markLeaderServicesReadyIfEpochCurrent(epoch, LeaderServicesReadiness.READY);

    boolean loadReady = waitForLoadReady(epoch);
    if (!isCurrentLeaderServicesEpoch(epoch)) {
      return;
    }
    logLoadWarmUpIfNeeded(loadReady);
    LOGGER.info(
        ConfigNodeMessages.LOG_CONFIGNODE_ARG_AT_ARG_AS_CONFIG_REGION_LEADER_IS_READY_74DFCB26,
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        currentNodeTEndPoint);
  }

  private List<Runnable> leaderServiceStartups() {
    return Arrays.asList(
        () -> configManager.getProcedureManager().getStore().getProcedureInfo().upgrade(),
        () -> configManager.getRetryFailedTasksThread().startRetryFailedTasksService(),
        () -> configManager.getPartitionManager().startRegionCleaner(),
        configManager::checkUserPathPrivilege,
        configManager::addMetrics,
        () -> PipeConfigNodeAgent.runtime().notifyLeaderReady(),
        () -> configManager.getCQManager().startCQScheduler(),
        () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeMetaSync(),
        () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeHeartbeat(),
        () ->
            configManager
                .getSubscriptionManager()
                .getSubscriptionCoordinator()
                .startSubscriptionMetaSync(),
        () -> configManager.getClusterManager().checkClusterId());
  }

  private void stopLeaderServices() {
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeMetaSync();
    configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeHeartbeat();
    configManager.getSubscriptionManager().getSubscriptionCoordinator().stopSubscriptionMetaSync();
    configManager.getLoadManager().stopLoadServices();
    configManager.getProcedureManager().stopExecutor();
    configManager.getRetryFailedTasksThread().stopRetryFailedTasksService();
    configManager.getPartitionManager().stopRegionCleaner();
    configManager.getCQManager().stopCQScheduler();
    configManager.getClusterSchemaManager().clearSchemaQuotaCache();
    configManager.removeMetrics();
    PipeConfigNodeAgent.runtime().notifyLeaderUnavailable();
    PipeConfigNodeAgent.receiver().cleanPipeReceiverDir();
    LOGGER.info(
        ConfigNodeMessages.LOG_CONFIGNODE_ARG_AT_ARG_LEADER_SERVICES_ARE_UNAVAILABLE_0223CF16,
        currentNodeId,
        currentNodeTEndPoint);
  }

  private CompletableFuture<Void> startInParallelIfEpochCurrent(
      final long epoch, final Runnable startup) {
    return CompletableFuture.runAsync(
        () -> {
          if (isCurrentLeaderServicesEpoch(epoch)) {
            startup.run();
          }
        },
        leaderServicesStartupPool);
  }

  private void startPipeOnLeaderChangedIfEpochCurrent(final long epoch) {
    startInParallelIfEpochCurrent(
            epoch,
            () -> {
              try {
                configManager
                    .getPipeManager()
                    .getPipeRuntimeCoordinator()
                    .onConfigRegionGroupLeaderChanged();
              } catch (final Exception e) {
                LOGGER.error(
                    String.format(
                        ConfigNodeMessages
                            .LOG_FAILED_TO_START_CONFIGNODE_LEADER_SERVICE_ON_NODE_ARG_AT_ARG_ARG_THE_NODE_WILL_FINISH_WARMING_UP_BUT_THIS_SERVICE_REMAINS_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_9E0D12F9,
                        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
                        currentNodeTEndPoint,
                        "PipeOnLeaderChanged"),
                    e);
              }
            })
        .join();
  }

  private void markLeaderServicesReadyIfEpochCurrent(
      final long epoch, final LeaderServicesReadiness readiness) {
    synchronized (leaderServicesLock) {
      if (isCurrentLeaderServicesEpoch(epoch)) {
        leaderServicesReadiness = readiness;
      }
    }
  }

  private void logLoadWarmUpIfNeeded(final boolean loadReady) {
    if (!loadReady) {
      LOGGER.info(
          ConfigNodeMessages
              .LOG_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_WARM_UP_IS_STILL_IN_PROGRESS_ARG_17C09A31,
          ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
          currentNodeTEndPoint,
          configManager.getLoadManager().getLoadReadyReason());
    }
  }

  private boolean waitForLoadReady(final long epoch) {
    long startTime = System.currentTimeMillis();
    long timeout = CommonDescriptor.getInstance().getConfig().getCnConnectionTimeoutInMS() / 2;
    while (isCurrentLeaderServicesEpoch(epoch)
        && System.currentTimeMillis() - startTime < timeout) {
      if (configManager.getLoadManager().isLoadReady()) {
        return true;
      }
      if (!sleepForLoadReady()) {
        return false;
      }
    }
    return isCurrentLeaderServicesEpoch(epoch) && configManager.getLoadManager().isLoadReady();
  }

  private boolean sleepForLoadReady() {
    try {
      TimeUnit.MILLISECONDS.sleep(100);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          ConfigNodeMessages
              .LOG_UNEXPECTED_INTERRUPTION_WHILE_WAITING_FOR_CONFIGNODE_LEADER_LOAD_WARM_UP_BB5AA4F7,
          e);
      return false;
    }
  }

  public boolean areLeaderServicesReady(final boolean forInternalProcedure) {
    final LeaderServicesReadiness readiness = leaderServicesReadiness;
    return readiness == LeaderServicesReadiness.READY
        || (forInternalProcedure && readiness == LeaderServicesReadiness.READY_FOR_PROCEDURES);
  }

  private long nextLeaderServicesEpoch() {
    synchronized (leaderServicesLock) {
      leaderServicesReadiness = LeaderServicesReadiness.WARMING_UP;
      return leaderServicesEpoch.incrementAndGet();
    }
  }

  private void invalidateLeaderServices() {
    synchronized (leaderServicesLock) {
      leaderServicesReadiness = LeaderServicesReadiness.WARMING_UP;
      leaderServicesEpoch.incrementAndGet();
    }
  }

  private boolean isCurrentLeaderServicesEpoch(final long epoch) {
    return leaderServicesEpoch.get() == epoch
        && configManager.getConsensusManager().isLeaderReady();
  }

  @Override
  public void start() {
    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      initStandAloneConfigNode();
    }
  }

  @Override
  public void stop() {
    if (ConsensusFactory.SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      closeSimpleLogWriter();
    }
    // Shutdown leader related service for config pipe
    PipeConfigNodeAgent.runtime().notifyLeaderUnavailable();
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  private SimpleConsensusPersistResult persistPlanForSimpleConsensus(ConfigPhysicalPlan plan) {
    File persistedLogFile = null;
    long logFileSizeBeforeWrite = 0;
    int endIndexBeforeWrite = endIndex;
    try {
      if (simpleConsensusWritesDisabled) {
        throw new IOException(
            ConfigNodeMessages
                .EXCEPTION_CONFIGNODE_SIMPLECONSENSUS_WRITES_ARE_DISABLED_AFTER_A_WAL_CLEANUP_FAILURE_RESTART_IS_REQUIRED_D52F0107);
      }
      if (simpleLogWriter == null || simpleLogFile == null) {
        throw new IOException(ConfigNodeMessages.SIMPLECONSENSUS_LOG_WRITER_IS_NOT_INITIALIZED);
      }

      if (simpleLogFile.length() > LOG_FILE_MAX_SIZE) {
        rollSimpleConsensusLogFile();
      }

      persistedLogFile = simpleLogFile;
      logFileSizeBeforeWrite = persistedLogFile.length();
      endIndexBeforeWrite = endIndex;

      ByteBuffer buffer = plan.serializeToByteBuffer();
      buffer.position(buffer.limit());
      simpleLogWriter.write(buffer);
      simpleLogWriter.force();

      endIndex = endIndex + 1;
    } catch (Exception e) {
      LOGGER.error(
          ConfigNodeMessages
              .PERSIST_CURRENT_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE_FAILED,
          e);
      TSStatus status =
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(
                  String.format(
                      ConfigNodeMessages.PERSIST_CONFIGNODE_SIMPLECONSENSUS_LOG_FAILED,
                      e.getMessage()));
      // A failed append or force may still leave a complete or partial record in the WAL.
      if (persistedLogFile != null
          && !rollbackFailedPlanForSimpleConsensus(
              plan,
              SimpleConsensusPersistResult.success(
                  persistedLogFile, logFileSizeBeforeWrite, endIndexBeforeWrite))) {
        status.setMessage(
            status.getMessage()
                + " "
                + ConfigNodeMessages.FAILED_TO_ROLLBACK_PERSISTED_CONFIGNODE_SIMPLECONSENSUS_LOG);
      }
      return SimpleConsensusPersistResult.failure(status);
    }
    return SimpleConsensusPersistResult.success(
        persistedLogFile, logFileSizeBeforeWrite, endIndexBeforeWrite);
  }

  private boolean rollbackFailedPlanForSimpleConsensus(
      ConfigPhysicalPlan plan, SimpleConsensusPersistResult persistResult) {
    try {
      if (!closeSimpleLogWriter()) {
        throw new IOException(
            ConfigNodeMessages
                .EXCEPTION_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_WRITER_BC0F417C);
      }
      try (FileChannel channel =
          FileChannel.open(persistResult.logFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(persistResult.logFileSizeBeforeWrite);
        channel.force(true);
      }
      simpleLogFile = persistResult.logFile;
      simpleLogWriter = new LogWriter(simpleLogFile, false);
      endIndex = persistResult.endIndexBeforeWrite;
      return true;
    } catch (IOException e) {
      // Never append past an entry whose removal could not be made durable.
      simpleConsensusWritesDisabled = true;
      LOGGER.error(
          ConfigNodeMessages
              .ROLLBACK_FAILED_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE_FAILED,
          plan.getType(),
          persistResult.logFile,
          persistResult.logFileSizeBeforeWrite,
          persistResult.endIndexBeforeWrite,
          e);
      return false;
    }
  }

  private void rollSimpleConsensusLogFile() throws IOException {
    simpleLogWriter.force();
    if (!closeSimpleLogWriter()) {
      simpleConsensusWritesDisabled = true;
      throw new IOException(
          ConfigNodeMessages
              .EXCEPTION_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_WRITER_BC0F417C);
    }
    try {
      Files.move(
          simpleLogFile.toPath(),
          simpleLogFile.toPath().resolveSibling(LOG_FILE_PREFIX + startIndex + "_" + endIndex),
          StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      // The source file and indexes are unchanged when renaming fails. Reopen it for retries.
      try {
        simpleLogWriter = new LogWriter(simpleLogFile, false);
      } catch (IOException reopenException) {
        simpleConsensusWritesDisabled = true;
        e.addSuppressed(reopenException);
      }
      throw e;
    }
    startIndex = endIndex + 1;
    createLogFile(startIndex);
  }

  private void initStandAloneConfigNode() {
    File dir = new File(CURRENT_FILE_DIR);
    dir.mkdirs();
    String[] list = new File(CURRENT_FILE_DIR).list();
    endIndex = 0;
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

        final int recoveredStartIndex = parseStartIndex(logFileName);
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
        if (isInProgressLogFile(logFileName)) {
          sealRecoveredInProgressLogFile(logFile, recoveredStartIndex, endIndex);
        }
      }
    }
    startIndex = endIndex + 1;
    createLogFile(startIndex);
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

  private void sealRecoveredInProgressLogFile(
      File logFile, int recoveredStartIndex, int recoveredEndIndex) {
    try {
      if (recoveredStartIndex > recoveredEndIndex) {
        Files.deleteIfExists(logFile.toPath());
        return;
      }
      Files.move(
          logFile.toPath(),
          new File(FILE_PATH + recoveredStartIndex + "_" + recoveredEndIndex).toPath(),
          StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      LOGGER.warn(
          ConfigNodeMessages.SEAL_RECOVERED_CONFIGNODE_SIMPLECONSENSUS_LOG_FAILED, logFile, e);
    }
  }

  private boolean isInProgressLogFile(String filename) {
    return filename.startsWith(LOG_INPROGRESS_FILE_PREFIX);
  }

  private boolean closeSimpleLogWriter() {
    if (simpleLogWriter == null) {
      return true;
    }
    for (int retry = 0; retry < 5; retry++) {
      try {
        simpleLogWriter.close();
        simpleLogWriter = null;
        return true;
      } catch (IOException e) {
        LOGGER.warn(
            ConfigNodeMessages
                .LOG_CONFIGREGION_UPDATE_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_FILEPATH_ARG_RETRY_ARG_188A702E,
            simpleLogFile == null ? null : simpleLogFile.getAbsolutePath(),
            retry);
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
          LOGGER.warn(
              ConfigNodeMessages.UNEXPECTED_INTERRUPTION_DURING_THE_CLOSE_METHOD_OF_LOGWRITER);
          break;
        }
      }
    }
    return false;
  }

  static long parseEndIndex(String filename) {
    final String endIndexString;
    if (filename.startsWith(LOG_INPROGRESS_FILE_PREFIX)) {
      endIndexString = filename.substring(LOG_INPROGRESS_FILE_PREFIX.length());
    } else if (filename.startsWith(LOG_FILE_PREFIX)) {
      final int lastSeparatorIndex = filename.lastIndexOf('_');
      if (lastSeparatorIndex < LOG_FILE_PREFIX.length()) {
        return 0;
      }
      endIndexString = filename.substring(lastSeparatorIndex + 1);
    } else {
      return 0;
    }

    return isDigits(endIndexString) ? Long.parseLong(endIndexString) : 0;
  }

  static int parseStartIndex(String filename) {
    final String startIndexString;
    if (filename.startsWith(LOG_INPROGRESS_FILE_PREFIX)) {
      startIndexString = filename.substring(LOG_INPROGRESS_FILE_PREFIX.length());
    } else if (filename.startsWith(LOG_FILE_PREFIX)) {
      final int lastSeparatorIndex = filename.lastIndexOf('_');
      if (lastSeparatorIndex <= LOG_FILE_PREFIX.length()) {
        return 0;
      }
      startIndexString = filename.substring(LOG_FILE_PREFIX.length(), lastSeparatorIndex);
    } else {
      return 0;
    }

    return isDigits(startIndexString) ? Integer.parseInt(startIndexString) : 0;
  }

  private static boolean isDigits(String value) {
    if (value.isEmpty()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      if (!Character.isDigit(value.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static class SimpleConsensusPersistResult {

    private final TSStatus status;
    private final File logFile;
    private final long logFileSizeBeforeWrite;
    private final int endIndexBeforeWrite;

    private SimpleConsensusPersistResult(
        TSStatus status, File logFile, long logFileSizeBeforeWrite, int endIndexBeforeWrite) {
      this.status = status;
      this.logFile = logFile;
      this.logFileSizeBeforeWrite = logFileSizeBeforeWrite;
      this.endIndexBeforeWrite = endIndexBeforeWrite;
    }

    private static SimpleConsensusPersistResult success(
        File logFile, long logFileSizeBeforeWrite, int endIndexBeforeWrite) {
      return new SimpleConsensusPersistResult(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          logFile,
          logFileSizeBeforeWrite,
          endIndexBeforeWrite);
    }

    private static SimpleConsensusPersistResult failure(TSStatus status) {
      return new SimpleConsensusPersistResult(status, null, 0, 0);
    }
  }
}
