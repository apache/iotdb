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
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.lease.DataNodeContactTracker;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.persistence.schema.ConfigNodeSnapshotParser;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.confignode.writelog.io.SingleFileLogReader;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** {@link IStateMachine} for ConfigRegion. */
public class ConfigRegionStateMachine implements IStateMachine, IStateMachine.EventApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRegionStateMachine.class);

  /**
   * Serializes leadership transitions (become-leader / resign-leader). A single worker thread is
   * the barrier that keeps epochs strictly serial: the orchestration of one transition runs to
   * completion before the next one begins.
   */
  private static final ExecutorService leaderServicesTransitionExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.CONFIG_NODE_LEADER_SERVICES_TRANSITION.getName());

  /** Runs the individual leader services in parallel within a single become-leader epoch. */
  private static final ExecutorService leaderServicesStartupPool =
      IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.CONFIG_NODE_RECOVER.getName());

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long WAIT_LOAD_READY_TIMEOUT_MS =
      CommonDescriptor.getInstance().getConfig().getCnConnectionTimeoutInMS() / 2;
  private static final long WAIT_LOAD_READY_INTERVAL_MS = 100;
  private final ConfigPlanExecutor executor;

  /**
   * Whether the leader services of the {@link #leaderServicesEpoch current epoch} have finished
   * starting up. Read by {@link ConsensusManager#confirmLeader()} to gate external serving.
   */
  private final AtomicBoolean leaderServicesReady;

  /**
   * Monotonically increasing leadership generation. Every become-leader / resign-leader transition
   * bumps it, so any work submitted for an older epoch can detect it is stale and bail out.
   */
  private final AtomicLong leaderServicesEpoch;

  /** Guards {@link #leaderServicesReady} and {@link #leaderServicesEpoch} as a unit. */
  private final Object leaderServicesLock;

  private ConfigManager configManager;

  /** Variables for {@link ConfigNode} Simple Consensus. */
  private LogWriter simpleLogWriter;

  private File simpleLogFile;
  private int startIndex;
  private int endIndex;

  private static final String CURRENT_FILE_DIR =
      ConsensusManager.getConfigRegionDir() + File.separator + "current";
  private static final String LOG_INPROGRESS_FILE_PREFIX = "log_inprogress_";
  private static final String LOG_FILE_PREFIX = "log_";
  private static final String PROGRESS_FILE_PATH =
      CURRENT_FILE_DIR + File.separator + LOG_INPROGRESS_FILE_PREFIX;
  private static final String FILE_PATH = CURRENT_FILE_DIR + File.separator + LOG_FILE_PREFIX;
  private static final long LOG_FILE_MAX_SIZE =
      CONF.getConfigNodeSimpleConsensusLogSegmentSizeMax();
  private final TEndPoint currentNodeTEndPoint;

  public ConfigRegionStateMachine(ConfigManager configManager, ConfigPlanExecutor executor) {
    this.executor = executor;
    this.leaderServicesReady = new AtomicBoolean(false);
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
    return request == null
        ? new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
        : write((ConfigPhysicalPlan) request);
  }

  /** Transmit {@link ConfigPhysicalPlan} to {@link ConfigPlanExecutor} */
  protected TSStatus write(ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executeNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException e) {
      LOGGER.error(ConfigNodeMessages.EXECUTE_NON_QUERY_PLAN_FAILED, e);
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
            ConfigNodeMessages.DESERIALIZATION_ERROR_FOR_WRITE_PLAN_REQUEST_BYTEBUFFER,
            request,
            request.serializeToByteBuffer(),
            e);
        return null;
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      result = request;
    } else {
      LOGGER.error(
          ConfigNodeMessages.UNEXPECTED_WRITE_PLAN_REQUEST_BYTEBUFFER,
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
      LOGGER.error(ConfigNodeMessages.UNEXPECTED_READ_PLAN, request);
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
      LOGGER.error(ConfigNodeMessages.EXECUTE_QUERY_PLAN_FAILED, e);
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
            .tryListenToSnapshots(ConfigNodeSnapshotParser.getSnapshots());
        return true;
      } catch (IOException e) {
        if (PipeConfigNodeAgent.runtime().listener().isOpened()) {
          LOGGER.warn(
              ConfigNodeMessages
                  .CONFIG_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_THE_HISTORICAL,
              e);
        }
      }
    }
    return false;
  }

  @Override
  public boolean loadSnapshot(final File latestSnapshotRootDir) {
    // The boolean result must reflect whether the ConfigRegion state-machine data was loaded, so
    // callers (e.g. the AddPeer flow) can detect a real failure. The pipe-listener recomputation
    // below is best-effort post-processing: a failure there is logged but must NOT be reported as a
    // snapshot-load failure, otherwise it would (e.g.) abort ConfigNode (re)initialization on what
    // is actually a healthy data load.
    final boolean loadSucceeded = executor.loadSnapshot(latestSnapshotRootDir);
    try {
      // We recompute the snapshot for pipe listener when loading snapshot
      // to recover the newest snapshot in cache
      PipeConfigNodeAgent.runtime()
          .listener()
          .tryListenToSnapshots(ConfigNodeSnapshotParser.getSnapshots());
    } catch (final IOException e) {
      if (PipeConfigNodeAgent.runtime().listener().isOpened()) {
        LOGGER.warn(
            ConfigNodeMessages.CONFIG_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_WHEN_STARTUP,
            e);
      }
    }
    return loadSucceeded;
  }

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the ConfigRegionStateMachine
    final int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    if (currentNodeId != newLeaderId) {
      LOGGER.info(
          ConfigNodeMessages.CURRENT_NODE_NODEID_IP_PORT_IS_NO_LONGER_THE_LEADER
              + ConfigNodeMessages.LOG_NEW_LEADER_NODEID_ARG_0A63760B,
          currentNodeId,
          currentNodeTEndPoint,
          newLeaderId);
      resignLeaderAsync();
    }
  }

  @Override
  public void notifyNotLeader() {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the ConfigRegionStateMachine
    final int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    LOGGER.info(
        ConfigNodeMessages.CURRENT_NODE_NODEID_IP_PORT_IS_NO_LONGER_THE_LEADER
            + ConfigNodeMessages.LOG_START_CLEANING_UP_RELATED_SERVICES_A409E261,
        currentNodeId,
        currentNodeTEndPoint);
    resignLeaderAsync();
  }

  @Override
  public void notifyLeaderReady() {
    LOGGER.info(
        ConfigNodeMessages.CURRENT_NODE_NODEID_IP_PORT_BECOMES_CONFIG_REGION_LEADER,
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        currentNodeTEndPoint);

    // Reset every DataNode's last-contact time to now on (re)acquiring leadership: a stale
    // timestamp
    // left from a previous leadership term (while another ConfigNode was contacting the DataNodes)
    // would otherwise let the metadata-broadcast verdict wrongly judge a live DataNode as fenced.
    DataNodeContactTracker.getInstance()
        .onLeadershipAcquired(
            configManager.getNodeManager().getRegisteredDataNodeLocations().keySet());

    // Bump the epoch eagerly so that any in-flight services of an older epoch are invalidated
    // immediately, even before the (serialized) become-leader orchestration gets to run.
    final long epoch = nextLeaderServicesEpoch();
    leaderServicesTransitionExecutor.submit(() -> becomeLeader(epoch));
  }

  /**
   * Submit a resign-leader transition. The epoch is bumped eagerly (on the consensus thread) so
   * that stale leader work is invalidated at once, while the teardown itself is serialized behind
   * any in-flight transition on {@link #leaderServicesTransitionExecutor}.
   */
  private void resignLeaderAsync() {
    invalidateLeaderServices();
    leaderServicesTransitionExecutor.submit(this::stopLeaderServices);
  }

  /**
   * Bring up the leader services for {@code epoch}. Runs on the single transition thread, so it is
   * strictly serialized against every other transition. Within the epoch, the load services start
   * first (to warm up as early as possible), then the remaining services start in parallel and are
   * joined before the epoch is marked ready.
   */
  private void becomeLeader(final long epoch) {
    if (!isCurrentLeaderServicesEpoch(epoch)) {
      LOGGER.info(
          ConfigNodeMessages
              .MESSAGE_CURRENT_NODE_NODEID_ARG_IP_PORT_ARG_IS_NO_LONGER_THE_LEADER_SKIP_STARTING_LEADER_SERVICES_BECAUSE_THE_LEADER_EPOCH_IS_STALE_CCF39435,
          ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
          currentNodeTEndPoint);
      return;
    }

    // Always start load services first. ConsensusManager gates external serving until warm-up.
    configManager.getLoadManager().startLoadServices();
    if (CONF.isEnableTopologyProbing()) {
      configManager.getLoadManager().startTopologyService();
    }

    // Start the remaining leader services in parallel and wait for all of them to finish. Each
    // startup swallows and logs its own failure (see startInParallelIfEpochCurrent), so a single
    // misbehaving service cannot abort the whole transition and leave the node stuck warming up.
    final CompletableFuture<?>[] startups =
        leaderServiceStartups().stream()
            .map(startup -> startInParallelIfEpochCurrent(epoch, startup))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(startups).join();

    if (!isCurrentLeaderServicesEpoch(epoch)) {
      return;
    }
    // The procedure executor may report readiness asynchronously once it has caught up.
    configManager
        .getProcedureManager()
        .startExecutor(() -> markLeaderServicesReadyIfEpochCurrent(epoch));
    markLeaderServicesReadyIfEpochCurrent(epoch);

    final boolean loadReady = waitForLoadReady(epoch);
    if (!isCurrentLeaderServicesEpoch(epoch)) {
      return;
    }
    logLoadWarmUpIfNeeded(loadReady);
    LOGGER.info(
        ConfigNodeMessages.CURRENT_NODE_NODEID_IP_PORT_AS_CONFIG_REGION_LEADER_IS,
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        currentNodeTEndPoint);
  }

  /** The leader services that can be started independently, in parallel, within one epoch. */
  private List<LeaderServiceStartup> leaderServiceStartups() {
    return Arrays.asList(
        new LeaderServiceStartup(
            "ProcedureInfo.upgrade",
            () -> configManager.getProcedureManager().getStore().getProcedureInfo().upgrade()),
        new LeaderServiceStartup(
            "RetryFailedTasksService",
            () -> configManager.getRetryFailedTasksThread().startRetryFailedTasksService()),
        new LeaderServiceStartup(
            "RegionCleaner", () -> configManager.getPartitionManager().startRegionCleaner()),
        // Add metrics after leader ready.
        new LeaderServiceStartup("Metrics", () -> configManager.addMetrics()),
        // Activate leader related service for config pipe.
        new LeaderServiceStartup(
            "PipeConfigNodeRuntime", () -> PipeConfigNodeAgent.runtime().notifyLeaderReady()),
        // CQ recovery may be time-consuming, so it is just one more parallel startup.
        new LeaderServiceStartup(
            "CQScheduler", () -> configManager.getCQManager().startCQScheduler()),
        new LeaderServiceStartup(
            "PipeMetaSync",
            () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeMetaSync()),
        new LeaderServiceStartup(
            "PipeHeartbeat",
            () -> configManager.getPipeManager().getPipeRuntimeCoordinator().startPipeHeartbeat()),
        new LeaderServiceStartup(
            "PipeOnLeaderChanged",
            () ->
                configManager
                    .getPipeManager()
                    .getPipeRuntimeCoordinator()
                    .onConfigRegionGroupLeaderChanged()),
        new LeaderServiceStartup(
            "SubscriptionMetaSync",
            () ->
                configManager
                    .getSubscriptionManager()
                    .getSubscriptionCoordinator()
                    .startSubscriptionMetaSync()),
        // To adapt old version, we check cluster ID after state machine has been fully recovered.
        new LeaderServiceStartup(
            "CheckClusterId", () -> configManager.getClusterManager().checkClusterId()));
  }

  /** Tear down every leader service. Runs on the single transition thread. */
  private void stopLeaderServices() {
    final int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    // Stop leader scheduling services
    configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeMetaSync();
    configManager.getPipeManager().getPipeRuntimeCoordinator().stopPipeHeartbeat();
    configManager.getSubscriptionManager().getSubscriptionCoordinator().stopSubscriptionMetaSync();
    configManager.getLoadManager().stopTopologyService();
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
        ConfigNodeMessages.CURRENT_NODE_NODEID_IP_PORT_IS_NO_LONGER_THE_LEADER
            + ConfigNodeMessages.LOG_ALL_SERVICES_OLD_LEADER_UNAVAILABLE_NOW_8A22E60F,
        currentNodeId,
        currentNodeTEndPoint);
  }

  /**
   * Run {@code startup} on {@link #leaderServicesStartupPool}, skipping it if the epoch has gone
   * stale by the time it is picked up. Any {@link RuntimeException} thrown by the startup is caught
   * and logged here instead of being allowed to escape: this keeps one misbehaving service from
   * failing the {@link CompletableFuture#allOf} join barrier in {@link #becomeLeader}, which would
   * otherwise abort the whole transition before {@link #markLeaderServicesReadyIfEpochCurrent} runs
   * and leave the node stuck returning {@code CONFIG_NODE_LEADER_WARMING_UP} forever. The returned
   * future therefore always completes normally, so {@code allOf} acts as a clean join barrier.
   */
  private CompletableFuture<Void> startInParallelIfEpochCurrent(
      final long epoch, final LeaderServiceStartup startup) {
    return CompletableFuture.runAsync(
        () -> {
          if (!isCurrentLeaderServicesEpoch(epoch)) {
            return;
          }
          try {
            startup.run();
          } catch (final Exception e) {
            // Swallow and log so a single failed startup cannot stall leader warm-up. The service
            // stays unstarted, but the node still finishes warming up and begins serving; the
            // failure is observable through this error log.
            LOGGER.error(
                ConfigNodeMessages
                    .MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FAILED_TO_START_LEADER_SERVICE_ARG_THE_NODE_WILL_STILL_FINISH_WARMING_UP_THIS_SERVICE_STAYS_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_E89A98E7,
                ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
                currentNodeTEndPoint,
                startup.name(),
                e);
          }
        },
        leaderServicesStartupPool);
  }

  private void markLeaderServicesReadyIfEpochCurrent(final long epoch) {
    synchronized (leaderServicesLock) {
      if (isCurrentLeaderServicesEpoch(epoch)) {
        leaderServicesReady.set(true);
      }
    }
  }

  private void logLoadWarmUpIfNeeded(final boolean loadReady) {
    if (!loadReady) {
      LOGGER.info(
          ConfigNodeMessages
              .MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_WARM_UP_IS_STILL_IN_PROGRESS_ARG_17C09A31,
          ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
          currentNodeTEndPoint,
          configManager.getLoadManager().getLoadReadyReason());
    }
  }

  private boolean waitForLoadReady(final long epoch) {
    long startTime = System.currentTimeMillis();
    while (isCurrentLeaderServicesEpoch(epoch)
        && System.currentTimeMillis() - startTime < WAIT_LOAD_READY_TIMEOUT_MS) {
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
      TimeUnit.MILLISECONDS.sleep(WAIT_LOAD_READY_INTERVAL_MS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          ConfigNodeMessages
              .MESSAGE_UNEXPECTED_INTERRUPTION_WHILE_WAITING_FOR_CONFIGNODE_LEADER_LOAD_WARM_UP_BB5AA4F7,
          e);
      return false;
    }
  }

  public boolean areLeaderServicesReady() {
    return leaderServicesReady.get();
  }

  /** Open a new leadership generation, invalidating the previous one. */
  private long nextLeaderServicesEpoch() {
    synchronized (leaderServicesLock) {
      leaderServicesReady.set(false);
      return leaderServicesEpoch.incrementAndGet();
    }
  }

  /** Invalidate the current leadership generation without opening a serving one. */
  private void invalidateLeaderServices() {
    synchronized (leaderServicesLock) {
      leaderServicesReady.set(false);
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
        LOGGER.error(
            ConfigNodeMessages.CAN_T_FORCE_LOGWRITER_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE, e);
      }
      for (int retry = 0; retry < 5; retry++) {
        try {
          simpleLogWriter.close();
        } catch (IOException e) {
          LOGGER.warn(
              ConfigNodeMessages.CAN_T_CLOSE_STANDALONELOG_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE
                  + ConfigNodeMessages.LOG_FILEPATH_ARG_RETRY_ARG_16284354,
              simpleLogFile.getAbsolutePath(),
              retry);
          try {
            // Sleep 1s and retry
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            LOGGER.warn(
                ConfigNodeMessages.UNEXPECTED_INTERRUPTION_DURING_THE_CLOSE_METHOD_OF_LOGWRITER);
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
          ConfigNodeMessages
              .CAN_T_SERIALIZE_CURRENT_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE,
          e);
    }
  }

  private void initStandAloneConfigNode() {
    File dir = new File(CURRENT_FILE_DIR);
    dir.mkdirs();
    String[] list = new File(CURRENT_FILE_DIR).list();
    if (list != null && list.length != 0) {
      Arrays.sort(list, Comparator.comparingLong(ConfigRegionStateMachine::parseEndIndex));
      for (String logFileName : list) {
        File logFile =
            SystemFileFactory.INSTANCE.getFile(CURRENT_FILE_DIR + File.separator + logFileName);
        SingleFileLogReader logReader;
        try {
          logReader = new SingleFileLogReader(logFile);
        } catch (FileNotFoundException e) {
          LOGGER.error(
              ConfigNodeMessages
                  .INITSTANDALONECONFIGNODE_MEETS_ERROR_CAN_T_FIND_STANDALONE_LOG_FILES_FILEPATH,
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
            LOGGER.error(ConfigNodeMessages.TRY_LISTEN_TO_PLAN_FAILED, e);
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
        LOGGER.error(
            ConfigNodeMessages.CAN_T_FORCE_LOGWRITER_FOR_CONFIGNODE_FLUSHWALFORSIMPLECONSENSUS, e);
      }
    }
  }

  private void createLogFile(int startIndex) {
    simpleLogFile = SystemFileFactory.INSTANCE.getFile(PROGRESS_FILE_PATH + startIndex);
    try {
      if (!simpleLogFile.createNewFile()) {
        LOGGER.warn(
            ConfigNodeMessages.CONFIGNODE_SIMPLECONSENSUSFILE_HAS_EXISTED_FILEPATH,
            simpleLogFile.getAbsolutePath());
      }
      simpleLogWriter = new LogWriter(simpleLogFile, false);
      LOGGER.info(
          ConfigNodeMessages.CREATE_CONFIGNODE_SIMPLECONSENSUSFILE,
          simpleLogFile.getAbsolutePath());
    } catch (Exception e) {
      LOGGER.warn(
          ConfigNodeMessages.CREATE_CONFIGNODE_SIMPLECONSENSUSFILE_FAILED_FILEPATH,
          simpleLogFile.getAbsolutePath(),
          e);
    }
  }

  private static long parseEndIndex(String filename) {
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

    if (endIndexString.isEmpty()) {
      return 0;
    }
    for (int i = 0; i < endIndexString.length(); i++) {
      if (!Character.isDigit(endIndexString.charAt(i))) {
        return 0;
      }
    }
    return Long.parseLong(endIndexString);
  }

  /**
   * A single leader service startup paired with a human-readable name, so a failure can be logged
   * against the service that produced it (see {@link #startInParallelIfEpochCurrent}).
   */
  private static class LeaderServiceStartup {

    private final String name;
    private final Runnable startup;

    private LeaderServiceStartup(final String name, final Runnable startup) {
      this.name = name;
      this.startup = startup;
    }

    private String name() {
      return name;
    }

    private void run() {
      startup.run();
    }
  }
}
