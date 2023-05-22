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

package org.apache.iotdb.consensus.natraft.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.GenericHandler;
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.exception.CheckConsistencyException;
import org.apache.iotdb.consensus.natraft.exception.LogExecutionException;
import org.apache.iotdb.consensus.natraft.exception.UnknownLogTypeException;
import org.apache.iotdb.consensus.natraft.protocol.consistency.CheckConsistency;
import org.apache.iotdb.consensus.natraft.protocol.consistency.MidCheckConsistency;
import org.apache.iotdb.consensus.natraft.protocol.consistency.StrongCheckConsistency;
import org.apache.iotdb.consensus.natraft.protocol.heartbeat.ElectionReqHandler;
import org.apache.iotdb.consensus.natraft.protocol.heartbeat.HeartbeatReqHandler;
import org.apache.iotdb.consensus.natraft.protocol.heartbeat.HeartbeatThread;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.BlockingLogAppender;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.LogAppender;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.LogAppenderFactory;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.SlidingWindowLogAppender.Factory;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.AsyncLogApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.BaseApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.catchup.CatchUpManager;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.LogDispatcher;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.VotingLogList;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.VotingLogList.AcceptedType;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol.FlowBalancer;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol.FlowMonitorManager;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.ConfigChangeEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.RequestEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.CommitLogCallback;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.CommitLogTask;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.DirectorySnapshotRaftLogManager;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization.SyncLogDequeSerializer;
import org.apache.iotdb.consensus.natraft.protocol.log.recycle.EntryAllocator;
import org.apache.iotdb.consensus.natraft.protocol.log.sequencing.LogSequencer;
import org.apache.iotdb.consensus.natraft.protocol.log.sequencing.LogSequencerFactory;
import org.apache.iotdb.consensus.natraft.protocol.log.sequencing.SynchronousSequencer;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.DirectorySnapshot;
import org.apache.iotdb.consensus.natraft.utils.IOUtils;
import org.apache.iotdb.consensus.natraft.utils.LogUtils;
import org.apache.iotdb.consensus.natraft.utils.NodeReport.RaftMemberReport;
import org.apache.iotdb.consensus.natraft.utils.NodeUtils;
import org.apache.iotdb.consensus.natraft.utils.Response;
import org.apache.iotdb.consensus.natraft.utils.StatusUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;
import org.apache.iotdb.consensus.raft.thrift.ElectionRequest;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatRequest;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatResponse;
import org.apache.iotdb.consensus.raft.thrift.RequestCommitIndexResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class RaftMember {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final String CONFIGURATION_TMP_FILE_NAME = "configuration.dat.tmp";
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);

  private RaftConfig config;
  protected final LogAppenderFactory appenderFactory;

  protected static final LogSequencerFactory SEQUENCER_FACTORY = new SynchronousSequencer.Factory();

  private static final String MSG_FORWARD_TIMEOUT = "{}: Forward {} to {} time out";
  private static final String MSG_FORWARD_ERROR =
      "{}: encountered an error when forwarding {} to" + " {}";
  private static final String MSG_NO_LEADER_COMMIT_INDEX =
      "{}: Cannot request commit index from {}";
  private static final String MSG_NO_LEADER_IN_SYNC = "{}: No leader is found when synchronizing";

  /**
   * when the leader of this node changes, the condition will be notified so other threads that wait
   * on this may be woken.
   */
  private final Object waitLeaderCondition = new Object();

  /** the lock is to make sure that only one thread can apply snapshot at the same time */
  private final Lock snapshotApplyLock = new ReentrantLock();
  /**
   * when the commit progress is updated by a heartbeat, this object is notified so that we may know
   * if this node is up-to-date with the leader, and whether the given consistency is reached
   */
  private final Object syncLock = new Object();

  protected Peer thisNode;
  /** the nodes that belong to the same raft group as thisNode. */
  protected volatile List<Peer> allNodes;

  protected volatile List<Peer> newNodes;

  protected ConsensusGroupId groupId;
  protected String name;
  protected String storageDir;

  protected RaftStatus status;

  /** the raft logs are all stored and maintained in the log manager */
  protected RaftLogManager logManager;

  protected HeartbeatThread heartbeatThread;
  protected HeartbeatReqHandler heartbeatReqHandler;
  protected ElectionReqHandler electionReqHandler;
  /**
   * if set to true, the node will reject all log appends when the header of a group is removed from
   * the cluster, the members of the group should no longer accept writes, but they still can be
   * candidates for weak consistency reads and provide snapshots for the new data holders
   */
  volatile boolean readOnly = false;

  /**
   * client manager that provides reusable Thrift clients to connect to other RaftMembers and
   * execute RPC requests. It will be initialized according to the implementation of the subclasses
   */
  protected IClientManager<TEndPoint, AsyncRaftServiceClient> clientManager;

  protected CatchUpManager catchUpManager;
  /** a thread pool that is used to do commit log tasks asynchronous in heartbeat thread */
  private ExecutorService commitLogPool;

  private long lastCommitTaskTime;
  private long lastReportIndex;

  /**
   * logDispatcher buff the logs orderly according to their log indexes and send them sequentially,
   * which avoids the followers receiving out-of-order logs, forcing them to wait for previous logs.
   */
  private volatile LogDispatcher logDispatcher;

  private VotingLogList votingLogList;
  private volatile boolean stopped;
  protected IStateMachine stateMachine;
  protected LogSequencer logSequencer;
  private volatile LogAppender logAppender;
  private FlowBalancer flowBalancer;
  private Consumer<ConsensusGroupId> onRemove;
  private EntryAllocator<RequestEntry> requestEntryAllocator;

  public RaftMember(
      String storageDir,
      RaftConfig config,
      Peer thisNode,
      List<Peer> allNodes,
      List<Peer> newNodes,
      ConsensusGroupId groupId,
      IStateMachine stateMachine,
      IClientManager<TEndPoint, AsyncRaftServiceClient> clientManager,
      Consumer<ConsensusGroupId> onRemove) {
    this.config = config;
    this.storageDir = storageDir;

    this.thisNode = thisNode;
    this.allNodes = allNodes;
    if (allNodes.isEmpty()) {
      recoverConfiguration();
    } else {
      persistConfiguration();
    }
    this.newNodes = newNodes;

    this.groupId = groupId;
    this.name =
        groupId.toString()
            + "-"
            + thisNode.getEndpoint().getIp()
            + "-"
            + thisNode.getEndpoint().getPort();
    this.status = new RaftStatus(name);

    this.clientManager = clientManager;
    this.stateMachine = stateMachine;

    this.votingLogList = new VotingLogList(this);
    votingLogList.setEnableWeakAcceptance(config.isEnableWeakAcceptance());
    this.heartbeatReqHandler = new HeartbeatReqHandler(this);
    this.electionReqHandler = new ElectionReqHandler(this);
    this.requestEntryAllocator =
        new EntryAllocator<>(config, RequestEntry::new, this::getSafeIndex);
    this.logManager =
        new DirectorySnapshotRaftLogManager(
            new SyncLogDequeSerializer(groupId, config, this),
            new AsyncLogApplier(new BaseApplier(stateMachine, this), name, config),
            name,
            stateMachine,
            config,
            this::examineUnappliedEntry,
            this::getSafeIndex,
            this::recycleEntry);
    this.appenderFactory =
        config.isUseFollowerSlidingWindow() ? new Factory() : new BlockingLogAppender.Factory();
    this.logAppender = appenderFactory.create(this, config);
    this.logSequencer = SEQUENCER_FACTORY.create(this, config);
    this.logDispatcher = new LogDispatcher(this, config);
    this.onRemove = onRemove;

    initPeerMap();
  }

  public void recycleEntry(Entry entry) {
    if (isLeader() && entry instanceof RequestEntry) {
      requestEntryAllocator.recycle(((RequestEntry) entry));
    }
  }

  public long getSafeIndex() {
    return votingLogList.getSafeIndex();
  }

  public void applyConfigChange(ConfigChangeEntry configChangeEntry) {
    List<Peer> newNodes = configChangeEntry.getNewPeers();
    if (!newNodes.equals(this.newNodes)) {
      return;
    }

    if (newNodes.contains(thisNode)) {
      applyNewNodes();
    }
  }

  public void applyNewNodes() {
    try {
      logManager.getLock().writeLock().lock();
      logDispatcher.applyNewNodes();
      allNodes = newNodes;
      newNodes = null;
      persistConfiguration();
    } finally {
      logManager.getLock().writeLock().unlock();
    }
  }

  public void remove() {
    stop();
    FileUtils.deleteDirectory(new File(storageDir));
    onRemove.accept(groupId);
  }

  private void examineUnappliedEntry(List<Entry> entries) {
    ConfigChangeEntry configChangeEntry = null;
    for (Entry entry : entries) {
      if (entry instanceof ConfigChangeEntry) {
        configChangeEntry = (ConfigChangeEntry) entry;
      }
    }
    if (configChangeEntry != null) {
      setNewNodes(configChangeEntry.getNewPeers());
    }
  }

  public void recoverConfiguration() {
    ByteBuffer buffer;
    try {
      Path tmpConfigurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_TMP_FILE_NAME).getAbsolutePath());
      Path configurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath());
      // If the tmpConfigurationPath exists, it means the `persistConfigurationUpdate` is
      // interrupted
      // unexpectedly, we need substitute configuration with tmpConfiguration file
      if (Files.exists(tmpConfigurationPath)) {
        if (Files.exists(configurationPath)) {
          Files.delete(configurationPath);
        }
        Files.move(tmpConfigurationPath, configurationPath);
      }
      buffer = ByteBuffer.wrap(Files.readAllBytes(configurationPath));
      int size = buffer.getInt();
      for (int i = 0; i < size; i++) {
        allNodes.add(Peer.deserialize(buffer));
      }
      logger.info("{}: Recover IoTConsensus server Impl, configuration: {}", name, allNodes);
    } catch (IOException e) {
      logger.error("Unexpected error occurs when recovering configuration", e);
    }
  }

  public void persistConfiguration() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      outputStream.writeInt(allNodes.size());
      for (Peer node : allNodes) {
        node.serialize(outputStream);
      }
      Files.write(
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath()),
          publicBAOS.getBuf());
    } catch (IOException e) {
      logger.error("Unexpected error occurs when persisting configuration", e);
    }
  }

  /**
   * Start the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   */
  public void start() {
    if (heartbeatThread != null) {
      return;
    }

    stateMachine.start();
    getLogDispatcher();

    startBackGroundThreads();
    setStopped(false);
    logger.info("{} started", name);
  }

  void startBackGroundThreads() {
    heartbeatThread = new HeartbeatThread(this, config);
    heartbeatThread.start();
    catchUpManager = new CatchUpManager(this, config);
    catchUpManager.start();
    commitLogPool = IoTDBThreadPoolFactory.newSingleThreadExecutor("RaftCommitLog");
    if (config.isUseFollowerLoadBalance()) {
      flowBalancer = new FlowBalancer(logDispatcher, this, config);
      flowBalancer.start();
    }
  }

  public void initPeerMap() {
    status.peerMap = new ConcurrentHashMap<>();
    for (Peer peer : allNodes) {
      status.peerMap.computeIfAbsent(peer, k -> new PeerInfo());
    }
  }

  /**
   * Stop the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   */
  public void stop() {
    if (stopped) {
      return;
    }
    setStopped(true);
    logger.info("Member {} stopping", name);

    closeLogManager();

    logDispatcher.stop();
    heartbeatThread.stop();
    catchUpManager.stop();

    if (logSequencer != null) {
      logSequencer.close();
    }

    if (commitLogPool != null) {
      commitLogPool.shutdownNow();
      try {
        commitLogPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for commitLogPool to end", e);
      }
    }

    if (flowBalancer != null) {
      flowBalancer.stop();
    }
    logger.info("Member {} stopped", name);
  }

  public void closeLogManager() {
    if (logManager != null) {
      logManager.close();
    }
  }

  /**
   * If "newTerm" is larger than the local term, give up the leadership, become a follower and reset
   * heartbeat timer.
   */
  public void stepDown(long newTerm, Peer newLeader) {
    try {
      logManager.getLock().writeLock().lock();
      long currTerm = status.term.get();
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        logger.info("{} has update it's term to {}", getName(), newTerm);
        status.term.set(newTerm);
        status.setVoteFor(null);
        status.setRole(RaftRole.CANDIDATE);
        status.setLeader(null);
        updateHardState(newTerm, status.getVoteFor());
      }

      if (currTerm <= newTerm && newLeader != null) {
        // only when the request is from a leader should we update lastHeartbeatReceivedTime,
        // otherwise the node may be stuck in FOLLOWER state by a stale node.
        status.setLeader(newLeader);
        status.setRole(RaftRole.FOLLOWER);
        heartbeatThread.setLastHeartbeatReceivedTime(System.currentTimeMillis());
      }
    } finally {
      logManager.getLock().writeLock().unlock();
    }
  }

  public void updateHardState(long currentTerm, Peer voteFor) {
    HardState state = logManager.getHardState();
    state.setCurrentTerm(currentTerm);
    state.setVoteFor(voteFor);
    logManager.updateHardState(state);
  }

  public void tryUpdateCommitIndex(long leaderTerm, long commitIndex, long commitTerm) {
    if (leaderTerm >= status.term.get()
        && logManager.getCommitLogIndex() < commitIndex
        && System.currentTimeMillis() - lastCommitTaskTime > 100) {
      // there are more local logs that can be committed, commit them in a ThreadPool so the
      // heartbeat response will not be blocked
      CommitLogTask commitLogTask = new CommitLogTask(logManager, commitIndex, commitTerm);
      commitLogTask.registerCallback(new CommitLogCallback(this));
      // if the log is not consistent, the commitment will be blocked until the leader makes the
      // node catch up
      if (commitLogPool != null && !commitLogPool.isShutdown()) {
        commitLogPool.submit(commitLogTask);
        lastCommitTaskTime = System.currentTimeMillis();
      }
    }
  }

  public void catchUp(Peer follower, long lastLogIdx) {
    catchUpManager.catchUp(follower, lastLogIdx);
  }

  /**
   * Process the HeartBeatRequest from the leader. If the term of the leader is smaller than the
   * local term, reject the request by telling it the newest term. Else if the local logs are
   * consistent with the leader's, commit them. Else help the leader find the last matched log. Also
   * update the leadership, heartbeat timer and term of the local node.
   */
  public HeartBeatResponse processHeartbeatRequest(HeartBeatRequest request) {
    return heartbeatReqHandler.processHeartbeatRequest(request);
  }

  /**
   * Process an ElectionRequest. If the request comes from the last leader, accept it. Else decide
   * whether to accept by examining the log status of the elector.
   */
  public long processElectionRequest(ElectionRequest electionRequest) {
    return electionReqHandler.processElectionRequest(electionRequest);
  }

  public AppendEntryResult appendEntries(AppendEntriesRequest request)
      throws UnknownLogTypeException {
    return appendEntriesInternal(request);
  }

  /** Similar to appendEntry, while the incoming load is batch of logs instead of a single log. */
  private AppendEntryResult appendEntriesInternal(AppendEntriesRequest request)
      throws UnknownLogTypeException {
    logger.debug("{} received an AppendEntriesRequest", name);

    // the term checked here is that of the leader, not that of the log
    long checkResult =
        checkRequestTerm(
            request.term,
            new Peer(
                ConsensusGroupId.Factory.createFromTConsensusGroupId(request.groupId),
                request.leaderId,
                request.leader));
    if (checkResult != Response.RESPONSE_AGREE) {
      return new AppendEntryResult(checkResult)
          .setGroupId(getRaftGroupId().convertToTConsensusGroupId());
    }

    AppendEntryResult response;
    long startTime = Statistic.RAFT_RECEIVER_PARSE_ENTRY.getOperationStartTime();
    List<Entry> entries = LogUtils.parseEntries(request.entries, stateMachine);
    Statistic.RAFT_RECEIVER_PARSE_ENTRY.calOperationCostTimeFromStart(startTime);

    response = logAppender.appendEntries(request.leaderCommit, request.term, entries);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} AppendEntriesRequest of log size {} completed with result {}",
          name,
          request.getEntries().size(),
          response);
    }

    return response;
  }

  /**
   * Check the term of the AppendEntryRequest. The term checked is the term of the leader, not the
   * term of the log. A new leader can still send logs of old leaders.
   *
   * @return -1 if the check is passed, >0 otherwise
   */
  private long checkRequestTerm(long leaderTerm, Peer leader) {
    long localTerm;

    // if the request comes before the heartbeat arrives, the local term may be smaller than the
    // leader term
    localTerm = status.term.get();
    if (leaderTerm < localTerm) {
      logger.debug(
          "{} rejected the AppendEntriesRequest for term: {}/{}", name, leaderTerm, localTerm);
      return localTerm;
    } else {
      if (leaderTerm > localTerm) {
        stepDown(leaderTerm, leader);
      } else {
        heartbeatThread.setLastHeartbeatReceivedTime(System.currentTimeMillis());
      }
      status.setLeader(leader);
    }

    logger.debug("{} accepted the AppendEntryRequest for term: {}", name, localTerm);
    return Response.RESPONSE_AGREE;
  }

  private boolean checkLogSize(Entry entry) {
    return !config.isEnableRaftLogPersistence()
        || entry.estimateSize() < config.getRaftLogBufferSize();
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public DataSet read(IConsensusRequest request) throws CheckConsistencyException {
    syncLeaderWithConsistencyCheck(false);
    return stateMachine.read(request);
  }

  public long getLastIndex() {
    return logManager.getLastLogIndex();
  }

  public long getAppliedIndex() {
    return logManager.getAppliedIndex();
  }

  public List<Peer> getConfiguration() {
    return allNodes;
  }

  public RaftConfig getConfig() {
    return config;
  }

  protected enum AppendLogResult {
    OK,
    TIME_OUT,
    LEADERSHIP_STALE,
    WEAK_ACCEPT
  }

  public boolean isLeader() {
    return Objects.equals(status.leader.get(), thisNode);
  }

  private TSStatus ensureLeader(IConsensusRequest request) {
    if (getLeader() == null) {
      waitLeader();
    }

    if (!isLeader()) {
      Peer leader = getLeader();
      if (leader == null) {
        return StatusUtils.NO_LEADER.deepCopy().setMessage("No leader in: " + groupId);
      } else if (request != null) {
        return forwardRequest(request, leader.getEndpoint(), leader.getGroupId());
      } else {
        return new TSStatus()
            .setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
            .setRedirectNode(leader.getEndpoint());
      }
    }
    return null;
  }

  public TSStatus processRequest(IConsensusRequest request) {
    if (readOnly) {
      return StatusUtils.NODE_READ_ONLY;
    }

    TSStatus tsStatus = ensureLeader(request);
    if (tsStatus != null) {
      return tsStatus;
    }

    logger.debug("{}: Processing request {}", name, request);
    RequestEntry entry = requestEntryAllocator.Allocate();
    entry.setRequest(request);
    entry.preSerialize();
    entry.receiveTime = System.nanoTime();

    // just like processPlanLocally,we need to check the size of log
    if (!checkLogSize(entry)) {
      logger.error(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "or reduce the size of requests you send.");
      return StatusUtils.INTERNAL_ERROR.deepCopy().setMessage("Log cannot fit into buffer");
    }

    // assign term and index to the new log and append it
    VotingEntry votingEntry = logSequencer.sequence(entry);
    Statistic.LOG_DISPATCHER_FROM_RECEIVE_TO_CREATE.add(entry.createTime - entry.receiveTime);

    if (config.isUseFollowerLoadBalance()) {
      FlowMonitorManager.INSTANCE.report(thisNode.getEndpoint(), entry.estimateSize());
    }

    if (votingEntry == null) {
      return StatusUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT);
    }

    return waitForEntryResult(votingEntry);
  }

  protected void waitApply(Entry entry) throws LogExecutionException {
    // when using async applier, the log here may not be applied. To return the execution
    // result, we must wait until the log is applied.
    while (!entry.isApplied()) {
      // wait until the log is applied
      synchronized (entry) {
        try {
          entry.wait(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new LogExecutionException(e);
        }
      }
    }

    if (entry.getException() != null) {
      throw new LogExecutionException(entry.getException());
    }
  }

  private TSStatus includeLogNumbersInStatus(TSStatus status, long index, long term) {
    return status.setMessage(
        getRaftGroupId().getType().ordinal()
            + "-"
            + getRaftGroupId().getId()
            + "-"
            + index
            + "-"
            + term);
  }

  protected AppendLogResult waitAppendResult(VotingEntry votingEntry) {
    // wait for the followers to vote

    AcceptedType acceptedType = votingLogList.computeAcceptedType(votingEntry);
    if (acceptedType == AcceptedType.NOT_ACCEPTED) {
      acceptedType = waitAppendResultLoop(votingEntry);
    }

    // a node has a larger status.term than the local node, so this node is no longer a valid leader
    if (status.term.get() != votingEntry.getEntry().getCurrLogTerm()) {
      return AppendLogResult.LEADERSHIP_STALE;
    }
    // the node knows it is no longer the leader from other requests
    if (status.role != RaftRole.LEADER) {
      return AppendLogResult.LEADERSHIP_STALE;
    }

    if (acceptedType == AcceptedType.WEAKLY_ACCEPTED) {
      return AppendLogResult.WEAK_ACCEPT;
    }

    if (acceptedType == AcceptedType.STRONGLY_ACCEPTED) {
      return AppendLogResult.OK;
    }

    // cannot get enough agreements within a certain amount of time
    logger.info("{} failed", votingEntry);
    return AppendLogResult.TIME_OUT;
  }

  protected TSStatus handleLogExecutionException(Object log, Throwable cause) {
    TSStatus tsStatus =
        StatusUtils.getStatus(StatusUtils.EXECUTE_STATEMENT_ERROR, cause.getMessage());
    if (cause instanceof RuntimeException) {
      logger.error("RuntimeException during executing {}", log, cause);
    }
    if (cause instanceof IoTDBException) {
      tsStatus.setCode(((IoTDBException) cause).getErrorCode());
    }
    return tsStatus;
  }

  /**
   * wait until "voteCounter" counts down to zero, which means the quorum has received the log, or
   * one follower tells the node that it is no longer a valid leader, or a timeout is triggered.
   */
  @SuppressWarnings({"java:S2445"}) // safe synchronized
  private AcceptedType waitAppendResultLoop(VotingEntry log) {

    long nextTimeToPrint = 5000;
    long waitStart = System.nanoTime();
    long alreadyWait = 0;

    String threadBaseName = Thread.currentThread().getName();
    if (logger.isDebugEnabled()) {
      Thread.currentThread()
          .setName(threadBaseName + "-waiting-" + log.getEntry().getCurrLogIndex());
    }
    long waitTime = 1;
    AcceptedType acceptedType = votingLogList.computeAcceptedType(log);

    Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_APPEND_START.calOperationCostTimeFromStart(
        log.getEntry().createTime);
    long startTime = Statistic.RAFT_SENDER_LOG_APPEND_WAIT.getOperationStartTime();
    while (acceptedType == AcceptedType.NOT_ACCEPTED
        && alreadyWait < config.getWriteOperationTimeoutMS()) {
      synchronized (log.getEntry()) {
        acceptedType = votingLogList.computeAcceptedType(log);
        if (acceptedType != AcceptedType.NOT_ACCEPTED) {
          break;
        }

        try {
          log.getEntry().wait(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Unexpected interruption when sending a log", e);
        }
      }

      acceptedType = votingLogList.computeAcceptedType(log);
      alreadyWait = (System.nanoTime() - waitStart) / 1000000;
      if (alreadyWait > nextTimeToPrint) {
        logger.info(
            "Still not receive enough votes for {}, weakly " + "accepted {}, wait {}ms ",
            log,
            log.getWeaklyAcceptedNodes(),
            alreadyWait);
        nextTimeToPrint *= 2;
      }
    }
    Statistic.RAFT_SENDER_LOG_APPEND_WAIT.calOperationCostTimeFromStart(startTime);

    if (logger.isDebugEnabled()) {
      Thread.currentThread().setName(threadBaseName);
    }

    if (alreadyWait > 15000) {
      logger.info(
          "Slow entry {}, weakly " + "accepted {}, waited time {}ms",
          log,
          log.getWeaklyAcceptedNodes(),
          alreadyWait);
    }
    return acceptedType;
  }

  public ConsensusWriteResponse executeForwardedRequest(IConsensusRequest request) {
    TSStatus tsStatus = processRequest(request);
    tsStatus.setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    tsStatus.setRedirectNode(config.getRpcConfig().getClientRPCEndPoint());
    return new ConsensusWriteResponse(null, tsStatus);
  }

  /**
   * according to the consistency configuration, decide whether to execute syncLeader or not and
   * throws exception when failed. Note that the write request will always try to sync leader
   */
  public void syncLeaderWithConsistencyCheck(boolean isWriteRequest)
      throws CheckConsistencyException {
    if (isWriteRequest) {
      syncLeader(new StrongCheckConsistency());
    } else {
      switch (config.getConsistencyLevel()) {
        case STRONG_CONSISTENCY:
          syncLeader(new StrongCheckConsistency());
          return;
        case MID_CONSISTENCY:
          // if leaderCommitId bigger than localAppliedId a value,
          // will throw CHECK_MID_CONSISTENCY_EXCEPTION
          syncLeader(new MidCheckConsistency(config.getMaxSyncLogLag()));
          return;
        case WEAK_CONSISTENCY:
          // do nothing
          return;
        default:
          // this should not happen in theory
          throw new CheckConsistencyException(
              "unknown consistency=" + config.getConsistencyLevel().name());
      }
    }
  }

  public void syncLeader(CheckConsistency checkConsistency) throws CheckConsistencyException {
    if (status.role == RaftRole.LEADER) {
      return;
    }
    waitLeader();
    if (status.leader.get() == null || status.leader.get() == null) {
      // the leader has not been elected, we must assume the node falls behind
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      return;
    }
    if (status.role == RaftRole.LEADER) {
      return;
    }
    logger.debug("{}: try synchronizing with the leader {}", name, status.leader.get());
    waitUntilCatchUp(checkConsistency);
  }

  /**
   * Request the leader's commit index and wait until the local commit index becomes not less than
   * it.
   *
   * @throws CheckConsistencyException if leaderCommitId bigger than localAppliedId a threshold
   *     value after timeout
   */
  protected void waitUntilCatchUp(CheckConsistency checkConsistency)
      throws CheckConsistencyException {
    long leaderCommitId = Long.MIN_VALUE;
    RequestCommitIndexResponse response;
    try {
      response = requestCommitIdAsync();
      leaderCommitId = response.getCommitLogIndex();

      tryUpdateCommitIndex(
          response.getTerm(), response.getCommitLogIndex(), response.getCommitLogTerm());

      syncLocalApply(leaderCommitId, true);
    } catch (TException e) {
      logger.error(MSG_NO_LEADER_COMMIT_INDEX, name, status.leader.get(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error(MSG_NO_LEADER_COMMIT_INDEX, name, status.leader.get(), e);
    } finally {
      if (checkConsistency != null) {
        checkConsistency.postCheckConsistency(leaderCommitId, logManager.getAppliedIndex());
      }
    }
    logger.debug("Start to sync with leader, leader commit id is {}", leaderCommitId);
  }

  /**
   * sync local applyId to leader commitId
   *
   * @param leaderCommitId leader commit id
   * @param fastFail if enabled, when log differ too much, return false directly.
   */
  public void syncLocalApply(long leaderCommitId, boolean fastFail) {
    long startTime = System.currentTimeMillis();
    long waitedTime = 0;
    long localAppliedId;

    if (fastFail && leaderCommitId - logManager.getAppliedIndex() > config.getMaxSyncLogLag()) {
      logger.info(
          "{}: The raft log of this member is too backward to provide service directly.", name);
      return;
    }

    while (waitedTime < config.getSyncLeaderMaxWaitMs()) {
      try {
        localAppliedId = logManager.getAppliedIndex();
        logger.debug("{}: synchronizing commitIndex {}/{}", name, localAppliedId, leaderCommitId);
        if (leaderCommitId <= localAppliedId) {
          // this node has caught up
          if (logger.isDebugEnabled()) {
            waitedTime = System.currentTimeMillis() - startTime;
            logger.debug(
                "{}: synchronized to target index {} after {}ms", name, leaderCommitId, waitedTime);
          }
          return;
        }
        // wait for next heartbeat to catch up
        // the local node will not perform a commit here according to the leaderCommitId because
        // the node may have some inconsistent logs with the leader
        waitedTime = System.currentTimeMillis() - startTime;
        synchronized (syncLock) {
          syncLock.wait(config.getHeartbeatIntervalMs());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error(MSG_NO_LEADER_COMMIT_INDEX, name, status.leader.get(), e);
      }
    }
    logger.warn(
        "{}: Failed to synchronize to target index {} after {}ms",
        name,
        leaderCommitId,
        waitedTime);
  }

  /** Wait until the leader of this node becomes known or time out. */
  public void waitLeader() {
    long startTime = System.currentTimeMillis();
    while (status.leader.get() == null || status.leader.get() == null) {
      synchronized (waitLeaderCondition) {
        try {
          waitLeaderCondition.wait(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Unexpected interruption when waiting for a leader", e);
        }
      }
      long consumedTime = System.currentTimeMillis() - startTime;
      if (consumedTime >= config.getConnectionTimeoutInMS()) {
        logger.warn("{}: leader is still offline after {}ms", name, consumedTime);
        break;
      }
    }
    logger.debug("{}: current leader is {}", name, status.leader.get());
  }

  protected RequestCommitIndexResponse requestCommitIdAsync()
      throws TException, InterruptedException {
    // use Long.MAX_VALUE to indicate a timeout
    RequestCommitIndexResponse response = new RequestCommitIndexResponse(-1, -1, -1);
    AsyncRaftServiceClient client = getClient(status.leader.get().getEndpoint());
    if (client == null) {
      // cannot connect to the leader
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      return response;
    }
    GenericHandler<RequestCommitIndexResponse> handler =
        new GenericHandler<>(status.leader.get().getEndpoint());
    client.requestCommitIndex(getRaftGroupId().convertToTConsensusGroupId(), handler);
    return handler.getResult(config.getConnectionTimeoutInMS());
  }

  /** @return true if there is a log whose index is "index" and term is "term", false otherwise */
  public boolean matchLog(long index, long term) {
    boolean matched = logManager.matchTerm(term, index);
    logger.debug("Log {}-{} matched: {}", index, term, matched);
    return matched;
  }

  /**
   * Forward a non-query plan to a node using the default client.
   *
   * @param plan a non-query plan
   * @param node cannot be the local node
   * @param groupId must be set for data group communication, set to null for meta group
   *     communication
   * @return a TSStatus indicating if the forwarding is successful.
   */
  public TSStatus forwardRequest(IConsensusRequest plan, TEndPoint node, ConsensusGroupId groupId) {
    if (node == null || node.equals(thisNode.getEndpoint())) {
      logger.debug("{}: plan {} has no where to be forwarded", name, plan);
      return StatusUtils.NO_LEADER.deepCopy().setMessage("No leader to forward in: " + groupId);
    }
    logger.debug("{}: Forward {} to node {}", name, plan, node);

    TSStatus status;
    status = forwardPlanAsync(plan, node, groupId);
    if (status.getCode() == TSStatusCode.TIME_OUT.getStatusCode()
        && (groupId == null || groupId.equals(getRaftGroupId()))
        && (this.status.leader.get() != null)
        && this.status.leader.get().getEndpoint().equals(node)) {
      // leader is down, trigger a new election by resetting heartbeat
      heartbeatThread.setLastHeartbeatReceivedTime(-1);
      this.status.leader.set(null);
      waitLeader();
    }
    return status;
  }

  /**
   * Forward a non-query plan to "receiver" using "client".
   *
   * @param plan a non-query plan
   * @param groupId to determine which DataGroupMember of "receiver" will process the request.
   * @return a TSStatus indicating if the forwarding is successful.
   */
  private TSStatus forwardPlanAsync(
      IConsensusRequest plan, TEndPoint receiver, ConsensusGroupId groupId) {
    AsyncRaftServiceClient client = getClient(receiver);
    if (client == null) {
      logger.debug("{}: can not get client for node={}", name, receiver);
      return StatusUtils.TIME_OUT
          .deepCopy()
          .setMessage(String.format("%s cannot be reached", receiver));
    }
    return forwardPlanAsync(plan, receiver, groupId, client);
  }

  public TSStatus forwardPlanAsync(
      IConsensusRequest request,
      TEndPoint receiver,
      ConsensusGroupId groupId,
      AsyncRaftServiceClient client) {
    try {
      TSStatus tsStatus = SyncClientAdaptor.executeRequest(client, request, groupId, receiver);
      if (tsStatus == null) {
        tsStatus = StatusUtils.TIME_OUT;
        logger.warn(MSG_FORWARD_TIMEOUT, name, request, receiver);
      }
      return tsStatus;
    } catch (IOException | TException e) {
      logger.error(MSG_FORWARD_ERROR, name, request, receiver, e);
      return StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("{}: forward {} to {} interrupted", name, request, receiver);
      return StatusUtils.TIME_OUT;
    }
  }

  public String getLocalSnapshotTmpDir(String remoteSnapshotDirName) {
    return config.getStorageDir()
        + File.separator
        + groupId
        + File.separator
        + "remote_snapshot"
        + File.separator
        + remoteSnapshotDirName;
  }

  public TSStatus installSnapshot(ByteBuffer snapshotBytes, TEndPoint source) {
    if (!snapshotApplyLock.tryLock()) {
      return new TSStatus(TSStatusCode.SNAPSHOT_INSTALLING.getStatusCode());
    }

    DirectorySnapshot directorySnapshot;
    try {
      directorySnapshot = new DirectorySnapshot();
      directorySnapshot.deserialize(snapshotBytes);
      directorySnapshot.setSource(source);
      directorySnapshot.setMemberName(name);
      directorySnapshot.install(this);
    } finally {
      snapshotApplyLock.unlock();
    }
    logManager.getLock().writeLock().lock();
    try {
      logAppender.reset();
    } finally {
      logManager.getLock().writeLock().unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public boolean containsNode(Peer node) {
    for (Peer localNode : allNodes) {
      if (localNode.equals(node)) {
        return true;
      }
    }
    return false;
  }

  public Peer getLeader() {
    return status.leader.get();
  }

  public void setLeader(Peer leader) {
    if (!Objects.equals(leader, this.status.leader.get())) {
      if (leader == null) {
        logger.info("{} has been set to null in term {}", getName(), status.term.get());
      } else if (!Objects.equals(leader, this.thisNode)) {
        logger.info(
            "{} has become a follower of {} in term {}", getName(), leader, status.term.get());
      }
      synchronized (waitLeaderCondition) {
        this.status.leader.set(leader);
        if (this.status.leader.get() != null) {
          waitLeaderCondition.notifyAll();
        }
      }
    }
  }

  public RequestCommitIndexResponse requestCommitIndex() {
    long commitIndex;
    long commitTerm;
    long curTerm;
    try {
      logManager.getLock().readLock().lock();
      commitIndex = logManager.getCommitLogIndex();
      commitTerm = logManager.getCommitLogTerm();
      curTerm = status.getTerm().get();
    } finally {
      logManager.getLock().readLock().unlock();
    }

    return new RequestCommitIndexResponse(curTerm, commitIndex, commitTerm);
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  public ConsensusGroupId getRaftGroupId() {
    return groupId;
  }

  public VotingLogList getVotingLogList() {
    return votingLogList;
  }

  public boolean isStopped() {
    return stopped;
  }

  public void setStopped(boolean stopped) {
    this.stopped = stopped;
  }

  public LogDispatcher getLogDispatcher() {
    return logDispatcher;
  }

  public VotingEntry buildVotingLog(Entry e) {
    return new VotingEntry(e, null, allNodes, newNodes, config);
  }

  public HeartbeatThread getHeartbeatThread() {
    return heartbeatThread;
  }

  public Lock getSnapshotApplyLock() {
    return snapshotApplyLock;
  }

  public Object getSyncLock() {
    return syncLock;
  }

  public String getName() {
    return name;
  }

  public RaftRole getRole() {
    return status.role;
  }

  public RaftStatus getStatus() {
    return status;
  }

  public RaftLogManager getLogManager() {
    return logManager;
  }

  public Peer getThisNode() {
    return thisNode;
  }

  public List<Peer> getAllNodes() {
    return allNodes;
  }

  public List<Peer> getNewNodes() {
    return newNodes;
  }

  public void setNewNodes(List<Peer> newNodes) {
    logDispatcher.setNewNodes(this.newNodes);
    this.newNodes = newNodes;
  }

  public AsyncRaftServiceClient getHeartbeatClient(TEndPoint node) {
    try {
      return clientManager.borrowClient(node);
    } catch (Exception e) {
      logger.error("borrow async heartbeat client fail", e);
      return null;
    }
  }

  public AsyncRaftServiceClient getClient(TEndPoint node) {
    try {
      return clientManager.borrowClient(node);
    } catch (Exception e) {
      logger.error("borrow async client fail", e);
      return null;
    }
  }

  public TSStatus changeConfig(List<Peer> newNodes) {
    TSStatus tsStatus = ensureLeader(null);
    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return tsStatus;
    }

    List<Peer> oldNodes = new ArrayList<>(allNodes);
    VotingEntry votingEntry;
    try {
      logManager.getLock().writeLock().lock();
      if (this.newNodes != null) {
        return new TSStatus(TSStatusCode.CONFIGURATION_ERROR.getStatusCode())
            .setMessage("Last configuration change in progress");
      }
      ConfigChangeEntry e = new ConfigChangeEntry(oldNodes, newNodes);
      Entry lastEntry = logManager.getLastEntry();
      long lastIndex = lastEntry.getCurrLogIndex();
      long lastTerm = lastEntry.getCurrLogTerm();

      e.setCurrLogTerm(getStatus().getTerm().get());
      e.setCurrLogIndex(lastIndex + 1);
      e.setPrevTerm(lastTerm);

      logManager.append(Collections.singletonList(e), true);
      votingEntry = LogUtils.buildVotingLog(e, this);

      setNewNodes(newNodes);

      logDispatcher.offer(votingEntry);
    } finally {
      logManager.getLock().writeLock().unlock();
    }

    List<Peer> addedNodes = NodeUtils.computeAddedNodes(oldNodes, this.newNodes);
    for (Peer addedNode : addedNodes) {
      catchUp(addedNode, 0);
    }

    return waitForEntryResult(votingEntry);
  }

  private TSStatus waitForEntryResult(VotingEntry votingEntry) {
    try {
      AppendLogResult appendLogResult = waitAppendResult(votingEntry);
      Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_APPEND_END.add(
          System.nanoTime() - votingEntry.getEntry().createTime);
      switch (appendLogResult) {
        case WEAK_ACCEPT:
          Statistic.RAFT_LEADER_WEAK_ACCEPT.add(1);
          return includeLogNumbersInStatus(
              StatusUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
              votingEntry.getEntry().getCurrLogIndex(),
              votingEntry.getEntry().getCurrLogTerm());
        case OK:
          if (config.isWaitApply()) {
            waitApply(votingEntry.getEntry());
            votingEntry.getEntry().waitEndTime = System.nanoTime();
            Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_APPLY_END.add(
                votingEntry.getEntry().waitEndTime - votingEntry.getEntry().createTime);
            return includeLogNumbersInStatus(
                StatusUtils.OK.deepCopy(),
                votingEntry.getEntry().getCurrLogIndex(),
                votingEntry.getEntry().getCurrLogTerm());
          } else {
            return includeLogNumbersInStatus(
                StatusUtils.OK.deepCopy(),
                logManager.getAppliedIndex(),
                logManager.getAppliedTerm());
          }

        case TIME_OUT:
          logger.debug("{}: log {} timed out...", name, votingEntry.getEntry());
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          break;
      }
    } catch (LogExecutionException e) {
      return handleLogExecutionException(votingEntry.getEntry(), IOUtils.getRootCause(e));
    }
    return StatusUtils.getStatus(TSStatusCode.TIME_OUT);
  }

  public TSStatus addPeer(Peer newPeer) {
    List<Peer> allNodes = getAllNodes();
    if (allNodes.contains(newPeer)) {
      return StatusUtils.OK.deepCopy().setMessage("Peer already exists");
    }

    List<Peer> newPeers = new ArrayList<>(allNodes);
    newPeers.add(newPeer);
    return changeConfig(newPeers);
  }

  public TSStatus removePeer(Peer toRemove) {
    List<Peer> allNodes = getAllNodes();
    if (!allNodes.contains(toRemove)) {
      return StatusUtils.OK.deepCopy().setMessage("Peer already removed");
    }

    List<Peer> newPeers = new ArrayList<>(allNodes);
    newPeers.remove(toRemove);
    return changeConfig(newPeers);
  }

  public TSStatus updatePeer(Peer oldPeer, Peer newPeer) {
    List<Peer> allNodes = getAllNodes();
    if (!allNodes.contains(oldPeer)) {
      return StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy().setMessage("Peer already removed");
    }
    if (allNodes.contains(newPeer)) {
      return StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy().setMessage("Peer already exists");
    }

    List<Peer> newPeers = new ArrayList<>(allNodes);
    newPeers.remove(oldPeer);
    newPeers.add(newPeer);
    return changeConfig(newPeers);
  }

  public void triggerSnapshot() {
    logManager.takeSnapshot(this);
  }

  public TSStatus transferLeader(Peer peer) {
    if (thisNode.equals(peer)) {
      return StatusUtils.OK;
    }
    if (!isLeader()) {
      return StatusUtils.NO_LEADER.deepCopy().setMessage("This node is not a leader");
    }
    if (!allNodes.contains(peer)) {
      return StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy().setMessage("Peer not in this group");
    }

    AsyncRaftServiceClient client = getClient(peer.getEndpoint());
    try {
      return SyncClientAdaptor.forceElection(client, groupId);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public TSStatus forceElection() {
    if (isLeader()) {
      return StatusUtils.OK;
    }

    heartbeatThread.setLastHeartbeatReceivedTime(0);
    heartbeatThread.notifyHeartbeat();
    long waitStart = System.currentTimeMillis();
    long maxWait = 10_000L;
    while (!isLeader() && (System.currentTimeMillis() - waitStart) < maxWait) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        return StatusUtils.TIME_OUT;
      }
    }
    if (isLeader()) {
      return StatusUtils.OK;
    } else {
      return StatusUtils.TIME_OUT;
    }
  }

  public RaftMemberReport genMemberReport() {
    long prevLastLogIndex = lastReportIndex;
    lastReportIndex = logManager.getLastLogIndex();
    return new RaftMemberReport(
        status.role,
        status.getLeader(),
        status.getTerm().get(),
        logManager.getLastLogTerm(),
        lastReportIndex,
        logManager.getCommitLogIndex(),
        logManager.getCommitLogTerm(),
        logManager.getPersistedLogIndex(),
        readOnly,
        heartbeatThread.getLastHeartbeatReceivedTime(),
        prevLastLogIndex,
        logManager.getAppliedIndex(),
        requestEntryAllocator.toString());
  }
}
