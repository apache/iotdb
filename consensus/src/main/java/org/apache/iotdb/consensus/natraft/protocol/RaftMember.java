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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.natraft.Utils.IOUtils;
import org.apache.iotdb.consensus.natraft.Utils.StatusUtils;
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
import org.apache.iotdb.consensus.natraft.protocol.log.CommitLogCallback;
import org.apache.iotdb.consensus.natraft.protocol.log.CommitLogTask;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.LogParser;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingLog;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.BlockingLogAppender;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.LogAppender;
import org.apache.iotdb.consensus.natraft.protocol.log.appender.LogAppenderFactory;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.AsyncLogApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.BaseApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.catchup.CatchUpManager;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.LogDispatcher;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.VotingLogList;
import org.apache.iotdb.consensus.natraft.protocol.log.flowcontrol.FlowBalancer;
import org.apache.iotdb.consensus.natraft.protocol.log.flowcontrol.FlowMonitorManager;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.RequestEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.SnapshotRaftLogManager;
import org.apache.iotdb.consensus.natraft.protocol.log.sequencing.LogSequencer;
import org.apache.iotdb.consensus.natraft.protocol.log.sequencing.LogSequencerFactory;
import org.apache.iotdb.consensus.natraft.protocol.log.sequencing.SynchronousSequencer;
import org.apache.iotdb.consensus.natraft.protocol.log.serialization.SyncLogDequeSerializer;
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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RaftMember {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);

  private RaftConfig config;
  private boolean enableWeakAcceptance;
  protected static final LogAppenderFactory appenderFactory = new BlockingLogAppender.Factory();

  protected static final LogSequencerFactory SEQUENCER_FACTORY = new SynchronousSequencer.Factory();

  private static final String MSG_FORWARD_TIMEOUT = "{}: Forward {} to {} time out";
  private static final String MSG_FORWARD_ERROR =
      "{}: encountered an error when forwarding {} to" + " {}";
  private static final String MSG_NO_LEADER_COMMIT_INDEX =
      "{}: Cannot request commit index from {}";
  private static final String MSG_NO_LEADER_IN_SYNC = "{}: No leader is found when synchronizing";
  public static final String MSG_LOG_IS_ACCEPTED = "{}: log {} is accepted";

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
  private Object syncLock = new Object();

  protected TEndPoint thisNode;
  /** the nodes that belong to the same raft group as thisNode. */
  protected List<TEndPoint> allNodes;

  protected ConsensusGroupId groupId;
  protected String name;
  protected String storageDir;

  protected RaftStatus status = new RaftStatus();

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
   * lastLogIndex when generating the previous member report, to show the log ingestion rate of the
   * member by comparing it with the current last log index.
   */
  long lastReportedLogIndex;

  /**
   * client manager that provides reusable Thrift clients to connect to other RaftMembers and
   * execute RPC requests. It will be initialized according to the implementation of the subclasses
   */
  protected IClientManager<TEndPoint, AsyncRaftServiceClient> clientManager;

  protected CatchUpManager catchUpManager;
  /** a thread pool that is used to do commit log tasks asynchronous in heartbeat thread */
  private ExecutorService commitLogPool;

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

  public RaftMember(
      RaftConfig config,
      TEndPoint thisNode,
      List<TEndPoint> allNodes,
      ConsensusGroupId groupId,
      IStateMachine stateMachine,
      IClientManager<TEndPoint, AsyncRaftServiceClient> clientManager) {
    this.config = config;
    initConfig();

    this.thisNode = thisNode;
    this.allNodes = allNodes;
    if (allNodes.isEmpty()) {
      recoverConfiguration();
    } else {
      persistConfiguration();
    }

    this.groupId = groupId;
    this.name = groupId + "-" + thisNode;

    this.clientManager = clientManager;
    this.stateMachine = stateMachine;
    this.logManager =
        new SnapshotRaftLogManager(
            new SyncLogDequeSerializer(groupId, config),
            new AsyncLogApplier(new BaseApplier(stateMachine), name, config),
            name,
            stateMachine);
    this.votingLogList = new VotingLogList(allNodes.size() / 2, this);
    this.logAppender = appenderFactory.create(this, config);
    this.logSequencer = SEQUENCER_FACTORY.create(this, logManager, config);
    this.heartbeatReqHandler = new HeartbeatReqHandler(this);

    initPeerMap();
  }

  public void recoverConfiguration() {
    ByteBuffer buffer;
    try {
      buffer =
          ByteBuffer.wrap(
              Files.readAllBytes(
                  Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath())));
      int size = buffer.getInt();
      for (int i = 0; i < size; i++) {
        allNodes.add(ThriftCommonsSerDeUtils.deserializeTEndPoint(buffer));
      }
      logger.info("Recover Raft, configuration: {}", allNodes);
    } catch (IOException e) {
      logger.error("Unexpected error occurs when recovering configuration", e);
    }
  }

  public void persistConfiguration() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      outputStream.writeInt(allNodes.size());
      for (TEndPoint node : allNodes) {
        ThriftCommonsSerDeUtils.serializeTEndPoint(node, outputStream);
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
    catchUpManager = new CatchUpManager(this, config);
    catchUpManager.start();
    commitLogPool = IoTDBThreadPoolFactory.newSingleThreadExecutor("RaftCommitLog");
    if (config.isUseFollowerLoadBalance()) {
      FlowMonitorManager.INSTANCE.register(thisNode);
      flowBalancer = new FlowBalancer(logDispatcher, this, config);
      flowBalancer.start();
    }
  }

  private void initConfig() {
    this.enableWeakAcceptance = config.isEnableWeakAcceptance();
    this.storageDir = config.getStorageDir();
  }

  public void initPeerMap() {
    status.peerMap = new ConcurrentHashMap<>();
    for (TEndPoint entry : allNodes) {
      status.peerMap.computeIfAbsent(entry, k -> new PeerInfo(logManager.getLastLogIndex()));
    }
  }

  /**
   * Stop the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   */
  public void stop() {
    setStopped(true);
    closeLogManager();
    if (clientManager != null) {
      clientManager.close();
    }
    if (logSequencer != null) {
      logSequencer.close();
    }

    if (heartbeatThread == null) {
      return;
    }

    heartbeatThread.stop();
    catchUpManager.stop();

    if (commitLogPool != null) {
      commitLogPool.shutdownNow();
      try {
        commitLogPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for commitLogPool to end", e);
      }
    }

    status.leader.set(null);
    catchUpManager = null;
    heartbeatThread = null;

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
  public void stepDown(long newTerm, TEndPoint newLeader) {
    try {
      logManager.getLock().writeLock().lock();
      long currTerm = status.term.get();
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        logger.info("{} has update it's term to {}", getName(), newTerm);
        status.term.set(newTerm);
        status.setVoteFor(null);
        status.setRole(RaftRole.CANDIDATE);
        status.getLeader().set(null);
        updateHardState(newTerm, status.getVoteFor());
      }

      if (currTerm <= newTerm && newLeader != null) {
        // only when the request is from a leader should we update lastHeartbeatReceivedTime,
        // otherwise the node may be stuck in FOLLOWER state by a stale node.
        status.getLeader().set(newLeader);
        status.setRole(RaftRole.FOLLOWER);
        heartbeatThread.setLastHeartbeatReceivedTime(System.currentTimeMillis());
      }
    } finally {
      logManager.getLock().writeLock().unlock();
    }
  }

  public void updateHardState(long currentTerm, TEndPoint voteFor) {
    HardState state = logManager.getHardState();
    state.setCurrentTerm(currentTerm);
    state.setVoteFor(voteFor);
    logManager.updateHardState(state);
  }

  public void tryUpdateCommitIndex(long leaderTerm, long commitIndex, long commitTerm) {
    if (leaderTerm >= status.term.get() && logManager.getCommitLogIndex() < commitIndex) {
      // there are more local logs that can be committed, commit them in a ThreadPool so the
      // heartbeat response will not be blocked
      CommitLogTask commitLogTask = new CommitLogTask(logManager, commitIndex, commitTerm);
      commitLogTask.registerCallback(new CommitLogCallback(this));
      // if the log is not consistent, the commitment will be blocked until the leader makes the
      // node catch up
      if (commitLogPool != null && !commitLogPool.isShutdown()) {
        commitLogPool.submit(commitLogTask);
      }

      logger.debug(
          "{}: Inconsistent log found, leaderCommit: {}-{}, localCommit: {}-{}, "
              + "localLast: {}-{}",
          name,
          commitIndex,
          commitTerm,
          logManager.getCommitLogIndex(),
          logManager.getCommitLogTerm(),
          logManager.getLastLogIndex(),
          logManager.getLastLogTerm());
    }
  }

  public void catchUp(TEndPoint follower, long lastLogIdx) {
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
    long checkResult = checkRequestTerm(request.term, request.leader);
    if (checkResult != Response.RESPONSE_AGREE) {
      return new AppendEntryResult(checkResult)
          .setGroupId(getRaftGroupId().convertToTConsensusGroupId());
    }

    AppendEntryResult response;
    List<Entry> entries = new ArrayList<>();
    for (ByteBuffer buffer : request.getEntries()) {
      buffer.mark();
      Entry e;
      try {
        e = LogParser.getINSTANCE().parse(buffer);
        e.setByteSize(buffer.limit() - buffer.position());
      } catch (BufferUnderflowException ex) {
        buffer.reset();
        throw ex;
      }
      entries.add(e);
    }

    response = logAppender.appendEntries(request, entries);

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
  private long checkRequestTerm(long leaderTerm, TEndPoint leader) {
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
      status.getLeader().set(leader);
    }

    logger.debug("{} accepted the AppendEntryRequest for term: {}", name, localTerm);
    return Response.RESPONSE_AGREE;
  }

  private boolean checkLogSize(Entry entry) {
    return !config.isEnableRaftLogPersistence()
        || entry.serialize().capacity() + Integer.BYTES < config.getRaftLogBufferSize();
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public DataSet read(IConsensusRequest request) throws CheckConsistencyException {
    syncLeaderWithConsistencyCheck(false);
    return stateMachine.read(request);
  }

  protected enum AppendLogResult {
    OK,
    TIME_OUT,
    LEADERSHIP_STALE,
    WEAK_ACCEPT
  }

  public TSStatus processRequest(IConsensusRequest request) {
    if (readOnly) {
      return StatusUtils.NODE_READ_ONLY;
    }

    logger.debug("{}: Processing request {}", name, request);
    Entry entry = new RequestEntry(request);

    // just like processPlanLocally,we need to check the size of log
    if (!checkLogSize(entry)) {
      logger.error(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "or reduce the size of requests you send.");
      return StatusUtils.INTERNAL_ERROR.deepCopy().setMessage("Log cannot fit into buffer");
    }

    // assign term and index to the new log and append it
    VotingLog sendLogRequest = logSequencer.sequence(entry);
    FlowMonitorManager.INSTANCE.report(thisNode, entry.estimateSize());

    if (sendLogRequest == null) {
      return StatusUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT);
    }

    try {
      AppendLogResult appendLogResult =
          waitAppendResult(sendLogRequest, sendLogRequest.getQuorumSize());
      switch (appendLogResult) {
        case WEAK_ACCEPT:
          return includeLogNumbersInStatus(
              StatusUtils.getStatus(TSStatusCode.WEAKLY_ACCEPTED), entry);
        case OK:
          waitApply(entry);
          return includeLogNumbersInStatus(StatusUtils.OK.deepCopy(), entry);
        case TIME_OUT:
          logger.debug("{}: log {} timed out...", name, entry);
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          break;
      }
    } catch (LogExecutionException e) {
      return handleLogExecutionException(entry, IOUtils.getRootCause(e));
    }
    return StatusUtils.TIME_OUT;
  }

  protected void waitApply(Entry entry) throws LogExecutionException {
    // when using async applier, the log here may not be applied. To return the execution
    // result, we must wait until the log is applied.
    synchronized (entry) {
      while (!entry.isApplied()) {
        // wait until the log is applied
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

  private TSStatus includeLogNumbersInStatus(TSStatus status, Entry entry) {
    return status.setMessage(
        getRaftGroupId() + "-" + entry.getCurrLogIndex() + "-" + entry.getCurrLogTerm());
  }

  protected AppendLogResult waitAppendResult(VotingLog log, int quorumSize) {
    // wait for the followers to vote
    int totalAccepted = votingLogList.totalAcceptedNodeNum(log);
    int weaklyAccepted = log.getWeaklyAcceptedNodes().size();
    int stronglyAccepted = totalAccepted - weaklyAccepted;

    if (log.getEntry().getCurrLogIndex() == Long.MIN_VALUE
        || ((stronglyAccepted < quorumSize
            || (!enableWeakAcceptance || (totalAccepted < quorumSize)) && !log.isHasFailed()))) {
      waitAppendResultLoop(log, quorumSize);
    }
    totalAccepted = votingLogList.totalAcceptedNodeNum(log);
    weaklyAccepted = log.getWeaklyAcceptedNodes().size();
    stronglyAccepted = totalAccepted - weaklyAccepted;

    // a node has a larger status.term than the local node, so this node is no longer a valid leader
    if (status.term.get() != log.getEntry().getCurrLogTerm()) {
      return AppendLogResult.LEADERSHIP_STALE;
    }
    // the node knows it is no longer the leader from other requests
    if (status.role != RaftRole.LEADER) {
      return AppendLogResult.LEADERSHIP_STALE;
    }

    if (totalAccepted >= quorumSize && stronglyAccepted < quorumSize) {
      return AppendLogResult.WEAK_ACCEPT;
    }

    // cannot get enough agreements within a certain amount of time
    if (totalAccepted < quorumSize) {
      return AppendLogResult.TIME_OUT;
    }

    // voteCounter has counted down to zero
    return AppendLogResult.OK;
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
  private void waitAppendResultLoop(VotingLog log, int quorumSize) {
    int totalAccepted = votingLogList.totalAcceptedNodeNum(log);
    int weaklyAccepted = log.getWeaklyAcceptedNodes().size();
    int stronglyAccepted = totalAccepted - weaklyAccepted;
    long nextTimeToPrint = 5000;

    long waitStart = System.nanoTime();
    long alreadyWait = 0;

    String threadBaseName = Thread.currentThread().getName();
    long waitTime = 1;
    synchronized (log.getEntry()) {
      while (log.getEntry().getCurrLogIndex() == Long.MIN_VALUE
          || (stronglyAccepted < quorumSize
              && (!(enableWeakAcceptance || (totalAccepted < quorumSize))
                  && alreadyWait < config.getWriteOperationTimeoutMS()
                  && !log.isHasFailed()))) {
        try {
          log.getEntry().wait(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Unexpected interruption when sending a log", e);
        }
        waitTime = waitTime * 2;

        alreadyWait = (System.nanoTime() - waitStart) / 1000000;
        if (alreadyWait > nextTimeToPrint) {
          logger.info(
              "Still not receive enough votes for {}, weakly " + "accepted {}, wait {}ms ",
              log,
              log.getWeaklyAcceptedNodes(),
              alreadyWait);
          nextTimeToPrint *= 2;
        }
        totalAccepted = votingLogList.totalAcceptedNodeNum(log);
        weaklyAccepted = log.getWeaklyAcceptedNodes().size();
        stronglyAccepted = totalAccepted - weaklyAccepted;
      }
    }
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
  }

  public ConsensusWriteResponse executeForwardedRequest(IConsensusRequest request) {
    return new ConsensusWriteResponse(null, processRequest(request));
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

  public boolean syncLeader(CheckConsistency checkConsistency) throws CheckConsistencyException {
    if (status.role == RaftRole.LEADER) {
      return true;
    }
    waitLeader();
    if (status.leader.get() == null || status.leader.get() == null) {
      // the leader has not been elected, we must assume the node falls behind
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      return false;
    }
    if (status.role == RaftRole.LEADER) {
      return true;
    }
    logger.debug("{}: try synchronizing with the leader {}", name, status.leader.get());
    return waitUntilCatchUp(checkConsistency);
  }

  /**
   * Request the leader's commit index and wait until the local commit index becomes not less than
   * it.
   *
   * @return true if this node has caught up before timeout, false otherwise
   * @throws CheckConsistencyException if leaderCommitId bigger than localAppliedId a threshold
   *     value after timeout
   */
  protected boolean waitUntilCatchUp(CheckConsistency checkConsistency)
      throws CheckConsistencyException {
    long leaderCommitId = Long.MIN_VALUE;
    RequestCommitIndexResponse response;
    try {
      response = requestCommitIdAsync();
      leaderCommitId = response.getCommitLogIndex();

      tryUpdateCommitIndex(
          response.getTerm(), response.getCommitLogIndex(), response.getCommitLogTerm());

      return syncLocalApply(leaderCommitId, true);
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
    return false;
  }

  /**
   * sync local applyId to leader commitId
   *
   * @param leaderCommitId leader commit id
   * @param fastFail if enabled, when log differ too much, return false directly.
   * @return true if leaderCommitId <= localAppliedId
   */
  public boolean syncLocalApply(long leaderCommitId, boolean fastFail) {
    long startTime = System.currentTimeMillis();
    long waitedTime = 0;
    long localAppliedId;

    if (fastFail && leaderCommitId - logManager.getAppliedIndex() > config.getMaxSyncLogLag()) {
      logger.info(
          "{}: The raft log of this member is too backward to provide service directly.", name);
      return false;
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
          return true;
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
    return false;
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
    AsyncRaftServiceClient client = getClient(status.leader.get());
    if (client == null) {
      // cannot connect to the leader
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      return response;
    }
    GenericHandler<RequestCommitIndexResponse> handler = new GenericHandler<>(status.leader.get());
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
    if (node == null || node.equals(thisNode)) {
      logger.debug("{}: plan {} has no where to be forwarded", name, plan);
      return StatusUtils.NO_LEADER;
    }
    logger.debug("{}: Forward {} to node {}", name, plan, node);

    TSStatus status;
    status = forwardPlanAsync(plan, node, groupId);
    if (status.getCode() == TSStatusCode.NO_CONNECTION.getStatusCode()
        && (groupId == null || groupId.equals(getRaftGroupId()))
        && (this.status.leader.get() != null)
        && this.status.leader.get().equals(node)) {
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
      return StatusUtils.NO_CONNECTION
          .deepCopy()
          .setMessage(String.format("%s cannot be reached", receiver));
    }
    return forwardPlanAsync(plan, receiver, groupId, client);
  }

  public TSStatus forwardPlanAsync(
      IConsensusRequest request,
      TEndPoint receiver,
      ConsensusGroupId header,
      AsyncRaftServiceClient client) {
    try {
      TSStatus tsStatus = SyncClientAdaptor.executeRequest(client, request, header, receiver);
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

  public void installSnapshot(ByteBuffer snapshotBytes) {
    // TODO-raft: implement
  }

  public boolean containsNode(TEndPoint node) {
    for (TEndPoint localNode : allNodes) {
      if (localNode.equals(node)) {
        return true;
      }
    }
    return false;
  }

  public TEndPoint getLeader() {
    return status.leader.get();
  }

  public void setLeader(TEndPoint leader) {
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

  public VotingLog buildVotingLog(Entry e) {
    return new VotingLog(e, allNodes.size(), null, allNodes.size() / 2, config);
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

  public TEndPoint getThisNode() {
    return thisNode;
  }

  public List<TEndPoint> getAllNodes() {
    return allNodes;
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
      logger.error("borrow async heartbeat client fail", e);
      return null;
    }
  }
}
