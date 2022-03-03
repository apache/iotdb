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

package org.apache.iotdb.cluster.expr;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogDispatcher;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.DummyPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExprMember extends MetaGroupMember {

  private static final Logger logger = LoggerFactory.getLogger(ExprMember.class);
  public static boolean bypassRaft = false;
  public static boolean useSlidingWindow = false;

  private int windowCapacity =
      ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem() * 2;
  private int windowLength = 0;
  private Log[] logWindow = new Log[windowCapacity];
  private long firstPosPrevIndex = 0;
  private long[] prevTerms = new long[windowCapacity];

  private ExecutorService bypassPool;

  public ExprMember() {}

  public ExprMember(Node thisNode, PartitionGroup allNodes) {
    this.thisNode = thisNode;
    this.allNodes = allNodes;
  }

  public ExprMember(TProtocolFactory factory, Node thisNode, Coordinator coordinator)
      throws QueryProcessException {
    super(factory, thisNode, coordinator);
    resetWindow(logManager.getLastLogIndex(), logManager.getLastLogTerm());
    bypassPool = Executors.newFixedThreadPool(LogDispatcher.bindingThreadNum);
  }

  @Override
  protected synchronized void startSubServers() {
    // do not start data groups in such experiments
  }

  public void resetWindow(long firstPosPrevIndex, long firstPosPrevTerm) {
    this.firstPosPrevIndex = firstPosPrevIndex;
    this.prevTerms[0] = firstPosPrevTerm;
    logWindow = new Log[windowCapacity];
  }

  @Override
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      if (bypassRaft) {
        if (allNodes.size() == 1) {
          return StatusUtils.OK;
        }
        CountDownLatch latch = new CountDownLatch(allNodes.size() / 2);
        int bufferSize = 4096;
        if (plan instanceof DummyPlan && !((DummyPlan) plan).isNeedForward()) {
          return StatusUtils.OK;
        } else if (plan instanceof DummyPlan) {
          ((DummyPlan) plan).setNeedForward(false);
          bufferSize += ((DummyPlan) plan).getWorkload().length;
        }

        ExecutNonQueryReq req = new ExecutNonQueryReq();
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        plan.serialize(byteBuffer);
        byteBuffer.flip();
        req.setPlanBytes(byteBuffer);
        List<Future> futures = new ArrayList<>();
        for (Node node : getAllNodes()) {
          if (!ClusterUtils.isNodeEquals(node, thisNode)) {
            futures.add(
                bypassPool.submit(
                    () -> {
                      Client syncClient = getSyncClient(node);
                      try {
                        long operationStartTime =
                            Statistic.RAFT_SENDER_SEND_LOG.getOperationStartTime();
                        syncClient.executeNonQueryPlan(req);
                        Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(
                            operationStartTime);
                        latch.countDown();
                      } catch (TException e) {
                        ClientUtils.putBackSyncClient(syncClient);
                        return StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
                      }
                      ClientUtils.putBackSyncClient(syncClient);
                      return null;
                    }));
          }
        }
        if (allNodes.size() > 1) {
          latch.await();
        }
        return StatusUtils.OK;
      }
      long startTime = Timer.Statistic.META_GROUP_MEMBER_EXECUTE_NON_QUERY.getOperationStartTime();
      TSStatus tsStatus = processNonPartitionedMetaPlan(plan);
      Timer.Statistic.META_GROUP_MEMBER_EXECUTE_NON_QUERY.calOperationCostTimeFromStart(startTime);
      return tsStatus;
    } catch (Exception e) {
      logger.error("Exception in processing plan", e);
      return StatusUtils.INTERNAL_ERROR.deepCopy().setMessage(e.getMessage());
    }
  }

  /**
   * After insert an entry into the window, check if its previous and latter entries should be
   * removed if it mismatches.
   *
   * @param pos
   */
  private void checkLog(int pos) {
    checkLogPrev(pos);
    checkLogNext(pos);
  }

  private void checkLogPrev(int pos) {
    // check the previous entry
    long prevLogTerm = prevTerms[pos];
    if (pos > 0) {
      Log prev = logWindow[pos - 1];
      if (prev != null && prev.getCurrLogTerm() != prevLogTerm) {
        logWindow[pos - 1] = null;
      }
    }
  }

  private void checkLogNext(int pos) {
    // check the next entry
    Log log = logWindow[pos];
    boolean nextMismatch = false;
    if (pos < windowCapacity - 1) {
      long nextPrevTerm = prevTerms[pos + 1];
      if (nextPrevTerm != log.getCurrLogTerm()) {
        nextMismatch = true;
      }
    }
    if (nextMismatch) {
      for (int i = pos + 1; i < windowCapacity; i++) {
        if (logWindow[i] != null) {
          logWindow[i] = null;
          if (i == windowLength - 1) {
            windowLength = pos + 1;
          }
        } else {
          break;
        }
      }
    }
  }

  /**
   * Flush window range [0, flushPos) into the LogManager, where flushPos is the first null position
   * in the window.
   *
   * @param result
   * @param leaderCommit
   * @return
   */
  private long flushWindow(AppendEntryResult result, long leaderCommit) {
    long windowPrevLogIndex = firstPosPrevIndex;
    long windowPrevLogTerm = prevTerms[0];

    int flushPos = 0;
    for (; flushPos < windowCapacity; flushPos++) {
      if (logWindow[flushPos] == null) {
        break;
      }
    }

    // flush [0, flushPos)
    List<Log> logs = Arrays.asList(logWindow).subList(0, flushPos);
    // logger.info("{}, Flushing {} into log manager", logManager.getLastLogIndex(), logs);
    long success =
        logManager.maybeAppend(windowPrevLogIndex, windowPrevLogTerm, leaderCommit, logs);
    if (success != -1) {
      System.arraycopy(logWindow, flushPos, logWindow, 0, windowCapacity - flushPos);
      System.arraycopy(prevTerms, flushPos, prevTerms, 0, windowCapacity - flushPos);
      for (int i = 1; i <= flushPos; i++) {
        logWindow[windowCapacity - i] = null;
      }
    }
    firstPosPrevIndex = logManager.getLastLogIndex();
    result.status = Response.RESPONSE_STRONG_ACCEPT;
    result.setLastLogIndex(firstPosPrevIndex);
    result.setLastLogTerm(logManager.getLastLogTerm());
    return success;
  }

  protected AppendEntryResult appendEntries(
      long prevLogIndex, long prevLogTerm, long leaderCommit, List<Log> logs) {
    if (!useSlidingWindow) {
      return super.appendEntries(prevLogIndex, prevLogTerm, leaderCommit, logs);
    }

    if (logs.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE).setHeader(getHeader());
    }

    AppendEntryResult result = null;
    for (Log log : logs) {
      result = appendEntry(prevLogIndex, prevLogTerm, leaderCommit, log);

      if (result.status != Response.RESPONSE_AGREE
          && result.status != Response.RESPONSE_STRONG_ACCEPT
          && result.status != Response.RESPONSE_WEAK_ACCEPT) {
        return result;
      }
      prevLogIndex = log.getCurrLogIndex();
      prevLogTerm = log.getCurrLogTerm();
    }

    return result;
  }

  protected AppendEntryResult appendEntry(
      long prevLogIndex, long prevLogTerm, long leaderCommit, Log log) {
    if (!useSlidingWindow) {
      return super.appendEntry(prevLogIndex, prevLogTerm, leaderCommit, log);
    }

    long startTime = Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
    long appendedPos = 0;

    AppendEntryResult result = new AppendEntryResult();
    synchronized (logManager) {
      int windowPos = (int) (log.getCurrLogIndex() - logManager.getLastLogIndex() - 1);
      if (windowPos < 0) {
        // the new entry may replace an appended entry
        appendedPos = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, log);
        result.status = Response.RESPONSE_STRONG_ACCEPT;
        result.setLastLogIndex(logManager.getLastLogIndex());
        result.setLastLogTerm(logManager.getLastLogTerm());
      } else if (windowPos < windowCapacity) {
        // the new entry falls into the window
        logWindow[windowPos] = log;
        prevTerms[windowPos] = prevLogTerm;
        if (windowLength < windowPos + 1) {
          windowLength = windowPos + 1;
        }
        checkLog(windowPos);
        if (windowPos == 0) {
          appendedPos = flushWindow(result, leaderCommit);
        } else {
          result.status = Response.RESPONSE_WEAK_ACCEPT;
        }

        Statistic.RAFT_WINDOW_LENGTH.add(windowLength);
      } else {
        Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
        result.setStatus(Response.RESPONSE_OUT_OF_WINDOW);
        result.setHeader(getHeader());
        return result;
      }
    }

    Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
    if (appendedPos == -1) {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    return result;
  }

  public void setBypassPool(ExecutorService bypassPool) {
    this.bypassPool = bypassPool;
  }
}
