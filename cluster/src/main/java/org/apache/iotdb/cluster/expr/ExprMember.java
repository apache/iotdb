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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.log.Log;
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
import org.apache.iotdb.db.qp.physical.sys.ExprPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprMember extends MetaGroupMember {

  private static final Logger logger = LoggerFactory.getLogger(ExprMember.class);
  private static final ExecutorService bypassPool = Executors.newCachedThreadPool();
  public static boolean bypassRaft = false;
  public static boolean useSlidingWindow = false;

  private int windowSize = 10000;
  private Log[] logWindow = new Log[windowSize];
  private long firstPosPrevIndex = 0;
  private long[] prevTerms = new long[windowSize];

  public ExprMember() {
  }

  public ExprMember(Node thisNode, List<Node> allNodes) {
    this.thisNode = thisNode;
    this.allNodes = allNodes;
  }

  public ExprMember(TProtocolFactory factory,
      Node thisNode, Coordinator coordinator)
      throws QueryProcessException {
    super(factory, thisNode, coordinator);
    this.firstPosPrevIndex = logManager.getLastLogIndex();
    this.prevTerms[0] = logManager.getLastLogTerm();
  }

  @Override
  protected synchronized void startSubServers() {
    // do not start data groups in such experiments
  }

  @Override
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      if (bypassRaft) {
        int bufferSize = 4096;
        if (plan instanceof ExprPlan && !((ExprPlan) plan).isNeedForward()) {
          return StatusUtils.OK;
        } else if (plan instanceof ExprPlan) {
          ((ExprPlan) plan).setNeedForward(false);
          bufferSize += ((ExprPlan) plan).getWorkload().length;
        }

        ExecutNonQueryReq req = new ExecutNonQueryReq();
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        plan.serialize(byteBuffer);
        byteBuffer.flip();
        req.setPlanBytes(byteBuffer);
        List<Future> futures = new ArrayList<>();
        for (Node node : getAllNodes()) {
          if (!ClusterUtils.isNodeEquals(node, thisNode)) {
            futures.add(bypassPool.submit(() -> {
              Client syncClient = getSyncClient(node);
              try {
                long operationStartTime = Statistic.RAFT_SENDER_SEND_LOG
                    .getOperationStartTime();
                syncClient.executeNonQueryPlan(req);
                Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(operationStartTime);
              } catch (TException e) {
                ClientUtils.putBackSyncClient(syncClient);
                return StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
              }
              ClientUtils.putBackSyncClient(syncClient);
              return null;
            }));
          }
        }
        for (Future future : futures) {
          future.get();
        }
        return StatusUtils.OK;
      }
      return processNonPartitionedMetaPlan(plan);
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
    if (pos < windowSize - 1) {
      long nextPrevTerm = prevTerms[pos + 1];
      if (nextPrevTerm != log.getCurrLogTerm()) {
        nextMismatch = true;
      }
    }
    if (nextMismatch) {
      for (int i = pos + 1; i < windowSize; i++) {
        if (logWindow[i] != null) {
          logWindow[i] = null;
        } else {
          break;
        }
      }
    }
  }

  /**
   * Flush window range [0, flushPos) into the LogManager, where flushPos is the first null
   * position in the window.
   * @param result
   * @param leaderCommit
   * @return
   */
  private long flushWindow(AppendEntryResult result, long leaderCommit) {
    long windowPrevLogIndex = firstPosPrevIndex;
    long windowPrevLogTerm = prevTerms[0];

    int flushPos = 0;
    for (; flushPos < windowSize; flushPos++) {
      if (logWindow[flushPos] == null) {
        break;
      }
    }

    // flush [0, flushPos)
    List<Log> logs = Arrays.asList(logWindow).subList(0, flushPos);
    long success = logManager.maybeAppend(windowPrevLogIndex, windowPrevLogTerm, leaderCommit,
        logs);
    if (success != -1) {
      System.arraycopy(logWindow, flushPos, logWindow, 0, windowSize - flushPos);
      System.arraycopy(prevTerms, flushPos, prevTerms, 0, windowSize - flushPos);
      for (int i = 1; i <= flushPos; i++) {
        logWindow[windowSize - i] = null;
      }
    }
    firstPosPrevIndex = logManager.getLastLogIndex();
    result.status = Response.RESPONSE_STRONG_ACCEPT;
    result.setLastLogIndex(firstPosPrevIndex);
    result.setLastLogTerm(logManager.getLastLogTerm());
    return success;
  }

  protected AppendEntryResult appendEntry(long prevLogIndex, long prevLogTerm, long leaderCommit,
      Log log) {
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
      } else if (windowPos < windowSize) {
        // the new entry falls into the window
        logWindow[windowPos] = log;
        prevTerms[windowPos] = prevLogTerm;
        checkLog(windowPos);
        if (windowPos == 0) {
          appendedPos = flushWindow(result, leaderCommit);
        } else {
          result.status = Response.RESPONSE_WEAK_ACCEPT;
        }
      } else {
        return new AppendEntryResult(Response.RESPONSE_LOG_MISMATCH);
      }
    }

    Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
    if (appendedPos == -1) {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    return result;
  }

}
