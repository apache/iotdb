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
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ExprPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;

public class ExprMember extends MetaGroupMember {

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
  }

  private int windowSize = 10000;
  private Log[] logWindow = new Log[windowSize];
  private long windowPrevLogIndex;
  private long windowPrevLogTerm;
  private long windowTerm;

  @Override
  protected synchronized void startSubServers() {

  }

  @Override
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    if (false) {
      if (plan instanceof ExprPlan && !((ExprPlan) plan).isNeedForward()) {
        return StatusUtils.OK;
      } else if (plan instanceof ExprPlan) {
        ((ExprPlan) plan).setNeedForward(false);
      }

      ExecutNonQueryReq req = new ExecutNonQueryReq();
      ByteBuffer byteBuffer = ByteBuffer.allocate(128 * 1024);
      plan.serialize(byteBuffer);
      byteBuffer.flip();
      req.setPlanBytes(byteBuffer);

      for (Node node : getAllNodes()) {
        if (!ClusterUtils.isNodeEquals(node, thisNode)) {
          Client syncClient = getSyncClient(node);
          try {
            syncClient.executeNonQueryPlan(req);
          } catch (TException e) {
            ClientUtils.putBackSyncClient(syncClient);
            return StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
          }
          ClientUtils.putBackSyncClient(syncClient);
        }
      }
      return StatusUtils.OK;
    }
    return processNonPartitionedMetaPlan(plan);
  }

  protected long appendEntry1(long prevLogIndex, long prevLogTerm, long leaderCommit, Log log) {
    long resp;
    long startTime = Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
    long success = 0;

    synchronized (logManager) {
      long windowPos = log.getCurrLogIndex() - logManager.getLastLogIndex() - 1;
      if (windowPos < 0) {
        success = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, log);
      } else if (windowPos < windowSize) {
        logWindow[(int) windowPos] = log;
        if (windowPos == 0) {
          windowPrevLogIndex = prevLogIndex;
          windowPrevLogTerm = prevLogTerm;

          int flushPos = 0;
          for (; flushPos < windowSize; flushPos++) {
            if (logWindow[flushPos] == null) {
              break;
            }
          }
          // flush [0, flushPos)
          List<Log> logs = Arrays.asList(logWindow).subList(0, flushPos);
          success = logManager.maybeAppend(windowPrevLogIndex, windowPrevLogTerm, leaderCommit,
              logs);
          if (success != -1) {
            System.arraycopy(logWindow, flushPos, logWindow, 0, windowSize - flushPos);
            for (int i = 1; i <= flushPos; i++) {
              logWindow[windowSize - i] = null;
            }
          } else {
            System.out.println("not success");
          }
        }
      } else {
        return Response.RESPONSE_LOG_MISMATCH;
      }
    }

    Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
    if (success != -1) {
      resp = Response.RESPONSE_AGREE;
    } else {
      // the incoming log points to an illegal position, reject it
      resp = Response.RESPONSE_LOG_MISMATCH;
    }
    return resp;
  }

}
