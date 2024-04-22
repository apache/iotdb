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

package org.apache.iotdb.db.pipe.receiver.protocol.pipeconsensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PipeConsensusReceiver extends IoTDBDataNodeReceiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiver.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private final RequestExecutor requestExecutor = new RequestExecutor();

  /**
   * This method cannot be set to synchronize. Receive events can be concurrent since reqBuffer but
   * load event must be synchronized.
   */
  public TPipeConsensusTransferResp receive(final TPipeConsensusTransferReq req) {
    return requestExecutor.onRequest(req);
  }

  private TPipeConsensusTransferResp loadEvent(final TPipeConsensusTransferReq req) {
    // synchronized load event
    TPipeTransferReq originalPipeTransferReq = toTPipeTransferReq(req);

    // TODO: use DataRegionStateMachine to impl it. here will invoke @sc's impl interface
    // TODO: check memory when logging wal
    // TODO: check disk(read-only etc.) when writing tsfile
    return null;
  }

  // WIP
  private TPipeTransferReq toTPipeTransferReq(TPipeConsensusTransferReq req) {
    TPipeTransferReq result = new TPipeTransferReq();
    result.body = req.body;
    result.version = req.getVersion();
    result.type = req.getType();
    return result;
  }

  // WIP
  private TPipeConsensusTransferResp toTPipeConsensusTransferResp(TPipeTransferResp resp) {
    TPipeConsensusTransferResp result = new TPipeConsensusTransferResp();
    result.body = resp.body;
    result.status = resp.getStatus();
    return result;
  }

  /**
   * An executor component to ensure all events sent from connector can be loaded in sequence,
   * although events can arrive receiver in a random sequence.
   */
  private class RequestExecutor {
    // A min heap that buffers transfer request, whose length is not larger than
    // PIPE_CONSENSUS_EVENT_BUFFER_SIZE
    private final PriorityQueue<WrappedRequest> reqBuffer;
    private final Lock lock;
    private final Condition condition;
    private int onSyncedCommitIndex = -1;
    private int connectorRebootTimes = 1;

    public RequestExecutor() {
      reqBuffer =
          new PriorityQueue<>(
              COMMON_CONFIG.getPipeConsensusEventBufferSize(),
              Comparator.comparingInt(WrappedRequest::getRebootTime)
                  .thenComparingInt(WrappedRequest::getCommitIndex));
      lock = new ReentrantLock();
      condition = lock.newCondition();
    }

    private TPipeConsensusTransferResp onRequest(final TPipeConsensusTransferReq req) {
      lock.lock();
      WrappedRequest wrappedReq = new WrappedRequest(req);
      // if a req is deprecated, we will discard it
      // This case may happen in this scenario: leader has transferred {1,2} and is intending to
      // transfer {3, 4, 5, 6}. And in one moment, follower has received {4, 5, 6}, {3} is still
      // transferring due to some network latency.
      // At this time, leader restarts, and it will resend {3, 4, 5, 6} with incremental
      // rebootTimes. If the {3} sent before the leader restart arrives after the follower receives
      // the request with incremental rebootTimes, the {3} sent before the leader restart needs to
      // be discarded.
      if (wrappedReq.getRebootTime() < connectorRebootTimes) {
        final TSStatus status =
            new TSStatus(
                RpcUtils.getStatus(
                    TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST,
                    "PipeConsensus receiver received a deprecated request, which may be sent before the connector restart. Consider to discard it"));
        return new TPipeConsensusTransferResp(status);
      }
      try {
        reqBuffer.offer(wrappedReq);
        // Judge whether connector has rebooted or not, if the rebootTimes increases compared to
        // connectorRebootTimes, need to reset receiver because connector has been restarted.
        if (wrappedReq.getRebootTime() > connectorRebootTimes) {
          reset(connectorRebootTimes);
          // TODO：如果发送端重启，接收端堆积的请求怎么办？考虑 reset 时的堆积请求
          // TODO: 如：1,1 1,2 1,3 1,4 1,5 / 1,6 1,7 1,8（follwer 得想办法知道 leader 是否发满了/前置请求是否发完了）
          // TODO: RPC 60s 超时问题；如果存储引擎等非共识层写入超时，会导致已接收的副本重发，从而导致堆积。
        }

        // Polling to process
        while (true) {
          if (reqBuffer.peek().equals(req)
              && wrappedReq.getCommitIndex() == onSyncedCommitIndex + 1) {
            // If current req is supposed to be process, load this event through
            // DataRegionStateMachine.
            TPipeConsensusTransferResp resp = loadEvent(req);
            reqBuffer.remove();
            onSyncedCommitIndex++;
            return resp;
          }

          if (reqBuffer.size() >= COMMON_CONFIG.getPipeConsensusEventBufferSize()) {
            // If the reqBuffer is full and its peek is hold by current thread, load this event.
            if (reqBuffer.peek().equals(req)) {
              TPipeConsensusTransferResp resp = loadEvent(req);
              reqBuffer.remove();
              onSyncedCommitIndex = wrappedReq.getCommitIndex();
              return resp;
            } else {
              // If reqBuffer is full and current thread do not hold the reqBuffer's peek, this req
              // is not supposed to be processed. So current thread should notify the corresponding
              // threads to process the peek.
              condition.signalAll();
            }
          } else {
            // if the req is not supposed to be processed and reqBuffer is not full, current thread
            // should wait until reqBuffer is full, which indicates the receiver has received all
            // the requests from the connector without duplication or leakage.
            try {
              condition.await(
                  COMMON_CONFIG.getPipeConsensusReceiverMaxWaitingTimeForEventsInMs(),
                  TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              LOGGER.warn(
                  "current waiting is interrupted. onSyncedCommitIndex: {}. Exception: ",
                  wrappedReq.getCommitIndex(),
                  e);
              Thread.currentThread().interrupt();
            }
          }
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Reset all data to initial status and set connectorRebootTimes properly. This method is called
     * when receiver identifies connector has rebooted.
     */
    private void reset(int connectorRebootTimes) {
      this.reqBuffer.clear();
      this.onSyncedCommitIndex = -1;
      // sync the follower's connectorRebootTimes with connector's actual rebootTimes
      this.connectorRebootTimes = connectorRebootTimes;
    }
  }

  /**
   * Wrapped TPipeConsensusTransferReq for RequestExecutor.reqBuffer in order to save memory
   * allocation. We don’t really need to hold a reference to TPipeConsensusTransferReq here, because
   * we only need the commitId information of TPipeConsensusTransferReq in the
   * RequestExecutor.reqBuffer.
   */
  private static class WrappedRequest {
    final int rebootTime;
    final int commitIndex;

    public WrappedRequest(TPipeConsensusTransferReq req) {
      this.rebootTime = req.rebootTimes;
      this.commitIndex = req.commitIndex;
    }

    public int getRebootTime() {
      return rebootTime;
    }

    public int getCommitIndex() {
      return commitIndex;
    }
  }
}
