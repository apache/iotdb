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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.consensus.pipe.PipeConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferResp;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletRawReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// TODO: 如：1,1 1,2 1,3 1,4 1,5 / 1,6 1,7 1,8（follower 得想办法知道 leader
// 是否发满了/前置请求是否发完了）：发送端等待事件超时后尝试握手
// TODO: 处理 tablet 攒批
public class PipeConsensusReceiver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiver.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private final RequestExecutor requestExecutor = new RequestExecutor();
  private final PipeConsensus pipeConsensus;
  private final ConsensusGroupId consensusGroupId;

  public PipeConsensusReceiver(PipeConsensus pipeConsensus, ConsensusGroupId consensusGroupId) {
    this.pipeConsensus = pipeConsensus;
    this.consensusGroupId = consensusGroupId;
  }

  /**
   * This method cannot be set to synchronize. Receive events can be concurrent since reqBuffer but
   * load event must be synchronized.
   */
  public TPipeConsensusTransferResp receive(final TPipeConsensusTransferReq req) {
    final short rawRequestType = req.getType();
    if (PipeConsensusRequestType.isValidatedRequestType(rawRequestType)) {
      switch (PipeConsensusRequestType.valueOf(rawRequestType)) {
        case TRANSFER_TS_FILE_PIECE:
        case TRANSFER_TS_FILE_PIECE_WITH_MOD:
          return requestExecutor.onRequest(req, true);
        default:
          return requestExecutor.onRequest(req, false);
      }
    }
    // Unknown request type, which means the request can not be handled by this receiver,
    // maybe the version of the receiver is not compatible with the sender
    final TSStatus status =
        RpcUtils.getStatus(
            TSStatusCode.PIPE_TYPE_ERROR,
            String.format("PipeConsensus Unknown PipeRequestType %s.", rawRequestType));
    if (LOGGER.isWarnEnabled()) {
      LOGGER.warn("PipeConsensus Unknown PipeRequestType, response status = {}.", status);
    }
    return new TPipeConsensusTransferResp(status);
  }

  // TODO: support batch transfer
  public TPipeConsensusBatchTransferResp receive(final TPipeConsensusBatchTransferReq req) {
    return null;
  }

  private TPipeConsensusTransferResp loadEvent(final TPipeConsensusTransferReq req) {
    // synchronized load event
    try {
      final short rawRequestType = req.getType();
      if (PipeConsensusRequestType.isValidatedRequestType(rawRequestType)) {
        switch (PipeConsensusRequestType.valueOf(rawRequestType)) {
          case TRANSFER_TABLET_INSERT_NODE:
            return handleTransferTabletInsertNode(
                PipeConsensusTabletInsertNodeReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TABLET_RAW:
            return handleTransferTabletRaw(
                PipeConsensusTabletRawReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TABLET_BINARY:
            return handleTransferTabletBinary(
                PipeConsensusTabletBinaryReq.fromTPipeConsensusTransferReq(req));
          case TRANSFER_TS_FILE_PIECE:

          case TRANSFER_TS_FILE_SEAL:

          case TRANSFER_TS_FILE_PIECE_WITH_MOD:

          case TRANSFER_TS_FILE_SEAL_WITH_MOD:

          case TRANSFER_TABLET_BATCH:
            LOGGER.info("PipeConsensus transfer batch hasn't been implemented yet.");
          default:
            break;
        }
      }
    } catch (Exception e) {

    }
    // TODO: use DataRegionStateMachine to impl it. here will invoke @sc's impl interface
    // TODO: check memory when logging wal
    // TODO: check disk(read-only etc.) when writing tsFile
    // for test: we return success by default
    return new TPipeConsensusTransferResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private TPipeConsensusTransferResp handleTransferTabletInsertNode(
      PipeConsensusTabletInsertNodeReq req) throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    return new TPipeConsensusTransferResp(impl.write(req.getInsertNode()));
  }

  private TPipeConsensusTransferResp handleTransferTabletBinary(PipeConsensusTabletBinaryReq req)
      throws ConsensusGroupNotExistException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    return new TPipeConsensusTransferResp(impl.write(req.convertToInsertNode()));
  }

  private TPipeConsensusTransferResp handleTransferTabletRaw(PipeConsensusTabletRawReq req)
      throws Exception {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(pipeConsensus.getImpl(consensusGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(consensusGroupId));
    return new TPipeConsensusTransferResp(impl.write(req.convertToInsertTabletNode()));
  }

  public PipeConsensusRequestVersion getVersion() {
    return PipeConsensusRequestVersion.VERSION_1;
  }

  /**
   * An executor component to ensure all events sent from connector can be loaded in sequence,
   * although events can arrive receiver in a random sequence.
   */
  private class RequestExecutor {
    // An ordered set that buffers transfer request, whose length is not larger than
    // PIPE_CONSENSUS_EVENT_BUFFER_SIZE.
    // Here we use set is to avoid duplicate events being received in some special cases
    private final TreeSet<WrappedRequest> reqBuffer;
    private final Lock lock;
    private final Condition condition;
    private long onSyncedCommitIndex = -1;
    private int connectorRebootTimes = 1;

    public RequestExecutor() {
      reqBuffer =
          new TreeSet<>(
              Comparator.comparingInt(WrappedRequest::getRebootTime)
                  .thenComparingLong(WrappedRequest::getCommitIndex));
      lock = new ReentrantLock();
      condition = lock.newCondition();
    }

    private void onSuccess(long nextSyncedCommitIndex) {
      LOGGER.info("Debug only: process no.{} event successfully!", nextSyncedCommitIndex);
      reqBuffer.pollFirst();
      onSyncedCommitIndex = nextSyncedCommitIndex;
    }

    private TPipeConsensusTransferResp onRequest(
        final TPipeConsensusTransferReq req, final boolean isTransferTsFilePiece) {
      LOGGER.info(
          "Debug only: no.{} event try to acquire lock", req.getCommitId().getCommitIndex());
      lock.lock();
      try {
        WrappedRequest wrappedReq = new WrappedRequest(req);
        LOGGER.info("Debug only: start process no.{} event", wrappedReq.getCommitIndex());
        // if a req is deprecated, we will discard it
        // This case may happen in this scenario: leader has transferred {1,2} and is intending to
        // transfer {3, 4, 5, 6}. And in one moment, follower has received {4, 5, 6}, {3} is still
        // transferring due to some network latency.
        // At this time, leader restarts, and it will resend {3, 4, 5, 6} with incremental
        // rebootTimes. If the {3} sent before the leader restart arrives after the follower
        // receives
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
        // Judge whether connector has rebooted or not, if the rebootTimes increases compared to
        // connectorRebootTimes, need to reset receiver because connector has been restarted.
        if (wrappedReq.getRebootTime() > connectorRebootTimes) {
          resetWithNewestRebootTime(wrappedReq.getRebootTime());
        }
        reqBuffer.add(wrappedReq);

        if (reqBuffer.size() >= COMMON_CONFIG.getPipeConsensusEventBufferSize()
            && !reqBuffer.first().equals(wrappedReq)) {
          // If reqBuffer is full and current thread do not hold the reqBuffer's peek, this req
          // is not supposed to be processed. So current thread should notify the corresponding
          // threads to process the peek.
          condition.signalAll();
        }

        // Polling to process
        while (true) {
          if (reqBuffer.first().equals(wrappedReq)
              && wrappedReq.getCommitIndex() == onSyncedCommitIndex + 1) {
            // If current req is supposed to be process, load this event through
            // DataRegionStateMachine.
            TPipeConsensusTransferResp resp = loadEvent(req);

            // Only when event apply is successful and what is transmitted is not TsFilePiece, req
            // will be removed from the buffer and onSyncedCommitIndex will be updated. Because pipe
            // will transfer multi reqs with same commitId in a single TsFileInsertionEvent, only
            // when the last seal req is applied, we can discard this event.
            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                && !isTransferTsFilePiece) {
              onSuccess(onSyncedCommitIndex + 1);
            }
            return resp;
          }

          if (reqBuffer.size() >= COMMON_CONFIG.getPipeConsensusEventBufferSize()
              && reqBuffer.first().equals(wrappedReq)) {
            // If the reqBuffer is full and its peek is hold by current thread, load this event.
            TPipeConsensusTransferResp resp = loadEvent(req);

            if (resp != null
                && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                && !isTransferTsFilePiece) {
              onSuccess(wrappedReq.getCommitIndex());
              // signal all other reqs that may wait for this event
              condition.signalAll();
            }
            return resp;
          } else {
            // if the req is not supposed to be processed and reqBuffer is not full, current thread
            // should wait until reqBuffer is full, which indicates the receiver has received all
            // the requests from the connector without duplication or leakage.
            try {
              LOGGER.info(
                  "Debug only: no.{} event waiting on the lock...",
                  req.getCommitId().getCommitIndex());
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
    private void resetWithNewestRebootTime(int connectorRebootTimes) {
      this.reqBuffer.clear();
      this.onSyncedCommitIndex = -1;
      // sync the follower's connectorRebootTimes with connector's actual rebootTimes
      this.connectorRebootTimes = connectorRebootTimes;
    }

    @TestOnly
    public int getConnectorRebootTimes() {
      return connectorRebootTimes;
    }

    @TestOnly
    public long getOnSyncedCommitIndex() {
      return onSyncedCommitIndex;
    }
  }

  @TestOnly
  public int getConnectorRebootTimes() {
    return requestExecutor.getConnectorRebootTimes();
  }

  @TestOnly
  public long getOnSyncedCommitIndex() {
    return requestExecutor.getOnSyncedCommitIndex();
  }

  /**
   * Wrapped TPipeConsensusTransferReq for RequestExecutor.reqBuffer in order to save memory
   * allocation. We don’t really need to hold a reference to TPipeConsensusTransferReq here, because
   * we only need the commitId information of TPipeConsensusTransferReq in the
   * RequestExecutor.reqBuffer.
   */
  private static class WrappedRequest {
    final int rebootTime;
    final long commitIndex;

    public WrappedRequest(TPipeConsensusTransferReq req) {
      this.rebootTime = req.getCommitId().getRebootTimes();
      this.commitIndex = req.getCommitId().getCommitIndex();
    }

    public int getRebootTime() {
      return rebootTime;
    }

    public long getCommitIndex() {
      return commitIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WrappedRequest that = (WrappedRequest) o;
      return rebootTime == that.rebootTime && commitIndex == that.commitIndex;
    }
  }
}
