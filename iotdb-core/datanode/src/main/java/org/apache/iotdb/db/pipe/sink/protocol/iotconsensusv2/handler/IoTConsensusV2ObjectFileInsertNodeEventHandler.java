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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncIoTConsensusV2ServiceClient;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.IoTConsensusV2AsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2ObjectFilePieceReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2ObjectFileUtils.ObjectFileDescriptor;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletInsertNodeReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IoTConsensusV2ObjectFileInsertNodeEventHandler
    implements AsyncMethodCallback<TIoTConsensusV2TransferResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusV2ObjectFileInsertNodeEventHandler.class);

  private final PipeInsertNodeTabletInsertionEvent event;
  private final InsertNode insertNode;
  private final List<ObjectFileDescriptor> objectFileDescriptors;
  private final IoTConsensusV2AsyncSink connector;
  private final TCommitId commitId;
  private final TConsensusGroupId consensusGroupId;
  private final ProgressIndex progressIndex;
  private final int thisDataNodeId;
  private final IoTConsensusV2SinkMetrics metric;
  private final int readFileBufferSize;
  private final long createTime;

  private AsyncIoTConsensusV2ServiceClient client;
  private int currentObjectFileIndex;
  private long currentObjectFileOffset;
  private boolean insertNodeSent;

  public IoTConsensusV2ObjectFileInsertNodeEventHandler(
      final PipeInsertNodeTabletInsertionEvent event,
      final InsertNode insertNode,
      final List<ObjectFileDescriptor> objectFileDescriptors,
      final IoTConsensusV2AsyncSink connector,
      final TCommitId commitId,
      final TConsensusGroupId consensusGroupId,
      final ProgressIndex progressIndex,
      final int thisDataNodeId,
      final IoTConsensusV2SinkMetrics metric) {
    this.event = event;
    this.insertNode = insertNode;
    this.objectFileDescriptors = objectFileDescriptors;
    this.connector = connector;
    this.commitId = commitId;
    this.consensusGroupId = consensusGroupId;
    this.progressIndex = progressIndex;
    this.thisDataNodeId = thisDataNodeId;
    this.metric = metric;
    this.readFileBufferSize = Math.max(1, PipeConfig.getInstance().getPipeSinkReadFileBufferSize());
    this.createTime = System.nanoTime();
  }

  public void transfer(final AsyncIoTConsensusV2ServiceClient client) throws TException {
    this.client = client;
    client.setShouldReturnSelf(false);
    transferNextReq();
  }

  private void transferNextReq() throws TException {
    if (currentObjectFileIndex < objectFileDescriptors.size()) {
      final ObjectFileDescriptor descriptor = objectFileDescriptors.get(currentObjectFileIndex);
      final ObjectNode objectNode = nextObjectNode(descriptor);
      client.iotConsensusV2Transfer(
          IoTConsensusV2ObjectFilePieceReq.toTIoTConsensusV2TransferReq(
              objectNode, commitId, consensusGroupId, thisDataNodeId),
          this);
      return;
    }

    insertNodeSent = true;
    client.iotConsensusV2Transfer(
        IoTConsensusV2TabletInsertNodeReq.toTIoTConsensusV2TransferReq(
            insertNode, commitId, consensusGroupId, progressIndex, thisDataNodeId),
        this);
  }

  private ObjectNode nextObjectNode(final ObjectFileDescriptor descriptor) {
    final long objectSize = descriptor.getObjectSize();
    final int pieceLength =
        objectSize == 0
            ? 0
            : (int) Math.min(readFileBufferSize, objectSize - currentObjectFileOffset);
    final boolean isEOF = objectSize == 0 || currentObjectFileOffset + pieceLength >= objectSize;
    final ObjectNode objectNode =
        new ObjectNode(isEOF, currentObjectFileOffset, pieceLength, descriptor.getObjectPath());

    if (isEOF) {
      currentObjectFileIndex++;
      currentObjectFileOffset = 0;
    } else {
      currentObjectFileOffset += pieceLength;
    }

    return objectNode;
  }

  @Override
  public void onComplete(final TIoTConsensusV2TransferResp response) {
    if (response == null) {
      onError(new PipeException(DataNodePipeMessages.TIOTCONSENSUSV2TRANSFERRESP_IS_NULL));
      return;
    }

    try {
      final TSStatus status = response.getStatus();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        connector.statusHandler().handle(status, status.getMessage(), event.toString());
      }

      if (!insertNodeSent) {
        transferNextReq();
        return;
      }

      event.decreaseReferenceCount(
          IoTConsensusV2ObjectFileInsertNodeEventHandler.class.getName(), true);
      connector.removeEventFromBuffer(event);
      metric.recordConnectorWalTransferTimer(System.nanoTime() - createTime);

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            DataNodePipeMessages.INSERTNODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED,
            event.getReplicateIndexForIoTV2());
      }

      returnClientIfNecessary();
    } catch (final Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(final Exception exception) {
    LOGGER.warn(
        DataNodePipeMessages.FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE,
        event.coreReportMessage(),
        event.getCommitterKey(),
        event.getReplicateIndexForIoTV2(),
        exception);

    if (RetryUtils.needRetryWithIncreasingInterval(exception)) {
      if (event.getRetryInterval() << 2 <= 0) {
        event.setRetryInterval(1000L * 20);
      } else {
        event.setRetryInterval(Math.min(1000L * 20, event.getRetryInterval() << 2));
      }
    }

    connector.addFailureEventToRetryQueue(event);
    metric.recordRetryCounter();
    returnClientIfNecessary();
  }

  private void returnClientIfNecessary() {
    if (client != null) {
      client.setShouldReturnSelf(true);
      client.returnSelf();
    }
  }
}
