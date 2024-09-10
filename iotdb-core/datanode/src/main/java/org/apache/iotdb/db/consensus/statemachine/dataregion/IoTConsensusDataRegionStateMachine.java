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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class IoTConsensusDataRegionStateMachine extends DataRegionStateMachine {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusDataRegionStateMachine.class);

  public IoTConsensusDataRegionStateMachine(DataRegion region) {
    super(region);
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    try {
      if (request instanceof DeserializedBatchIndexedConsensusRequest) {
        List<TSStatus> subStatus = new LinkedList<>();
        for (IConsensusRequest consensusRequest :
            ((DeserializedBatchIndexedConsensusRequest) request).getInsertNodes()) {
          PlanNode writeNode = (PlanNode) consensusRequest;
          subStatus.add(write(writeNode));
        }
        return new TSStatus().setSubStatus(subStatus);
      } else {
        return write((PlanNode) request);
      }
    } catch (IllegalArgumentException e) {
      LOGGER.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  /**
   * Deserialize request to PlanNode or BatchedIndexedRequest
   *
   * @param request write request
   */
  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    IConsensusRequest result;
    if (request instanceof IndexedConsensusRequest) {
      IndexedConsensusRequest indexedRequest = (IndexedConsensusRequest) request;
      result = grabPlanNode(indexedRequest);
    } else if (request instanceof BatchIndexedConsensusRequest) {
      BatchIndexedConsensusRequest batchRequest = (BatchIndexedConsensusRequest) request;
      DeserializedBatchIndexedConsensusRequest deserializedRequest =
          new DeserializedBatchIndexedConsensusRequest(
              batchRequest.getStartSyncIndex(),
              batchRequest.getEndSyncIndex(),
              batchRequest.getRequests().size());
      for (IndexedConsensusRequest indexedRequest : batchRequest.getRequests()) {
        final PlanNode planNode = grabPlanNode(indexedRequest);
        if (planNode instanceof ComparableConsensusRequest) {
          final IoTProgressIndex ioTProgressIndex =
              new IoTProgressIndex(batchRequest.getSourcePeerId(), indexedRequest.getSyncIndex());
          ((ComparableConsensusRequest) planNode).setProgressIndex(ioTProgressIndex);
        }
        deserializedRequest.add(planNode);
      }
      result = deserializedRequest;
    } else {
      result = getPlanNode(request);
    }
    return result;
  }

  @Override
  protected PlanNode getPlanNode(IConsensusRequest request) {
    PlanNode node;
    if (request instanceof ByteBufferConsensusRequest) {
      node = PlanNodeType.deserialize(request.serializeToByteBuffer());
    } else if (request instanceof IoTConsensusRequest) {
      node = WALEntry.deserializeForConsensus(request.serializeToByteBuffer());
    } else if (request instanceof PlanNode) {
      node = (PlanNode) request;
    } else {
      LOGGER.error("Unexpected IConsensusRequest : {}", request);
      throw new IllegalArgumentException("Unexpected IConsensusRequest!");
    }
    return node;
  }
}
