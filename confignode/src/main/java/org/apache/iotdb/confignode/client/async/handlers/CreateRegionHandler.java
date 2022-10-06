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
package org.apache.iotdb.confignode.client.async.handlers;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Only use CreateRegionHandler when the LoadManager wants to create Regions */
public class CreateRegionHandler extends AbstractAsyncRPCHandler<TSStatus> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateRegionHandler.class);

  // The index of dataNodeLocations
  // We use Index instead of DataNodeId because it is possible to send multiple createRegion
  // requests to the same DataNode
  private final int index;
  // Used for Logger
  private final TConsensusGroupId consensusGroupId;

  public CreateRegionHandler(
      DataNodeRequestType requestType,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TSStatus> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TSStatus tsStatus) {
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataNodeLocationMap.remove(index);
      LOGGER.info(
          String.format(
              "Successfully create %s on DataNode: %s",
              ConsensusGroupId.formatTConsensusGroupId(consensusGroupId), targetDataNode));
    } else {
      LOGGER.error(
          String.format(
              "Create %s on DataNode: %s failed, %s",
              ConsensusGroupId.formatTConsensusGroupId(consensusGroupId),
              targetDataNode,
              tsStatus));
    }
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    LOGGER.error(
        String.format(
            "Create %s on DataNode: %s failed, %s",
            ConsensusGroupId.formatTConsensusGroupId(consensusGroupId), targetDataNode, e));
    countDownLatch.countDown();
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }
}
