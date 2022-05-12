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
package org.apache.iotdb.confignode.client.handlers;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.concurrent.CountDownLatch;

/** Only use CreateRegionHandler when the LoadManager wants to create Regions */
public class CreateRegionHandler implements AsyncMethodCallback<TSStatus> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateRegionHandler.class);

  // Mark BitSet when successfully create
  private final int index;
  private final BitSet bitSet;

  // Used to protect asynchronous creation
  private final CountDownLatch latch;

  // Used for Logger
  private final TConsensusGroupId consensusGroupId;
  private final TDataNodeLocation dataNodeLocation;

  public CreateRegionHandler(
      int index,
      BitSet bitSet,
      CountDownLatch latch,
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation dataNodeLocation) {
    this.index = index;
    this.bitSet = bitSet;
    this.latch = latch;
    this.consensusGroupId = consensusGroupId;
    this.dataNodeLocation = dataNodeLocation;
  }

  @Override
  public void onComplete(TSStatus tsStatus) {
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      synchronized (bitSet) {
        bitSet.set(index);
      }
      LOGGER.info(
          String.format(
              "Successfully create %s on DataNode: %s",
              ConsensusGroupId.formatTConsensusGroupId(consensusGroupId), dataNodeLocation));
    } else {
      LOGGER.error(
          String.format(
              "Create %s on DataNode: %s failed, %s",
              ConsensusGroupId.formatTConsensusGroupId(consensusGroupId),
              dataNodeLocation,
              tsStatus));
    }
    latch.countDown();
  }

  @Override
  public void onError(Exception e) {
    LOGGER.error(
        String.format(
            "Create %s on DataNode: %s failed, %s",
            ConsensusGroupId.formatTConsensusGroupId(consensusGroupId), dataNodeLocation, e));
    latch.countDown();
  }
}
