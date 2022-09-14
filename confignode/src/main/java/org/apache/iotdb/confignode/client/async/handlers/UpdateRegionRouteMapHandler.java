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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class UpdateRegionRouteMapHandler extends AbstractRetryHandler
    implements AsyncMethodCallback<TSStatus> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateRegionRouteMapHandler.class);

  public UpdateRegionRouteMapHandler(
      CountDownLatch countDownLatch,
      DataNodeRequestType requestType,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    super(countDownLatch, requestType, targetDataNode, dataNodeLocationMap);
  }

  @Override
  public void onComplete(TSStatus status) {
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataNodeLocationMap.remove(targetDataNode.getDataNodeId());
      LOGGER.info(
          "Successfully update the RegionRouteMap on DataNode: {}", targetDataNode.getDataNodeId());
    } else {
      LOGGER.error("Update RegionRouteMap on DataNode: {} failed", targetDataNode.getDataNodeId());
    }
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    LOGGER.error("Update RegionRouteMap on DataNode: {} failed", targetDataNode.getDataNodeId());
    countDownLatch.countDown();
  }
}
