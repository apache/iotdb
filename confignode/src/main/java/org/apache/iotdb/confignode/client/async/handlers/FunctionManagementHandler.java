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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class FunctionManagementHandler extends AbstractRetryHandler
    implements AsyncMethodCallback<TSStatus> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionManagementHandler.class);

  private final List<TSStatus> dataNodeResponseStatus;

  public FunctionManagementHandler(
      CountDownLatch countDownLatch,
      DataNodeRequestType requestType,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      List<TSStatus> dataNodeResponseStatus) {
    super(countDownLatch, requestType, targetDataNode, dataNodeLocationMap);
    this.dataNodeResponseStatus = dataNodeResponseStatus;
  }

  @Override
  public void onComplete(TSStatus response) {
    if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataNodeResponseStatus.add(response);
      dataNodeLocationMap.remove(targetDataNode.getDataNodeId());
      LOGGER.info("Successfully {} on DataNode: {}", dataNodeRequestType, targetDataNode);
    } else {
      LOGGER.info("Failed to {} on DataNode: {}", dataNodeRequestType, targetDataNode);
    }
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception exception) {
    dataNodeResponseStatus.add(
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(targetDataNode + exception.getMessage()));
    LOGGER.info("Failed to {} on DataNode: {}", dataNodeRequestType, targetDataNode);
    countDownLatch.countDown();
  }
}
