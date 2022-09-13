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
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class DeleteDataForDeleteTimeSeriesHandler extends AbstractRetryHandler
    implements AsyncMethodCallback<TSStatus> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DeleteDataForDeleteTimeSeriesHandler.class);

  private Map<Integer, TSStatus> dataNodeResponseStatusMap;

  public DeleteDataForDeleteTimeSeriesHandler(
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TSStatus> dataNodeResponseStatusMap) {
    super(DataNodeRequestType.DELETE_DATA_FOR_DELETE_TIMESERIES, dataNodeLocationMap);
    this.dataNodeResponseStatusMap = dataNodeResponseStatusMap;
  }

  public DeleteDataForDeleteTimeSeriesHandler(
      CountDownLatch countDownLatch,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    super(
        countDownLatch,
        DataNodeRequestType.DELETE_DATA_FOR_DELETE_TIMESERIES,
        targetDataNode,
        dataNodeLocationMap);
    this.dataNodeResponseStatusMap = new ConcurrentHashMap<>();
  }

  public Map<Integer, TSStatus> getDataNodeResponseStatusMap() {
    return dataNodeResponseStatusMap;
  }

  @Override
  public void onComplete(TSStatus tsStatus) {
    dataNodeResponseStatusMap.put(targetDataNode.getDataNodeId(), tsStatus);
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataNodeLocationMap.remove(targetDataNode.getDataNodeId());
      LOGGER.info("Successfully delete data for delete timeseries on DataNode: {}", targetDataNode);
    } else if (tsStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      dataNodeLocationMap.remove(targetDataNode.getDataNodeId());
      LOGGER.error(
          "Failed to delete data for delete timeseries on DataNode {}, {}",
          targetDataNode,
          tsStatus);
    } else {
      LOGGER.error(
          "Failed to delete data for delete timeseries on DataNode {}, {}",
          targetDataNode,
          tsStatus);
    }
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    countDownLatch.countDown();
    dataNodeResponseStatusMap.put(
        targetDataNode.dataNodeId,
        new TSStatus(
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
                "Delete data for delete timeseries error on DataNode: {id="
                    + targetDataNode.getDataNodeId()
                    + ", internalEndPoint="
                    + targetDataNode.getInternalEndPoint()
                    + "}"
                    + e.getMessage())));
  }
}
