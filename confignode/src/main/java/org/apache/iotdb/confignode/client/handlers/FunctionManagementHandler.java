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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class FunctionManagementHandler implements AsyncMethodCallback<TSStatus> {

  private final CountDownLatch countDownLatch;
  private final List<TSStatus> dataNodeResponseStatus;
  private final String ip;
  private final int port;

  public FunctionManagementHandler(
      CountDownLatch countDownLatch, List<TSStatus> dataNodeResponseStatus, String ip, int port) {
    this.countDownLatch = countDownLatch;
    this.dataNodeResponseStatus = dataNodeResponseStatus;
    this.ip = ip;
    this.port = port;
  }

  @Override
  public void onComplete(TSStatus response) {
    countDownLatch.countDown();
    dataNodeResponseStatus.add(response);
  }

  @Override
  public void onError(Exception exception) {
    countDownLatch.countDown();
    dataNodeResponseStatus.add(
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("DataNode[" + ip + ":" + port + "] " + exception.getMessage()));
  }
}
