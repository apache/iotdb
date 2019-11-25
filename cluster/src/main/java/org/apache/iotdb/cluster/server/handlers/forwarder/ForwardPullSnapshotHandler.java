/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.handlers.forwarder;

import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.pullSnapshot_call;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

public class ForwardPullSnapshotHandler implements AsyncMethodCallback<pullSnapshot_call> {

  private AsyncMethodCallback resultHandler;

  public ForwardPullSnapshotHandler(AsyncMethodCallback resultHandler) {
    this.resultHandler = resultHandler;
  }

  @Override
  public void onComplete(pullSnapshot_call response) {
    try {
      resultHandler.onComplete(response.getResult());
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    resultHandler.onError(exception);
  }
}
