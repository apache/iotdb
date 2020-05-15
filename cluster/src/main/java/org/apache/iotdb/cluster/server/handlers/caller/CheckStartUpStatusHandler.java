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
package org.apache.iotdb.cluster.server.handlers.caller;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckStartUpStatusHandler implements AsyncMethodCallback<CheckStatusResponse> {

  private static final Logger logger = LoggerFactory.getLogger(CheckStartUpStatusHandler.class);

  private AtomicReference<CheckStatusResponse> result;

  @Override
  public void onComplete(CheckStatusResponse response) {
    this.result.set(response);
  }

  @Override
  public void onError(Exception e) {
    logger.debug("Failed to check start up status because {}", e.getMessage());
  }

  public void setResponse(AtomicReference response) {
    this.result = response;
  }
}
