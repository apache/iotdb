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

package org.apache.iotdb.cluster.server.handlers.forwarder;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class ForwardPlanHandler implements AsyncMethodCallback<TSStatus> {

  private static final Logger logger = LoggerFactory.getLogger(ForwardPlanHandler.class);
  private PhysicalPlan plan;
  private AtomicReference<TSStatus> result;
  private Node node;

  public ForwardPlanHandler(AtomicReference<TSStatus> result, PhysicalPlan plan, Node node) {
    this.result = result;
    this.plan = plan;
    this.node = node;
  }

  @Override
  public void onComplete(TSStatus response) {
    synchronized (result) {
      result.set(response);
      result.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof IOException) {
      logger.warn("Cannot send plan {} to node {}: {}", plan, node, exception.getMessage());
    } else {
      logger.error("Cannot send plan {} to node {}", plan, node, exception);
    }
    synchronized (result) {
      TSStatus status = StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, exception.getMessage());
      result.set(status);
      result.notifyAll();
    }
  }
}
