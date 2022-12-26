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
package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class OperatePipeProcedureRollbackProcessor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(OperatePipeProcedureRollbackProcessor.class);
  private static final int TIME_INTERVAL = 5_000;

  private final NodeManager nodeManager;

  private final Map<Integer, Queue<TOperatePipeOnDataNodeReq>> messageMap =
      new ConcurrentHashMap<>();
  private volatile ScheduledFuture<?> promise;
  private ScheduledFuture<?> canceller;
  private final ScheduledExecutorService executorService =
      IoTDBThreadPoolFactory.newScheduledThreadPool(2, "OperatePipeProcedureRollback");

  public OperatePipeProcedureRollbackProcessor(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }
}
