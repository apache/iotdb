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
package org.apache.iotdb.db.doublelive;

import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

/**
 * OperationSyncProducer using BlockingQueue to cache PhysicalPlan. And persist some PhysicalPlan
 * when they are too many to transmit
 */
public class OperationSyncProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationSyncProducer.class);
  private final ArrayList<
          BlockingQueue<Pair<ByteBuffer, OperationSyncPlanTypeUtils.OperationSyncPlanType>>>
      operationSyncQueues;
  private final OperationSyncLogService dmlLogService;

  public OperationSyncProducer(
      ArrayList<BlockingQueue<Pair<ByteBuffer, OperationSyncPlanTypeUtils.OperationSyncPlanType>>>
          operationSyncQueue,
      OperationSyncLogService operationSyncDMLLogService) {
    this.operationSyncQueues = operationSyncQueue;
    this.dmlLogService = operationSyncDMLLogService;
  }

  public void put(
      Pair<ByteBuffer, OperationSyncPlanTypeUtils.OperationSyncPlanType> planPair,
      String deviceName) {

    ByteBuffer headBuffer;
    headBuffer = planPair.left;
    headBuffer.position(0);
    int index;
    if (deviceName == null) {
      index = operationSyncQueues.size() - 1;
    } else {
      try {
        index = Math.abs(deviceName.hashCode()) % (operationSyncQueues.size() - 1);
      } catch (Exception e) {
        index = 0;
      }
    }
    if (operationSyncQueues.get(index).offer(planPair)) {
      return;
    }
    try {
      // must set buffer position to limit() before serialization
      headBuffer.position(headBuffer.limit());
      dmlLogService.acquireLogWriter();
      dmlLogService.write(headBuffer);
    } catch (IOException e) {
      LOGGER.error("OperationSyncConsumer can't serialize physicalPlan", e);
    } finally {
      dmlLogService.releaseLogWriter();
    }
  }
}
