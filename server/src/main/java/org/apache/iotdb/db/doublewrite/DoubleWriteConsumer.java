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
package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DoubleWriteConsumer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteConsumer.class);

  private final BlockingQueue<Pair<ByteBuffer, DoubleWritePlanTypeUtils.DoubleWritePlanType>>
      doubleWriteQueue;
  private final SessionPool doubleWriteSessionPool;
  private final DoubleWriteLogService niLogService;

  public DoubleWriteConsumer(
      BlockingQueue<Pair<ByteBuffer, DoubleWritePlanTypeUtils.DoubleWritePlanType>>
          doubleWriteQueue,
      SessionPool doubleWriteSessionPool,
      DoubleWriteLogService niLogService) {
    this.doubleWriteQueue = doubleWriteQueue;
    this.doubleWriteSessionPool = doubleWriteSessionPool;
    this.niLogService = niLogService;
  }

  @Override
  public void run() {
    while (true) {
      Pair<ByteBuffer, DoubleWritePlanTypeUtils.DoubleWritePlanType> head;
      ByteBuffer headBuffer;
      DoubleWritePlanTypeUtils.DoubleWritePlanType headType;
      try {
        head = doubleWriteQueue.take();
        headBuffer = head.left;
        headType = head.right;
      } catch (InterruptedException e) {
        LOGGER.error("DoubleWriteConsumer been interrupted: ", e);
        continue;
      }

      if (headType == DoubleWritePlanTypeUtils.DoubleWritePlanType.IPlan) {
        try {
          // Sleep 10ms when it's I-Plan
          TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException ignore) {
          // Ignored
        }
      }

      headBuffer.position(0);
      boolean transmitStatus = false;
      try {
        headBuffer.position(0);
        transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(headBuffer);
      } catch (IoTDBConnectionException connectionException) {
        // warn IoTDBConnectionException and do serialization
        LOGGER.warn(
            "DoubleWriteConsumer can't transmit because network failure", connectionException);
      } catch (Exception e) {
        // The PhysicalPlan has internal error, reject transmit
        LOGGER.error("DoubleWriteConsumer can't transmit", e);
        continue;
      }

      if (!transmitStatus) {
        try {
          // must set buffer position to limit() before serialization
          headBuffer.position(headBuffer.limit());
          niLogService.acquireLogWriter().write(headBuffer);
        } catch (IOException e) {
          LOGGER.error("DoubleWriteConsumer can't serialize physicalPlan", e);
        }
        niLogService.releaseLogWriter();
      }
    }
  }
}
