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
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DoubleWriteConsumer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteConsumer.class);

  private final BlockingQueue<ByteBuffer> doubleWriteQueue;
  private final SessionPool doubleWriteSessionPool;

  public DoubleWriteConsumer(
    BlockingQueue<ByteBuffer> doubleWriteQueue, SessionPool doubleWriteSessionPool) {
    this.doubleWriteQueue = doubleWriteQueue;
    this.doubleWriteSessionPool = doubleWriteSessionPool;
  }

  @Override
  public void run() {
    while (true) {
      ByteBuffer head;
      try {
        head = doubleWriteQueue.take();
      } catch (InterruptedException e) {
        LOGGER.error("There is an exception in DoubleWriteConsumer: ", e);
        continue;
      }

      // transmit PhysicalPlan until it has been received
      while (true) {
        boolean transmitStatus = false;

        try {
          head.position(0);
          transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(head);
        } catch (IoTDBConnectionException ignore) {
          // ignore exception and retry
        } catch (StatementExecutionException e) {
          LOGGER.error("DoubleWrite can't transmit: ", e);
          break;
        }

        if (transmitStatus) {
          break;
        } else {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ignore) {
            // ignore and retry after one second
          }
        }
      }
    }
  }
}
