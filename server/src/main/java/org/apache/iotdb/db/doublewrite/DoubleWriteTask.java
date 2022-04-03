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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** DoubleWriteTask is used for transmit one E-Plan sending by a client */
public class DoubleWriteTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteTask.class);

  private final ByteBuffer physicalPlanBuffer;
  private final SessionPool doubleWriteSessionPool;
  private final DoubleWriteEProtector eProtector;
  private final DoubleWriteLogService eLogService;

  public DoubleWriteTask(
      ByteBuffer physicalPlanBuffer,
      SessionPool doubleWriteSessionPool,
      DoubleWriteEProtector eProtector,
      DoubleWriteLogService eLogService) {
    this.physicalPlanBuffer = physicalPlanBuffer;
    this.doubleWriteSessionPool = doubleWriteSessionPool;
    this.eProtector = eProtector;
    this.eLogService = eLogService;
  }

  @Override
  public void run() {
    if (eProtector.isAtWork()) {
      serializeEPlan();
    } else {
      boolean transmitStatus = false;
      try {
        physicalPlanBuffer.position(0);
        transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(physicalPlanBuffer);
      } catch (IoTDBConnectionException connectionException) {
        // warn IoTDBConnectionException and do serialization
        LOGGER.warn("DoubleWriteTask can't transmit because network failure", connectionException);
      } catch (Exception e) {
        // The PhysicalPlan has internal error, reject transmit
        LOGGER.error("DoubleWriteTask can't transmit", e);
        return;
      }
      if (!transmitStatus) {
        serializeEPlan();
      }
    }
  }

  private void serializeEPlan() {
    // serialize the E-Plan if necessary
    try {
      // must set buffer position to limit() before serialization
      physicalPlanBuffer.position(physicalPlanBuffer.limit());
      eLogService.acquireLogWriter().write(physicalPlanBuffer);
    } catch (IOException e) {
      LOGGER.error("can't serialize current PhysicalPlan", e);
    }
    eLogService.releaseLogWriter();
  }
}
