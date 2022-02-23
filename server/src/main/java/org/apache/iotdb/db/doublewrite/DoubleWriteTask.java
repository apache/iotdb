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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** DoubleWriteTask is used for transmit one InsertPlan sending by a client */
public class DoubleWriteTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteTask.class);

  private final DoubleWriteProtectorService protectorService;
  private final ByteBuffer physicalPlanBuffer;
  private final SessionPool doubleWriteSessionPool;

  public DoubleWriteTask(
      DoubleWriteProtectorService protectorService,
      ByteBuffer physicalPlanBuffer,
      SessionPool doubleWriteSessionPool) {
    this.protectorService = protectorService;
    this.physicalPlanBuffer = physicalPlanBuffer;
    this.doubleWriteSessionPool = doubleWriteSessionPool;
  }

  @Override
  public void run() {
    boolean transmitStatus = false;
    try {
      transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(physicalPlanBuffer);
    } catch (StatementExecutionException e) {
      LOGGER.error("double write can't transmit: ", e);
    } catch (IoTDBConnectionException ignore) {
      // ignore and execute following statements
    }

    // serialize the PhysicalPlan if transition failed
    if (!transmitStatus) {
      try {
        protectorService.acquireLogWriter().write(physicalPlanBuffer);
        protectorService.releaseLogWriter();
      } catch (IOException e) {
        LOGGER.error("can't serialize current PhysicalPlan", e);
      }
    }
  }
}
