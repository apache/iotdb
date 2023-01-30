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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.pool.SessionPool;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class OperationSyncDMLProtector extends OperationSyncProtector {

  private final OperationSyncDDLProtector ddlProtector;
  private final SessionPool operationSyncSessionPool;

  public OperationSyncDMLProtector(
      OperationSyncDDLProtector ddlProtector, SessionPool operationSyncSessionPool) {
    super();
    this.ddlProtector = ddlProtector;
    this.operationSyncSessionPool = operationSyncSessionPool;
  }

  @Override
  protected void preCheck() {
    while (ddlProtector.isAtWork()) {
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException ignore) {
        // ignore and retry
      }
    }
  }

  @Override
  protected void transmitPhysicalPlan(ByteBuffer planBuffer, PhysicalPlan physicalPlan) {
    while (true) {
      // transmit E-Plan until it's been received
      boolean transmitStatus = false;
      if (StorageEngine.isSecondaryAlive().get()) {
        try {
          // try operation sync
          planBuffer.position(0);
          transmitStatus = operationSyncSessionPool.operationSyncTransmit(planBuffer);
        } catch (IoTDBConnectionException connectionException) {
          // warn IoTDBConnectionException and retry
          LOGGER.warn("OperationSyncDMLProtector can't transmit, retrying...", connectionException);
        } catch (Exception e) {
          // error exception and break
          LOGGER.error("OperationSyncDMLProtector can't transmit", e);
          break;
        }
      } else {
        try {
          TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if (transmitStatus) {
        break;
      } else {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          LOGGER.warn("OperationSyncDMLProtector is interrupted", e);
        }
      }
    }
  }
}
