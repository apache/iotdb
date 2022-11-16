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
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.rpc.TSStatusCode.STORAGE_GROUP_NOT_READY;

public class OperationSyncDDLProtector extends OperationSyncProtector {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationSyncDDLProtector.class);;

  private final SessionPool operationSyncSessionPool;

  public OperationSyncDDLProtector(SessionPool operationSyncSessionPool) {
    super();
    this.operationSyncSessionPool = operationSyncSessionPool;
  }

  @Override
  protected void preCheck() {
    // do nothing
  }

  @Override
  protected void transmitPhysicalPlan(ByteBuffer planBuffer, PhysicalPlan physicalPlan) {
    long sleepTimeInSeconds = 1;
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
          LOGGER.warn(
              "OperationSyncDDLProtector can't transmit for connection error, retrying...",
              connectionException);
        } catch (BatchExecutionException batchExecutionException) {
          if (batchExecutionException.getStatusList().stream()
              .anyMatch(s -> s.getCode() == STORAGE_GROUP_NOT_READY.getStatusCode())) {
            LOGGER.warn(
                "OperationSyncDDLProtector can't transmit for STORAGE_GROUP_NOT_READY",
                batchExecutionException);
            sleepTimeInSeconds = 10;
          } else {
            LOGGER.warn(
                "OperationSyncDDLProtector can't transmit for batchExecutionException, discard it",
                batchExecutionException);
            break;
          }
        } catch (StatementExecutionException statementExecutionException) {
          if (statementExecutionException.getStatusCode()
              == STORAGE_GROUP_NOT_READY.getStatusCode()) {
            sleepTimeInSeconds = 10;
            LOGGER.warn(
                "OperationSyncDDLProtector can't transmit for STORAGE_GROUP_NOT_READY",
                statementExecutionException);
          } else {
            LOGGER.warn(
                "OperationSyncDDLProtector can't transmit for statementExecutionException, discard it",
                statementExecutionException);
            break;
          }
        } catch (Exception e) {
          // error exception and break
          LOGGER.error("OperationSyncDDLProtector can't transmit, discard it", e);
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
          TimeUnit.SECONDS.sleep(sleepTimeInSeconds);
        } catch (InterruptedException e) {
          LOGGER.warn("OperationSyncDDLProtector is interrupted", e);
        }
      }
    }
  }

  public boolean isAtWork() {
    return isProtectorAtWork;
  }
}
