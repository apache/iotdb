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

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.SingleFileLogReader;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class DoubleWriteEProtector extends DoubleWriteProtector {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteEProtector.class);

  private final SessionPool doubleWriteSessionPool;

  public DoubleWriteEProtector(SessionPool doubleWriteSessionPool) {
    super();
    this.doubleWriteSessionPool = doubleWriteSessionPool;
  }

  @Override
  protected void transmitLogFiles() {
    for (String logFileName : processingLogFiles) {
      File logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
      SingleFileLogReader logReader;
      try {
        logReader = new SingleFileLogReader(logFile);
      } catch (FileNotFoundException e) {
        LOGGER.error("DoubleWriteEProtector can't open DoubleWriteELog: {}, discarded", logFile.getAbsolutePath(), e);
        continue;
      }

      while (logReader.hasNext()) {
        // read and re-serialize the PhysicalPlan
        PhysicalPlan nextPlan = logReader.next();
        try {
          nextPlan.serialize(protectorSerializeStream);
        } catch (IOException e) {
          LOGGER.error("DoubleWriteEProtector can't serialize PhysicalPlan", e);
          continue;
        }
        ByteBuffer nextBuffer = ByteBuffer.wrap(protectorByteStream.toByteArray());
        protectorByteStream.reset();

        while (true) {
          // transmit E-Plan until it's been received
          boolean transmitStatus = false;

          try {
            // try double write
            nextBuffer.position(0);
            transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(nextBuffer);
          } catch (IoTDBConnectionException connectionException) {
            // warn IoTDBConnectionException and retry
            LOGGER.warn("DoubleWriteEProtector can't transmit, retrying...", connectionException);
          } catch (Exception e) {
            // error exception and break
            LOGGER.error("DoubleWriteEProtector can't transmit", e);
            break;
          }

          if (transmitStatus) {
            break;
          } else {
            try {
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
              LOGGER.warn("DoubleWriteEProtector is interrupted", e);
            }
          }
        }
      }

      logReader.close();
      try {
        // sleep one second then delete DoubleWriteLog
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        LOGGER.warn("DoubleWriteEProtector is interrupted", e);
      }

      for (int retryCnt = 0; retryCnt < 5; retryCnt++) {
        if (logFile.delete()) {
          LOGGER.info("DoubleWriteELog: {} is deleted.", logFile.getAbsolutePath());
          break;
        } else {
          LOGGER.warn("Delete DoubleWriteELog: {} failed. Retrying", logFile.getAbsolutePath());
        }
      }
      LOGGER.error("Couldn't delete DoubleWriteELog: {}", logFile.getAbsolutePath());
    }
  }

  public boolean isAtWork() {
    boolean result;
    atWorkLock.lock();
    result = isProtectorAtWork;
    atWorkLock.unlock();
    return result;
  }
}
