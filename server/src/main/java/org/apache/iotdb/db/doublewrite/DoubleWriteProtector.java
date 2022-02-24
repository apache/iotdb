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

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.SingleFileLogReader;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/** DoubleWriteProtector is used for transmit data in a DoubleWriteLog */
public class DoubleWriteProtector implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteProtector.class);

  private final SessionPool doubleWriteSessionPool;

  private final File logFile;
  private SingleFileLogReader doubleWriteLogReader;

  private static final int MAX_PHYSICALPLAN_SIZE = 16 * 1024 * 1024;
  private final ByteArrayOutputStream doubleWriteByteStream;
  private final DataOutputStream doubleWriteSerializeStream;

  DoubleWriteProtector(File logFile, SessionPool doubleWriteSessionPool) {
    this.doubleWriteSessionPool = doubleWriteSessionPool;
    this.logFile = logFile;

    // For serialize PhysicalPlan
    doubleWriteByteStream = new ByteArrayOutputStream(MAX_PHYSICALPLAN_SIZE);
    doubleWriteSerializeStream = new DataOutputStream(doubleWriteByteStream);

    // init DoubleWriteLog reader
    try {
      doubleWriteLogReader = new SingleFileLogReader(logFile);
    } catch (FileNotFoundException ignore) {
      doubleWriteLogReader = null;
    }
  }

  @Override
  public void run() {
    if (doubleWriteLogReader == null) {
      return;
    }

    while (doubleWriteLogReader.hasNext()) {
      // read and re-serialize the PhysicalPlan
      PhysicalPlan nextPlan = doubleWriteLogReader.next();
      try {
        nextPlan.serialize(doubleWriteSerializeStream);
      } catch (IOException e) {
        LOGGER.error("DoubleWriteProtector can't serialize PhysicalPlan", e);
        continue;
      }
      ByteBuffer nextBuffer = ByteBuffer.wrap(doubleWriteByteStream.toByteArray());
      doubleWriteByteStream.reset();

      while (true) {
        // transmit DoubleWriteRequest until it's been received
        boolean transmitStatus = false;

        try {
          // try double write
          nextBuffer.position(0);
          transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(nextBuffer);
        } catch (IoTDBConnectionException ignore) {
          // ignore exception and execute the following statements to retry
        } catch (StatementExecutionException e) {
          LOGGER.error("double write can't transmit: ", e);
          break;
        }

        if (transmitStatus) {
          break;
        } else {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ignore) {
            // ignore
          }
        }
      }
    }

    doubleWriteLogReader.close();
    try {
      // sleep one second then delete DoubleWriteLog file
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException ignore) {
      // ignore
    }
    while (true) {
      if (logFile.delete()) {
        LOGGER.info("DoubleWrite log file {} is deleted.", logFile.getName());
        break;
      } else {
        LOGGER.warn("delete DoubleWrite log file {} failed. Retrying", logFile.getName());
      }
    }
  }
}
