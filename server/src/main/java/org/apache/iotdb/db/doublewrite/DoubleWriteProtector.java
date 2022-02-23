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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.doublewrite.log.DoubleWriteLogReader;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/** DoubleWriteProtector is used for transmit data in a DoubleWriteLog */
public class DoubleWriteProtector implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteProtector.class);

  private final SessionPool doubleWriteSessionPool;

  private final File logFile;
  private final DoubleWriteLogReader doubleWriteLogReader;

  private final int maxLogSize =
      IoTDBDescriptor.getInstance().getConfig().getDoubleWriteMaxLogSize();

  DoubleWriteProtector(File logFile, SessionPool doubleWriteSessionPool) {
    this.doubleWriteSessionPool = doubleWriteSessionPool;
    this.logFile = logFile;
    doubleWriteLogReader = new DoubleWriteLogReader(logFile);
  }

  @Override
  public void run() {
    while (logFile.length() <= maxLogSize || doubleWriteLogReader.hasNextBuffer()) {
      // When DoubleWriteConsumer is running healthily,
      // there are barely PhysicalPlan needs DoubleWriteProtector to transmit.
      // So the Protector should in dormant
      while (logFile.exists() && !doubleWriteLogReader.hasNextBuffer()) {
        try {
          TimeUnit.MINUTES.sleep(1);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }

      // close DoubleWriteProtector when logFile doesn't exist
      if (!logFile.exists()) {
        return;
      }

      // read then transmit the PhysicalPlan until it has been received
      ByteBuffer nextBuffer = null;
      try {
        nextBuffer = doubleWriteLogReader.nextBuffer();
      } catch (IOException e) {
        LOGGER.error("There is an exception during deserialize: ", e);
      }
      // close DoubleWriteProtector when logFile doesn't exist
      if (nextBuffer == null) {
        return;
      }

      nextBuffer.position(0);
      while (true) {
        boolean transmitStatus;

        try {
          // try double write
          transmitStatus = doubleWriteSessionPool.doubleWriteTransmit(nextBuffer);
        } catch (IoTDBConnectionException ignore) {
          // ignore exception and retry
          continue;
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
