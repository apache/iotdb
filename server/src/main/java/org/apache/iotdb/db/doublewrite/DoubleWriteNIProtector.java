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
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class DoubleWriteNIProtector extends DoubleWriteProtector {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteNIProtector.class);

  private final DoubleWriteEProtector eProtector;
  private final DoubleWriteProducer producer;

  public DoubleWriteNIProtector(DoubleWriteEProtector eProtector, DoubleWriteProducer producer) {
    super();
    this.eProtector = eProtector;
    this.producer = producer;
  }

  @Override
  protected void transmitLogFiles() {
    // Make sure the DoubleWriteEProtector is not at work
    checkEProtector();

    for (String logFileName : processingLogFiles) {
      File logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
      SingleFileLogReader logReader;
      try {
        logReader = new SingleFileLogReader(logFile);
      } catch (FileNotFoundException e) {
        LOGGER.error(
            "DoubleWriteNIProtector can't open DoubleWriteNILog: {}, discarded",
            logFile.getAbsolutePath(),
            e);
        continue;
      }

      while (logReader.hasNext()) {
        // read and re-serialize the PhysicalPlan
        PhysicalPlan nextPlan = logReader.next();
        try {
          nextPlan.serialize(protectorSerializeStream);
        } catch (IOException e) {
          LOGGER.error("DoubleWriteNIProtector can't serialize PhysicalPlan", e);
          continue;
        }
        ByteBuffer nextBuffer = ByteBuffer.wrap(protectorByteStream.toByteArray());
        protectorByteStream.reset();

        producer.put(
            new Pair<>(nextBuffer, DoubleWritePlanTypeUtils.getDoubleWritePlanType(nextPlan)));
      }

      logReader.close();
      try {
        // sleep one second then delete DoubleWriteLog
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        LOGGER.warn("DoubleWriteNIProtector is interrupted", e);
      }

      boolean deleted = false;
      for (int retryCnt = 0; retryCnt < 5; retryCnt++) {
        if (logFile.delete()) {
          deleted = true;
          LOGGER.info("DoubleWriteNILog: {} is deleted.", logFile.getAbsolutePath());
          break;
        } else {
          LOGGER.warn("Delete DoubleWriteNILog: {} failed. Retrying", logFile.getAbsolutePath());
        }
      }
      if (!deleted) {
        LOGGER.error("Couldn't delete DoubleWriteNILog: {}", logFile.getAbsolutePath());
      }
    }
  }

  private void checkEProtector() {
    while (eProtector.isAtWork()) {
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException ignore) {
        // ignore and retry
      }
    }
  }
}
