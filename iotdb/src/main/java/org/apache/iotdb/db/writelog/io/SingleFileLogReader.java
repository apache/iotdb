/**
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
package org.apache.iotdb.db.writelog.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleFileLogReader implements ILogReader {

  private static final Logger logger = LoggerFactory.getLogger(SingleFileLogReader.class);
  public static final int LEAST_LOG_SIZE = 12; // size + checksum
  private DataInputStream logStream;
  private String filepath;
  private int bufferSize = 4 * 1024 * 1024;
  private byte[] buffer = new byte[bufferSize];
  private CRC32 checkSummer = new CRC32();
  private PhysicalPlan planBuffer = null;

  public SingleFileLogReader(File logFile) throws FileNotFoundException {
    open(logFile);
  }

  @Override
  public boolean hasNext() throws IOException{
    if (planBuffer != null) {
      return true;
    }

    if (logStream.available() < LEAST_LOG_SIZE) {
      return false;
    }

    int logSize = logStream.readInt();
    if (logSize > bufferSize) {
      bufferSize = logSize;
      buffer = new byte[bufferSize];
    }
    final long checkSum = logStream.readLong();
    logStream.read(buffer, 0, logSize);
    checkSummer.reset();
    checkSummer.update(buffer, 0, logSize);
    if (checkSummer.getValue() != checkSum) {
      throw new IOException("The check sum is incorrect!");
    }
    planBuffer = PhysicalPlanLogTransfer.logToPlan(buffer);
    return true;
  }

  @Override
  public PhysicalPlan next() throws IOException {
    if (!hasNext()){
      throw new NoSuchElementException();
    }

    PhysicalPlan ret = planBuffer;
    planBuffer = null;
    return ret;
  }

  @Override
  public void close() {
    if (logStream != null) {
      try {
        logStream.close();
      } catch (IOException e) {
        logger.error("Cannot setCloseMark log file {}", filepath, e);
      }
    }
  }

  public void open(File logFile) throws FileNotFoundException {
    logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.filepath = logFile.getPath();
  }
}
