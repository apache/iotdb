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

package org.apache.iotdb.commons.trigger.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.trigger.TriggerRegistrationInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

public class TriggerLogReader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TriggerLogReader.class);

  public static final int LEAST_LOG_SIZE = 12; // size + checksum

  private final DataInputStream logStream;
  private final String filepath;

  private final CRC32 checkSummer = new CRC32();

  // used to indicate the position of the broken log
  private int idx;
  // used to truncate the broken logs
  private long unbrokenLogsSize = 0;

  private BatchLogReader batchLogReader;

  private boolean fileCorrupted = false;

  public TriggerLogReader(File logFile) throws FileNotFoundException {
    close();
    logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    logger.info("open WAL file: {} size is {}", logFile.getName(), logFile.length());
    this.filepath = logFile.getPath();
    idx = 0;
  }

  public boolean hasNext() {
    try {
      if (batchLogReader != null && batchLogReader.hasNext()) {
        return true;
      }

      int logSize;
      try {
        logSize = logStream.readInt();
      } catch (EOFException e) {
        truncateBrokenLogs();
        return false;
      }

      if (logSize <= 0) {
        truncateBrokenLogs();
        return false;
      }
      byte[] buffer = new byte[logSize];

      int readLen = logStream.read(buffer, 0, logSize);
      if (readLen < logSize) {
        throw new IOException("Reach eof");
      }

      final long checkSum = logStream.readLong();
      checkSummer.reset();
      checkSummer.update(buffer, 0, logSize);
      if (checkSummer.getValue() != checkSum) {
        throw new IOException(
            String.format(
                "The check sum of the No.%d log batch is incorrect! In "
                    + "file: "
                    + "%d Calculated: %d.",
                idx, checkSum, checkSummer.getValue()));
      }

      batchLogReader = new BatchLogReader(ByteBuffer.wrap(buffer));
      if (!batchLogReader.isFileCorrupted()) {
        unbrokenLogsSize = unbrokenLogsSize + logSize + LEAST_LOG_SIZE;
      } else {
        truncateBrokenLogs();
      }
      fileCorrupted = fileCorrupted || batchLogReader.isFileCorrupted();
    } catch (Exception e) {
      logger.error(
          "Cannot read more TriggerRegistrationInformation from {}, successfully read index is {}. The reason is",
          idx,
          filepath,
          e);
      truncateBrokenLogs();
      fileCorrupted = true;
      return false;
    }
    return batchLogReader != null && batchLogReader.hasNext();
  }

  public TriggerRegistrationInformation next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    idx++;
    return batchLogReader.next();
  }

  @Override
  public void close() {
    if (logStream != null) {
      try {
        logStream.close();
      } catch (IOException e) {
        logger.error("Cannot close log file {}", filepath, e);
      }
    }
  }

  private void truncateBrokenLogs() {
    try (FileOutputStream outputStream = new FileOutputStream(filepath, true);
        FileChannel channel = outputStream.getChannel()) {
      channel.truncate(unbrokenLogsSize);
    } catch (IOException e) {
      logger.error("Fail to truncate log file to size {}", unbrokenLogsSize, e);
    }
  }

  private class BatchLogReader {
    private final Iterator<TriggerRegistrationInformation> infoIterator;
    private boolean fileCorrupted = false;

    public BatchLogReader(ByteBuffer buffer) {
      infoIterator = readLogs(buffer).iterator();
    }

    private List<TriggerRegistrationInformation> readLogs(ByteBuffer buffer) {
      List<TriggerRegistrationInformation> infoList = new ArrayList<>();
      while (buffer.position() != buffer.limit()) {
        try {
          infoList.add(TriggerRegistrationInformation.createFromBuffer(buffer));
        } catch (IllegalPathException | IOException e) {
          logger.error(
              "Cannot deserialize PhysicalPlans from ByteBuffer, ignore remaining logs", e);
          fileCorrupted = true;
          break;
        }
      }
      return infoList;
    }

    public boolean hasNext() {
      return infoIterator.hasNext();
    }

    public TriggerRegistrationInformation next() {
      return infoIterator.next();
    }

    public boolean isFileCorrupted() {
      return fileCorrupted;
    }
  }
}
