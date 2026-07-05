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
package org.apache.iotdb.confignode.writelog.io;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

public class SingleFileLogReader implements ILogReader {
  private static final Logger logger = LoggerFactory.getLogger(SingleFileLogReader.class);
  public static final int LEAST_LOG_SIZE = 12; // size + checksum

  private DataInputStream logStream;
  private String filepath;

  private byte[] buffer;
  private CRC32 checkSummer = new CRC32();

  // used to indicate the position of the broken log
  private int idx;
  // used to truncate the broken logs
  private long unbrokenLogsSize = 0;

  private BatchLogReader batchLogReader;

  private boolean fileCorrupted = false;

  public SingleFileLogReader(File logFile) throws FileNotFoundException {
    open(logFile);
  }

  @Override
  public boolean hasNext() {
    try {
      if (batchLogReader != null && batchLogReader.hasNext()) {
        return true;
      }

      if (logStream.available() < LEAST_LOG_SIZE) {
        return false;
      }

      int logSize = logStream.readInt();
      if (logSize <= 0) {
        return false;
      }
      buffer = new byte[logSize];

      logStream.readFully(buffer, 0, logSize);

      final long checkSum = logStream.readLong();
      checkSummer.reset();
      checkSummer.update(buffer, 0, logSize);
      if (checkSummer.getValue() != checkSum) {
        throw new IOException(
            String.format(
                ConfigNodeMessages.THE_CHECK_SUM_OF_THE_NO_LOG_BATCH_IS_INCORRECT
                    + ConfigNodeMessages.EXCEPTION_FILE_11296840
                    + ConfigNodeMessages.EXCEPTION_ARG_CALCULATED_ARG_0EEEE191,
                idx,
                checkSum,
                checkSummer.getValue()));
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
          ConfigNodeMessages
              .LOG_CANNOT_READ_MORE_PHYSICALPLANS_ARG_SUCCESSFULLY_READ_INDEX_ARG_REASON_2EC90E78,
          idx,
          filepath,
          e);
      truncateBrokenLogs();
      fileCorrupted = true;
      return false;
    }
    return true;
  }

  @Override
  public ConfigPhysicalPlan next() {
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
        logger.error(ConfigNodeMessages.CANNOT_CLOSE_LOG_FILE, filepath, e);
      }
    }
  }

  public void open(File logFile) throws FileNotFoundException {
    close();
    logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    logger.info(ConfigNodeMessages.OPEN_WAL_FILE_SIZE_IS, logFile.getName(), logFile.length());
    this.filepath = logFile.getPath();
    idx = 0;
  }

  public boolean isFileCorrupted() {
    return fileCorrupted;
  }

  private void truncateBrokenLogs() {
    try (FileOutputStream outputStream = new FileOutputStream(filepath, true);
        FileChannel channel = outputStream.getChannel()) {
      channel.truncate(unbrokenLogsSize);
    } catch (IOException e) {
      logger.error(ConfigNodeMessages.FAIL_TO_TRUNCATE_LOG_FILE_TO_SIZE, unbrokenLogsSize, e);
    }
  }
}
