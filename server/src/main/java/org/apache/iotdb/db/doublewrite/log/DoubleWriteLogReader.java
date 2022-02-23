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
package org.apache.iotdb.db.doublewrite.log;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

/**
 * DoubleWriteLogWriter is used for reading PhysicalPlan that are persisted. DoubleWriteLogWriter
 * using sharded lock in order to ensure concurrent security of log files during reading
 */
public class DoubleWriteLogReader implements ILogReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteLogReader.class);

  private FileInputStream fileInputStream;
  private FileChannel readLogChannel;

  private int offset = 0;
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
  private static final int maxBlockSize = 1024;
  private final ByteBuffer blockBuffer = ByteBuffer.allocate(1024);

  public DoubleWriteLogReader(File logFile) {
    try {
      // open file channel
      fileInputStream = new FileInputStream(logFile);
      readLogChannel = fileInputStream.getChannel();
    } catch (IOException e) {
      LOGGER.error("DoubleWriteLogReader get double write log channel failed.", e);
    }
  }

  /* release file channel and output stream */
  @Override
  public void close() {
    try {
      readLogChannel.close();
      readLogChannel = null;
      fileInputStream.close();
      fileInputStream = null;
    } catch (IOException e) {
      LOGGER.error("close DoubleWriteLogReader failed.", e);
    }
  }

  @Override
  public boolean hasNext() throws FileNotFoundException {
    return false;
  }

  /* use hasNextBuffer() not hasNext() */
  public boolean hasNextBuffer() {
    try {
      return offset < readLogChannel.size();
    } catch (IOException e) {
      LOGGER.error("DoubleWriteLogReader hasNext() failed.", e);
    }
    return false;
  }

  @Override
  public PhysicalPlan next() throws FileNotFoundException {
    throw new FileNotFoundException("use nextBuffer() not next()");
  }

  /* use nextBuffer() not next() */
  public ByteBuffer nextBuffer() throws IOException {
    // demand shared lock for concurrent security
    FileLock lock;
    while (true) {
      try {
        lock = readLogChannel.tryLock(0, Long.MAX_VALUE, true);
        if (lock != null) {
          break;
        }
      } catch (Exception e) {
        // ignored
      }
    }

    try {
      // read the length of PhysicalPlan
      lengthBuffer.clear();
      offset += readLogChannel.read(lengthBuffer, offset);
      lengthBuffer.position(0);

      // read PhysicalPlan
      int bufferSize = lengthBuffer.getInt();
      ByteBuffer planBuffer = ByteBuffer.allocate(bufferSize);
      if (bufferSize <= maxBlockSize) {
        planBuffer = ByteBuffer.allocate(bufferSize);
        offset += readLogChannel.read(planBuffer, offset);
      } else {
        int remainSize = bufferSize;
        while (remainSize > 0) {
          blockBuffer.clear();
          int readSize = readLogChannel.read(blockBuffer, offset);
          blockBuffer.flip();

          if (readSize <= 0) {
            try {
              TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ignore) {
              // ignore
            }
          } else if (readSize < remainSize) {
            offset += readSize;
            remainSize -= readSize;
            blockBuffer.limit(readSize);
            planBuffer.put(blockBuffer);
          } else {
            offset += remainSize;
            blockBuffer.limit(remainSize);
            planBuffer.put(blockBuffer);
            remainSize = 0;
          }
        }
      }

      planBuffer.flip();
      lock.release();
      return planBuffer;
    } catch (IOException e) {
      LOGGER.error("DoubleWriteLogReader read failed.", e);
    }

    lock.release();
    return null;
  }
}
