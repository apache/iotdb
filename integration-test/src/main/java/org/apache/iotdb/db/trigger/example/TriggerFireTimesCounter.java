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

package org.apache.iotdb.db.trigger.example;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TriggerFireTimesCounter implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerFireTimesCounter.class);
  private String TXT_PATH;

  private final int LOCK_FILE_RETRY_TIME = 10;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    String counterName = attributes.getString("name");
    TXT_PATH =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + counterName
            + ".txt";
    Path path = Paths.get(TXT_PATH);
    Files.deleteIfExists(path);
    try {
      Files.createFile(path);
    } catch (FileAlreadyExistsException ignore) {
      // do nothing
    }
    Files.write(Paths.get(TXT_PATH), String.valueOf(0).getBytes());
  }

  @Override
  public void onDrop() throws Exception {
    Files.deleteIfExists(Paths.get(TXT_PATH));
  }

  @Override
  public boolean fire(Tablet tablet) throws Exception {
    FileLock fileLock = null;
    FileChannel fileChannel = null;
    int retryNum = 0;
    try {
      fileChannel = FileChannel.open(Paths.get(TXT_PATH), StandardOpenOption.APPEND);
      while (fileLock == null) {
        fileLock = fileChannel.tryLock();
        if (fileLock == null) {
          if (retryNum++ >= LOCK_FILE_RETRY_TIME) {
            break;
          }
          Thread.sleep(100);
        }
      }
      int rows = tablet.getRowSize();
      if (fileLock != null && fileLock.isValid()) {
        String records = System.lineSeparator() + rows;
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        byteBuffer.put(records.getBytes());
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
          fileChannel.write(byteBuffer);
        }
      }
    } catch (Throwable t) {
      LOGGER.warn("TriggerFireTimesCounter error", t);
      return false;
    } finally {
      if (fileLock != null) {
        fileLock.close();
      }
      if (fileChannel != null) {
        fileChannel.close();
      }
    }
    return true;
  }
}
