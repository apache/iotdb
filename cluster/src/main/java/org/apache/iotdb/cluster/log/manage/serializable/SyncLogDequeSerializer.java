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
package org.apache.iotdb.cluster.log.manage.serializable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncLogDequeSerializer implements LogDequeSerializer {

  private static final Logger logger = LoggerFactory.getLogger(SyncLogDequeSerializer.class);

  File logFile;
  File metaFile;
  FileOutputStream logOutputStream;
  FileOutputStream metaOutputStream;
  LogParser parser = LogParser.getINSTANCE();
  private Deque<Integer> logSizeDeque = new ArrayDeque<>();
  private LogManagerMeta meta;
  // mark first log position
  private long firstLogPosition = 0;
  // removed log size
  private long removedLogSize = 0;
  // when the removedLogSize larger than this, we actually delete logs
  private long maxRemovedLogSize = ClusterDescriptor.getINSTANCE().getConfig()
      .getMaxRemovedLogSize();

  /**
   * for log tools
   *
   * @param logPath log dir path
   */
  public SyncLogDequeSerializer(String logPath) {
    logFile = SystemFileFactory.INSTANCE
        .getFile(logPath + File.separator + "logData");
    metaFile = SystemFileFactory.INSTANCE
        .getFile(logPath + File.separator + "logMeta");
    init();
  }

  /**
   * log in disk is size of log | log buffer meta in disk is firstLogPosition | size of log meta |
   * log meta buffer
   */
  public SyncLogDequeSerializer() {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    logFile = SystemFileFactory.INSTANCE
        .getFile(systemDir + File.separator + "raftLog" + File.separator + "logData");
    metaFile = SystemFileFactory.INSTANCE
        .getFile(systemDir + File.separator + "raftLog" + File.separator + "logMeta");
    init();
  }

  @TestOnly
  public void setMaxRemovedLogSize(long maxRemovedLogSize) {
    this.maxRemovedLogSize = maxRemovedLogSize;
  }

  public Deque<Integer> getLogSizeDeque() {
    return logSizeDeque;
  }

  // init output stream
  private void init() {
    try {
      if (!logFile.getParentFile().exists()) {
        logFile.getParentFile().mkdir();
        logFile.createNewFile();
        metaFile.createNewFile();
      }

      logOutputStream = new FileOutputStream(logFile, true);
      metaOutputStream = new FileOutputStream(metaFile, true);
    } catch (IOException e) {
      logger.error("Error in init log file: " + e.getMessage());
    }
  }

  @Override
  public void addLast(Log log, LogManagerMeta meta) {
    ByteBuffer data = log.serialize();
    int totalSize = 0;
    // write into disk
    try {
      totalSize = ReadWriteIOUtils.write(data, logOutputStream);
    } catch (IOException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }

    logSizeDeque.addLast(totalSize);
    serializeMeta(meta);
  }

  @Override
  public void removeLast(LogManagerMeta meta) {
    truncateLogIntern(1);
    serializeMeta(meta);
  }

  @Override
  public void truncateLog(int count, LogManagerMeta meta) {
    truncateLogIntern(count);
    serializeMeta(meta);
  }

  private void truncateLogIntern(int count) {
    if (logSizeDeque.size() > count) {
      throw new IllegalArgumentException("truncate log count is bigger than total log count");
    }

    int size = 0;
    for (int i = 0; i < count; i++) {
      size += logSizeDeque.removeLast();
    }
    // truncate file
    try {
      logOutputStream.getChannel().truncate(logFile.length() - size);
    } catch (IOException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }
  }

  @Override
  public void removeFirst(int num) {
    firstLogPosition += num;
    for (int i = 0; i < num; i++) {
      removedLogSize += logSizeDeque.removeFirst();
    }

    // do actual deletion
    if (removedLogSize > maxRemovedLogSize) {
      deleteRemovedLog();
    }

    // firstLogPosition changed
    serializeMeta(meta);
  }

  @Override
  public List<Log> recoverLog() {
    if (meta == null) {
      recoverMeta();
    }

    if (!logFile.exists()) {
      return new ArrayList<>();
    }

    List<Log> result = new ArrayList<>();
    try {
      FileInputStream logReader = new FileInputStream(logFile);
      FileChannel logChannel = logReader.getChannel();
      long actuallySkippedBytes = logReader.skip(removedLogSize);
      if (actuallySkippedBytes != removedLogSize) {
        logger.error(
            "Error in log serialization, skipped file length isn't consistent with removedLogSize!");
        return result;
      }
      while (logChannel.position() < logFile.length()) {
        // actual log
        Log log = readLog(logReader);
        result.add(log);
      }
      logReader.close();
    } catch (IOException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }

    return result;
  }

  // read single log
  private Log readLog(FileInputStream logReader) throws IOException {
    int logSize = ReadWriteIOUtils.readInt(logReader);
    int totalSize = Integer.BYTES + logSize;

    Log log = null;

    try {
      log = parser.parse(ByteBuffer.wrap(ReadWriteIOUtils.readBytes(logReader, logSize)));
    } catch (UnknownLogTypeException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }

    logSizeDeque.addLast(totalSize);

    return log;
  }

  @Override
  public LogManagerMeta recoverMeta() {
    if (meta == null && metaFile.exists() && metaFile.length() > 0) {
      try {
        FileInputStream metaReader = new FileInputStream(metaFile);
        firstLogPosition = ReadWriteIOUtils.readLong(metaReader);
        removedLogSize = ReadWriteIOUtils.readLong(metaReader);
        meta = LogManagerMeta.deserialize(
            ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(metaReader)));
        metaReader.close();
      } catch (IOException e) {
        logger.error("Error in log serialization: " + e.getMessage());
      }
    }

    return meta;
  }

  @Override
  public void serializeMeta(LogManagerMeta meta) {
    try {
      metaOutputStream.getChannel().truncate(0);
      ReadWriteIOUtils.write(firstLogPosition, metaOutputStream);
      ReadWriteIOUtils.write(removedLogSize, metaOutputStream);
      ReadWriteIOUtils.write(meta.serialize(), metaOutputStream);

      this.meta = meta;
    } catch (IOException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }

  }

  @Override
  public void close() {
    try {
      logOutputStream.close();
      metaOutputStream.close();
    } catch (IOException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }
  }

  // actually delete removed logs, this may take lots of times
  // TODO : use an async method to delete file
  private void deleteRemovedLog() {
    try {
      FileInputStream reader = new FileInputStream(logFile);
      // skip removed file
      long actuallySkippedBytes = reader.skip(removedLogSize);
      if (actuallySkippedBytes != removedLogSize) {
        logger.error(
            "Error in log serialization, skipped file length isn't consistent with removedLogSize!");
        return;
      }

      // begin to write
      File tempLogFile = SystemFileFactory.INSTANCE
          .getFile(
              IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "raftLog"
                  + File.separator + "logData.temp");

      logOutputStream.close();
      logOutputStream = new FileOutputStream(tempLogFile);
      int blockSize = 4096;
      long curPosition = reader.getChannel().position();
      while (curPosition < logFile.length()) {
        long transferSize = Math.min(blockSize, logFile.length() - curPosition);
        reader.getChannel().transferTo(curPosition, transferSize, logOutputStream.getChannel());
        curPosition += transferSize;
      }

      // rename file
      logFile.delete();
      tempLogFile.renameTo(logFile);
      logOutputStream.close();
      reader.close();
      logOutputStream = new FileOutputStream(logFile, true);
    } catch (IOException e) {
      logger.error("Error in log serialization: " + e.getMessage());
    }

    // re init and serialize
    removedLogSize = 0;
    firstLogPosition = 0;
    // meta will be serialized by caller
  }
}
