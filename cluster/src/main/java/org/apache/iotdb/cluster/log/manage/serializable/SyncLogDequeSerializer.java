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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.StableEntryManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncLogDequeSerializer implements StableEntryManager {

  private static final Logger logger = LoggerFactory.getLogger(SyncLogDequeSerializer.class);

  List<File> logFileList;
  LogParser parser = LogParser.getINSTANCE();
  private File metaFile;
  private FileOutputStream currentLogOutputStream;
  private Deque<Integer> logSizeDeque = new ArrayDeque<>();
  private LogManagerMeta meta;
  private HardState state;
  // mark first log position
  private long firstLogPosition = 0;
  // removed log size
  private long removedLogSize = 0;
  // when the removedLogSize larger than this, we actually delete logs
  private long maxRemovedLogSize = ClusterDescriptor.getInstance().getConfig()
      .getMaxRemovedLogSize();
  // min version of available log
  private long minAvailableVersion = 0;
  // max version of available log
  private long maxAvailableVersion = Long.MAX_VALUE;
  // log dir
  private String logDir;
  // version controller
  private VersionController versionController;

  /**
   * for log tools
   *
   * @param logPath log dir path
   */
  public SyncLogDequeSerializer(String logPath) {
    logFileList = new ArrayList<>();
    logDir = logPath + File.separator;
    init();
  }

  /**
   * log in disk is [size of log1 | log1 buffer] [size of log2 | log2 buffer]... log meta buffer
   * build serializer with node id
   */
  public SyncLogDequeSerializer(int nodeIdentifier) {
    logFileList = new ArrayList<>();
    logDir = getLogDir(nodeIdentifier);
    try {
      versionController = new SimpleFileVersionController(logDir);
    } catch (IOException e) {
      logger.error("log serializer build version controller failed");
    }
    init();
  }

  public static String getLogDir(int nodeIdentifier) {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    return systemDir + File.separator + "raftLog" + File.separator +
        nodeIdentifier + File.separator;
  }

  @TestOnly
  public String getLogDir() {
    return logDir;
  }

  @TestOnly
  public File getMetaFile() {
    return metaFile;
  }

  @TestOnly
  public void setMaxRemovedLogSize(long maxRemovedLogSize) {
    this.maxRemovedLogSize = maxRemovedLogSize;
  }

  public List<Log> getAllEntries() {
    return recoverLog();
  }

  public void append(List<Log> entries) {
    Log entry = entries.get(entries.size() - 1);
    meta.setCommitLogIndex(entry.getCurrLogIndex());
    meta.setCommitLogTerm(entry.getCurrLogTerm());
    meta.setLastLogIndex(entry.getCurrLogIndex());
    meta.setLastLogTerm(entry.getCurrLogTerm());
    append(entries, meta);
  }

  public void setHardStateAndFlush(HardState state) {
    this.state = state;
    serializeMeta(meta);
  }

  public HardState getHardState() {
    return state;
  }


  public void applyingSnapshot(Snapshot snapshot) {

  }

  public void removeCompactedEntries(long index) {

  }


  public Deque<Integer> getLogSizeDeque() {
    return logSizeDeque;
  }

  // init output stream
  private void init() {
    recoverMetaFile();
    recoverMeta();
    try {
      File[] logFiles = metaFile.getParentFile().listFiles();
      if (logger.isInfoEnabled()) {
        logger.info("Find log files {}", logFiles != null ? Arrays.asList(logFiles) :
            Collections.emptyList());
      }

      if (logFiles != null) {
        for (File file : logFiles) {
          if (file.length() == 0) {
            file.delete();
            continue;
          }

          if (file.getName().startsWith("data")) {
            long fileVersion = getFileVersion(file);
            // this means system down between save meta and data
            if (fileVersion <= minAvailableVersion || fileVersion >= maxAvailableVersion) {
              file.delete();
            } else {
              logFileList.add(file);
            }
          }
        }
      }

      logFileList.sort(Comparator.comparingLong(o -> Long.parseLong(o.getName().split("-")[1])));

      // add init log file
      if (logFileList.isEmpty()) {
        logFileList.add(createNewLogFile(metaFile.getParentFile().getPath()));
      }

    } catch (IOException e) {
      logger.error("Error in init log file: ", e);
    }
  }

  private void recoverMetaFile() {
    metaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta");

    // build dir
    if (!metaFile.getParentFile().exists()) {
      metaFile.getParentFile().mkdirs();
    }

    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
    // if we have temp file
    if (tempMetaFile.exists()) {
      // if temp file is empty, just return
      if (tempMetaFile.length() == 0) {
        tempMetaFile.delete();
      }
      // else use temp file rather than meta file
      else {
        if (metaFile.exists()) {
          metaFile.delete();
        }
        tempMetaFile.renameTo(metaFile);
      }
    } else if (!metaFile.exists()) {
      try {
        metaFile.createNewFile();
      } catch (IOException e) {
        logger.error("Error in log serialization: ",  e);
      }
    }
  }

  private void checkStream() throws FileNotFoundException {
    if (currentLogOutputStream == null) {
      currentLogOutputStream = new FileOutputStream(getCurrentLogFile(), true);
    }
  }

  private File createNewLogFile(String dirName) throws IOException {
    File logFile = SystemFileFactory.INSTANCE
        .getFile(dirName + File.separator + "data" + "-" + versionController.nextVersion());
    logFile.createNewFile();
    return logFile;
  }

  public void append(Log log, LogManagerMeta meta) {
    ByteBuffer data = log.serialize();
    int totalSize = 0;
    // write into disk
    try {
      checkStream();
      totalSize = ReadWriteIOUtils.write(data, currentLogOutputStream);
    } catch (IOException e) {
      logger.error("Error in log serialization: ",  e);
    }

    logSizeDeque.addLast(totalSize);
    serializeMeta(meta);
  }

  public void append(List<Log> logs, LogManagerMeta meta) {
    int bufferSize = 0;
    List<ByteBuffer> bufferList = new ArrayList<>(logs.size());
    for (Log log : logs) {
      ByteBuffer data = log.serialize();
      int size = data.capacity() + Integer.BYTES;
      logSizeDeque.addLast(size);
      bufferSize += size;

      bufferList.add(data);
    }

    ByteBuffer finalBuffer = ByteBuffer.allocate(bufferSize);
    for (ByteBuffer byteBuffer : bufferList) {
      finalBuffer.putInt(byteBuffer.capacity());
      finalBuffer.put(byteBuffer.array());
    }

    // write into disk
    try {
      checkStream();
      ReadWriteIOUtils.writeWithoutSize(finalBuffer, currentLogOutputStream);
    } catch (IOException e) {
      logger.error("Error in log serialization: ",  e);
    }

    serializeMeta(meta);
  }

  public void removeLast(LogManagerMeta meta) {
    truncateLogIntern(1);
    serializeMeta(meta);
  }

  public void truncateLog(int count, LogManagerMeta meta) {
    truncateLogIntern(count);
    serializeMeta(meta);
  }

  private File getCurrentLogFile() {
    return logFileList.get(logFileList.size() - 1);
  }

  private void truncateLogIntern(int count) {
    if (logSizeDeque.size() < count) {
      throw new IllegalArgumentException("truncate log count is bigger than total log count");
    }

    int size = 0;
    for (int i = 0; i < count; i++) {
      size += logSizeDeque.removeLast();
    }
    // truncate file
    while (size > 0) {
      File currentLogFile = getCurrentLogFile();
      // if the last file is smaller than truncate size, we can delete it directly
      if (currentLogFile.length() < size) {
        size -= currentLogFile.length();
        try {
          if (currentLogOutputStream != null) {
            currentLogOutputStream.close();
          }
          // if system down before delete, we can use this to delete file during recovery
          maxAvailableVersion = getFileVersion(currentLogFile);
          serializeMeta(meta);

          currentLogFile.delete();
          logFileList.remove(logFileList.size() - 1);
          logFileList.add(createNewLogFile(logDir));
        } catch (IOException e) {
          logger.error("Error in log serialization: ", e);
        }

        logFileList.remove(logFileList.size() - 1);
      }
      // else we just truncate it
      else {
        try {
          checkStream();
          currentLogOutputStream.getChannel().truncate(getCurrentLogFile().length() - size);
          break;
        } catch (IOException e) {
          logger.error("Error in log serialization: ", e);
        }
      }
    }

  }

  public void removeFirst(int num) {
    firstLogPosition += num;
    for (int i = 0; i < num; i++) {
      removedLogSize += logSizeDeque.removeFirst();
    }

    // firstLogPosition changed
    serializeMeta(meta);
    // TODO-Cluster: change to debug or remove
    logger.info("Log size after removal: {}/{}", removedLogSize, maxRemovedLogSize);
    // do actual deletion
    if (removedLogSize > maxRemovedLogSize) {
      openNewLogFile();
    }
  }

  public List<Log> recoverLog() {
    // if we can totally remove some old file, remove them
    if (removedLogSize > 0) {
      actuallyDeleteFile();
    }

    List<Log> result = new ArrayList<>();
    // skip removal file
    boolean shouldSkip = true;

    for (File logFile : logFileList) {
      int logNumInFile = 0;
      if (logFile.length() == 0) {
        continue;
      }

      try (FileInputStream logReader = new FileInputStream(logFile)) {
        FileChannel logChannel = logReader.getChannel();
        if (shouldSkip) {
          long actuallySkippedBytes = logReader.skip(removedLogSize);
          if (actuallySkippedBytes != removedLogSize) {
            logger.info(
                "Error in log serialization, skipped file length isn't consistent with removedLogSize!");
            return result;
          }
          shouldSkip = false;
        }

        while (logChannel.position() < logFile.length()) {
          // actual log
          Log log = readLog(logReader);
          result.add(log);
          logNumInFile ++;
        }
      } catch (IOException e) {
        logger.error("Error in log serialization: ", e);
      }
      logger.info("Recovered {} logs from {}", logNumInFile, logFile);
    }
    logger.info("Recovered {} logs totally", result.size());
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
      logger.error("Error in log serialization: ",  e);
    }

    logSizeDeque.addLast(totalSize);

    return log;
  }

  public LogManagerMeta recoverMeta() {
    if (meta == null) {
      if (metaFile.exists() && metaFile.length() > 0) {
        if (logger.isInfoEnabled()) {
          logger.info("MetaFile {} exists, last modified: {}", metaFile.getPath(),
              new Date(metaFile.lastModified()));
        }
        try (FileInputStream metaReader = new FileInputStream(metaFile)) {
          firstLogPosition = ReadWriteIOUtils.readLong(metaReader);
          removedLogSize = ReadWriteIOUtils.readLong(metaReader);
          minAvailableVersion = ReadWriteIOUtils.readLong(metaReader);
          maxAvailableVersion = ReadWriteIOUtils.readLong(metaReader);
          meta = LogManagerMeta.deserialize(
              ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(metaReader)));
          state = HardState.deserialize(
              ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(metaReader)));
        } catch (IOException e) {
          logger.error("Error in log serialization: ", e);
        }
      } else {
        meta = new LogManagerMeta();
        state = new HardState();
      }
    }
    logger.info("Recovered log meta: {}, firstLogPos: {}, removedLogSize: {}, availableVersion: [{},"
            + " {}], state: {}",
        meta, firstLogPosition, removedLogSize, minAvailableVersion, maxAvailableVersion, state);
    return meta;
  }

  public void serializeMeta(LogManagerMeta meta) {
    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
    tempMetaFile.getParentFile().mkdirs();
    logger.debug("Serializing log meta into {}", tempMetaFile.getPath());
    try (FileOutputStream tempMetaFileOutputStream = new FileOutputStream(tempMetaFile)) {

      ReadWriteIOUtils.write(firstLogPosition, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(removedLogSize, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(minAvailableVersion, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(maxAvailableVersion, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(meta.serialize(), tempMetaFileOutputStream);
      ReadWriteIOUtils.write(state.serialize(), tempMetaFileOutputStream);

      // close stream
      tempMetaFileOutputStream.close();
      // rename
      metaFile.delete();
      tempMetaFile.renameTo(metaFile);
      // rebuild meta stream

      this.meta = meta;
      logger.debug("Serialized log meta into {}", tempMetaFile.getPath());
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    }

  }

  public void close() {
    try {
      if (currentLogOutputStream != null) {
        currentLogOutputStream.close();
        currentLogOutputStream = null;
      }
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    }
  }

  /**
   * adjust maxRemovedLogSize to the first log file
   */
  private void adjustNextThreshold() {
    if (!logFileList.isEmpty()) {
      maxRemovedLogSize = logFileList.get(0).length();
    }
  }

  /**
   * actually delete the data file which only contains removed data
   */
  private void actuallyDeleteFile() {
    Iterator<File> logFileIterator = logFileList.iterator();
    while (logFileIterator.hasNext()) {
      File logFile = logFileIterator.next();
      if (logger.isDebugEnabled()) {
        logger.debug("Examining file for removal, file: {}, len: {}, removedLogSize: {}", logFile
            , logFile.length(), removedLogSize);
      }
      if (logFile.length() > removedLogSize) {
        break;
      }

      logger.info("Removing a log file {}, len: {}, removedLogSize: {}", logFile,
          logFile.length(), removedLogSize);
      removedLogSize -= logFile.length();
      // if system down before delete, we can use this to delete file during recovery
      minAvailableVersion = getFileVersion(logFile);
      serializeMeta(meta);

      logFile.delete();
      logFileIterator.remove();
    }
    adjustNextThreshold();
  }

  /**
   * open a new log file for log data 1. if we can totally remove some old file, remove them 2.
   * create a new log file for new log data
   */
  private void openNewLogFile() {
    // 1. if we can totally remove some old file, remove them
    actuallyDeleteFile();

    // 2. create a new log file for new log data
    try {
      File newLogFile = createNewLogFile(logDir);
      // save meta first
      maxAvailableVersion = getFileVersion(newLogFile) + 1;
      serializeMeta(meta);

      logFileList.add(newLogFile);
      close();
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    }
  }

  /**
   * get file version from file
   *
   * @param file file
   * @return version from file
   */
  private long getFileVersion(File file) {
    return Long.parseLong(file.getName().split("-")[1]);
  }
}
