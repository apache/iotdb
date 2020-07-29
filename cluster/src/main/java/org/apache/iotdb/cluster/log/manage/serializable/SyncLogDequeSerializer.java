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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
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
  private static final String LOG_FILE_PREFIX = ".data";

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
      .getMaxUnsnapshotedLogSize();
  // min version of available log
  private long minAvailableVersion = 0;
  // max version of available log
  private long maxAvailableVersion = Long.MAX_VALUE;
  // log dir
  private String logDir;
  // version controller
  private VersionController versionController;

  private ByteBuffer logBuffer = ByteBuffer
      .allocate(ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize());

  private final int flushRaftLogThreshold = ClusterDescriptor.getInstance().getConfig()
      .getFlushRaftLogThreshold();

  private int bufferedLogNum = 0;


  /**
   * the lock uses when change the logSizeDeque
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();


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
      logger.error("log serializer build version controller failed", e);
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

  /**
   * for log tools
   */
  public LogManagerMeta getMeta() {
    return meta;
  }

  /**
   * Recover all the logs in disk. This function will be called once this instance is created.
   */
  @Override
  public List<Log> getAllEntries() {
    List<Log> logs = recoverLog();
    int size = logs.size();
    if (size != 0 && meta.getLastLogIndex() <= logs.get(size - 1).getCurrLogIndex()) {
      meta.setLastLogTerm(logs.get(size - 1).getCurrLogTerm());
      meta.setLastLogIndex(logs.get(size - 1).getCurrLogIndex());
      meta.setCommitLogTerm(logs.get(size - 1).getCurrLogTerm());
      meta.setCommitLogIndex(logs.get(size - 1).getCurrLogIndex());
    }
    return logs;
  }

  @Override
  public void append(List<Log> entries) throws IOException {
    Log entry = entries.get(entries.size() - 1);
    meta.setCommitLogIndex(entry.getCurrLogIndex());
    meta.setCommitLogTerm(entry.getCurrLogTerm());
    meta.setLastLogIndex(entry.getCurrLogIndex());
    meta.setLastLogTerm(entry.getCurrLogTerm());
    lock.writeLock().lock();
    try {
      putLogs(entries);
      if (bufferedLogNum >= flushRaftLogThreshold) {
        flushLogBuffer();
      }
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "otherwise, please increase the JVM memory", e
      );
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Put each log in entries to local buffer. If the buffer overflows, flush the buffer to the disk,
   * and try to push the log again.
   *
   * @param entries logs to put to buffer
   */
  private void putLogs(List<Log> entries) {
    for (Log log : entries) {
      logBuffer.mark();
      ByteBuffer logData = log.serialize();
      try {
        int size = logData.capacity() + Integer.BYTES;
        logSizeDeque.addLast(size);
        logBuffer.putInt(logData.capacity());
        logBuffer.put(logData);
      } catch (BufferOverflowException e) {
        logger.info("Raft log buffer overflow!");
        logBuffer.reset();
        flushLogBuffer();
        logBuffer.putInt(logData.capacity());
        logBuffer.put(logData);
      }
      bufferedLogNum++;
    }
  }

  /**
   * Flush current log buffer to the disk.
   */
  private void flushLogBuffer() {
    lock.writeLock().lock();
    try {
      if (bufferedLogNum == 0) {
        return;
      }
      // write into disk
      try {
        checkStream();
        ReadWriteIOUtils
            .writeWithoutSize(logBuffer, 0, logBuffer.position(), currentLogOutputStream);
      } catch (IOException e) {
        logger.error("Error in logs serialization: ", e);
        return;
      }
      logBuffer.clear();
      bufferedLogNum = 0;
      logger.debug("End flushing log buffer.");
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void setHardStateAndFlush(HardState state) {
    this.state = state;
    serializeMeta(meta);
  }

  @Override
  public HardState getHardState() {
    return state;
  }

  @Override
  public void removeCompactedEntries(long index) {
    long distance = meta.getCommitLogIndex() - index;
    if (distance <= 0) {
      logger
          .info("compact ({}) is out of bound lastIndex ({})", index, meta.getCommitLogIndex());
      return;
    }
    if (distance > logSizeDeque.size()) {
      logger.info(
          "entries before request index ({}) have been compacted", index);
    }

    int numToRemove = logSizeDeque.size() - (int) distance;
    removeFirst(numToRemove);
  }

  public Deque<Integer> getLogSizeDeque() {
    return logSizeDeque;
  }

  // init output stream
  private void init() {
    recoverMetaFile();
    recoverMeta();
    try {
      recoverLogFiles();

      logFileList.sort(Comparator.comparingLong(o -> Long.parseLong(o.getName().split("-")[1])));

      // add init log file
      if (logFileList.isEmpty()) {
        logFileList.add(createNewLogFile(metaFile.getParentFile().getPath()));
      }

    } catch (IOException e) {
      logger.error("Error in init log file: ", e);
    }
  }

  private void recoverLogFiles() {
    File[] logFiles = metaFile.getParentFile().listFiles();
    if (logger.isInfoEnabled()) {
      logger.info("Find log files {}", logFiles != null ? Arrays.asList(logFiles) :
          Collections.emptyList());
    }

    if (logFiles == null) {
      return;
    }
    for (File file : logFiles) {
      checkLogFile(file);
    }
  }

  private void checkLogFile(File file) {
    if (file.length() == 0 || !file.getName().startsWith(LOG_FILE_PREFIX)) {
      try {
        if (file.exists() && !file.isDirectory() && file.length() == 0) {
          Files.delete(file.toPath());
        }
      } catch (IOException e) {
        logger.warn("Cannot delete empty log file {}", file, e);
      }
      return;
    }

    long fileVersion = getFileVersion(file);
    // this means system down between save meta and data
    if (fileVersion <= minAvailableVersion || fileVersion >= maxAvailableVersion) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete outdated log file {}", file);
      }
    } else {
      logFileList.add(file);
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
      recoverMetaFileFromTemp(tempMetaFile);
    } else if (!metaFile.exists()) {
      createNewMetaFile();
    }
  }

  private void recoverMetaFileFromTemp(File tempMetaFile) {
    // if temp file is empty, just return
    if (tempMetaFile.length() == 0) {
      try {
        Files.delete(tempMetaFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete file {}", tempMetaFile);
      }
    }
    // else use temp file rather than meta file
    else {
      if (metaFile.exists()) {
        try {
          Files.delete(metaFile.toPath());
        } catch (IOException e) {
          logger.warn("Cannot delete file {}", metaFile);
        }
      }
      if (!tempMetaFile.renameTo(metaFile)) {
        logger.warn("Failed to rename log meta file");
      }
    }
  }

  private void createNewMetaFile() {
    try {
      if (!metaFile.createNewFile()) {
        logger.warn("Cannot create log meta file");
      }
    } catch (IOException e) {
      logger.error("Cannot create new log meta file ", e);
    }
  }

  private void checkStream() throws FileNotFoundException {
    if (currentLogOutputStream == null) {
      currentLogOutputStream = new FileOutputStream(getCurrentLogFile(), true);
    }
  }

  private File createNewLogFile(String dirName) throws IOException {
    File logFile = SystemFileFactory.INSTANCE
        .getFile(
            dirName + File.separator + LOG_FILE_PREFIX + "-" + versionController.nextVersion());
    if (!logFile.createNewFile()) {
      logger.warn("Cannot create new log file {}", logFile);
    }
    return logFile;
  }

  @SuppressWarnings("unused") // to support serialization of uncommitted logs
  public void truncateLog(int count, LogManagerMeta meta) {
    truncateLogIntern(count);
    serializeMeta(meta);
  }

  private File getCurrentLogFile() {
    return logFileList.get(logFileList.size() - 1);
  }

  @SuppressWarnings("resource")
  private void truncateLogIntern(int count) {
    if (logSizeDeque.size() < count) {
      throw new IllegalArgumentException("truncate log count is bigger than total log count");
    }

    int size = 0;
    lock.writeLock().lock();
    try {
      for (int i = 0; i < count; i++) {
        size += logSizeDeque.removeLast();
      }
    } finally {
      lock.writeLock().unlock();
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

          Files.delete(currentLogFile.toPath());
          logFileList.remove(logFileList.size() - 1);
          logFileList.add(createNewLogFile(logDir));
        } catch (IOException e) {
          logger.error("Error when truncating {} logs: ", count, e);
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
          logger.error("Error truncating {} logs serialization: ", count, e);
        }
      }
    }

  }

  public void removeFirst(int num) {
    if (bufferedLogNum > 0) {
      flushLogBuffer();
    }
    firstLogPosition += num;
    for (int i = 0; i < num; i++) {
      removedLogSize += logSizeDeque.removeFirst();
    }

    // firstLogPosition changed
    serializeMeta(meta);
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

      try (FileInputStream logReader = new FileInputStream(logFile);
          FileChannel logChannel = logReader.getChannel()) {

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
          logNumInFile++;
        }
      } catch (IOException e) {
        logger.error("Error in recovering logs from {}: ", logFile, e);
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
      logger.error("Unknown log detected ", e);
    }

    lock.writeLock().lock();
    try {
      logSizeDeque.addLast(totalSize);
    } finally {
      lock.writeLock().unlock();
    }

    return log;
  }

  public void recoverMeta() {
    if (meta == null) {
      if (metaFile.exists() && metaFile.length() > 0) {
        if (logger.isInfoEnabled()) {
          SimpleDateFormat format = new SimpleDateFormat();
          logger.info("MetaFile {} exists, last modified: {}", metaFile.getPath(),
              format.format(new Date(metaFile.lastModified())));
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
          logger.error("Cannot recover log meta: ", e);
          meta = new LogManagerMeta();
          state = new HardState();
        }
      } else {
        meta = new LogManagerMeta();
        state = new HardState();
      }
    }
    logger
        .info("Recovered log meta: {}, firstLogPos: {}, removedLogSize: {}, availableVersion: [{},"
                + " {}], state: {}",
            meta, firstLogPosition, removedLogSize, minAvailableVersion, maxAvailableVersion,
            state);
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

    } catch (IOException e) {
      logger.error("Error in serializing log meta: ", e);
    }
    // rename
    try {
      if (metaFile.exists()) {
        Files.delete(metaFile.toPath());
      }
    } catch (IOException e) {
      logger.warn("Cannot delete old log meta file {}", metaFile, e);
    }
    if (!tempMetaFile.renameTo(metaFile)) {
      logger.warn("Cannot rename new log meta file {}", tempMetaFile);
    }

    // rebuild meta stream
    this.meta = meta;
    logger.debug("Serialized log meta into {}", tempMetaFile.getPath());
  }

  @Override
  public void close() {
    flushLogBuffer();
    lock.writeLock().lock();
    try {
      if (currentLogOutputStream != null) {
        currentLogOutputStream.close();
        currentLogOutputStream = null;
      }
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    } finally {
      lock.writeLock().unlock();
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

      try {
        Files.delete(logFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete log file {}", logFile, e);
      }
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
