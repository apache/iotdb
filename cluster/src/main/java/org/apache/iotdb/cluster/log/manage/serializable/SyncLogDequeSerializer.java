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

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncLogDequeSerializer implements StableEntryManager {

  private static final Logger logger = LoggerFactory.getLogger(SyncLogDequeSerializer.class);
  private static final String LOG_DATA_FILE_SUFFIX = "data";
  private static final String LOG_INDEX_FILE_SUFFIX = "idx";

  /**
   * the log data files
   */
  List<File> logDataFileList;

  /**
   * the log index files
   */
  List<File> logIndexFileList;

  private LogParser parser = LogParser.getINSTANCE();
  private File metaFile;
  private FileOutputStream currentLogDataOutputStream;
  private FileOutputStream currentLogIndexOutputStream;
  private Deque<Integer> logSizeDeque = new ArrayDeque<>();
  private LogManagerMeta meta;
  private HardState state;
  /**
   * mark first log position
   */
  private long firstLogPosition = 0;

  /**
   * removed log size
   */
  private long removedLogSize = 0;

  /**
   * when the removedLogSize larger than this, we actually delete logs
   */
  private long maxRemovedLogSize = ClusterDescriptor.getInstance().getConfig()
      .getMaxUnsnapshotLogSize();

  /**
   * min version of available log
   */
  private long minAvailableVersion = 0;

  /**
   * max version of available log
   */
  private long maxAvailableVersion = Long.MAX_VALUE;

  private String logDir;

  private VersionController versionController;

  private ByteBuffer logDataBuffer = ByteBuffer
      .allocate(ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize());
  private ByteBuffer logIndexBuffer = ByteBuffer
      .allocate(ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize());

  private final int flushRaftLogThreshold = ClusterDescriptor.getInstance().getConfig()
      .getFlushRaftLogThreshold();

  private int bufferedLogNum = 0;

  private long offsetOfTheCurrentLogDataOutputStream = 0;

  /**
   * file name pattern:
   * <p>
   * for log data file: ${startTime}-${Long.MAX_VALUE}-{version}-data
   * <p>
   * for log index file: ${startTime}-${Long.MAX_VALUE}-{version}-idx
   */
  private static final int FILE_NAME_PART_LENGTH = 4;

  private final int maxRaftLogIndexSizeInMemory = ClusterDescriptor.getInstance().getConfig()
      .getMaxRaftLogIndexSizeInMemory();

  private final int maxRaftLogPersistDataSizePerFile = ClusterDescriptor.getInstance().getConfig()
      .getMaxRaftLogPersistDataSizePerFile();

  private final int maxNumberOfPersistRaftLogFiles = ClusterDescriptor.getInstance().getConfig()
      .getMaxNumberOfPersistRaftLogFiles();

  private final int maxPersistRaftLogNumberOnDisk = ClusterDescriptor.getInstance().getConfig()
      .getMaxPersistRaftLogNumberOnDisk();

  private ScheduledExecutorService persistLogDeleteExecutorService;
  private ScheduledFuture<?> persistLogDeleteLogFuture;

  /**
   * indicate the first raft log's index of {@link SyncLogDequeSerializer#logIndexOffsetList}, for
   * example, if firstLogIndex=1000, then the offset of the log index 1000 equals
   * logIndexOffsetList[0], the offset of the log index 1001 equals logIndexOffsetList[1], and so
   * on.
   */
  private long firstLogIndex = 0;

  /**
   * the offset of the log's index, for example, the first value is the offset of index
   * ${firstLogIndex}, the second value is the offset of index ${firstLogIndex+1}
   */
  private List<Long> logIndexOffsetList;

  private static final int logDeleteCheckIntervalSecond = 1;

  /**
   * the lock uses when change the logSizeDeque
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();


  private void initCommonProperties() {
    this.firstLogIndex = meta.getCommitLogIndex() + 1;
    this.logDataFileList = new ArrayList<>();
    this.logIndexFileList = new ArrayList<>();
    this.logIndexOffsetList = new ArrayList<>(maxRaftLogIndexSizeInMemory);
    try {
      versionController = new SimpleFileVersionController(logDir);
    } catch (IOException e) {
      logger.error("log serializer build version controller failed", e);
    }
    this.persistLogDeleteExecutorService = new ScheduledThreadPoolExecutor(1,
        new BasicThreadFactory.Builder().namingPattern("persist-log-delete-%d").daemon(true)
            .build());

    this.persistLogDeleteLogFuture = persistLogDeleteExecutorService
        .scheduleAtFixedRate(this::checkDeletePersistRaftLog, logDeleteCheckIntervalSecond,
            logDeleteCheckIntervalSecond,
            TimeUnit.SECONDS);

  }

  /**
   * for log tools
   *
   * @param logPath log dir path
   */
  public SyncLogDequeSerializer(String logPath) {
    logDir = logPath + File.separator;
    initCommonProperties();
    initMetaAndLogFiles();
  }

  /**
   * log in disk is [size of log1 | log1 buffer] [size of log2 | log2 buffer]
   * <p>
   * build serializer with node id
   */
  public SyncLogDequeSerializer(int nodeIdentifier) {
    logDir = getLogDir(nodeIdentifier);
    initCommonProperties();
    initMetaAndLogFiles();
  }

  public static String getLogDir(int nodeIdentifier) {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    return systemDir + File.separator + "raftLog" + File.separator +
        nodeIdentifier + File.separator;
  }

  @TestOnly
  String getLogDir() {
    return logDir;
  }

  @TestOnly
  File getMetaFile() {
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
  public List<Log> getAllEntriesBeforeAppliedIndex() {
    if (meta.getMaxHaveAppliedCommitIndex() >= meta.getCommitLogIndex()) {
      Collections.emptyList();
    }
    return getLogs(meta.getMaxHaveAppliedCommitIndex(), meta.getCommitLogIndex());
  }

  @Override
  public void append(List<Log> entries, long maxHaveAppliedCommitIndex) throws IOException {
    lock.writeLock().lock();
    try {
      putLogs(entries);
      Log entry = entries.get(entries.size() - 1);
      meta.setCommitLogIndex(entry.getCurrLogIndex());
      meta.setCommitLogTerm(entry.getCurrLogTerm());
      meta.setLastLogIndex(entry.getCurrLogIndex());
      meta.setLastLogTerm(entry.getCurrLogTerm());
      meta.setMaxHaveAppliedCommitIndex(maxHaveAppliedCommitIndex);
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "otherwise, please increase the JVM memory", e
      );
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void closeFile() {

  }

  /**
   * Put each log in entries to local buffer. If the buffer overflows, flush the buffer to the disk,
   * and try to push the log again.
   *
   * @param entries logs to put to buffer
   */
  private void putLogs(List<Log> entries) {
    for (Log log : entries) {
      logDataBuffer.mark();
      logIndexBuffer.mark();
      ByteBuffer logData = log.serialize();
      int size = logData.capacity() + Integer.BYTES;
      offsetOfTheCurrentLogDataOutputStream += size;
      try {
        logDataBuffer.putInt(logData.capacity());
        logDataBuffer.put(logData);
        logIndexBuffer.putLong(offsetOfTheCurrentLogDataOutputStream);

        //TODO 是否还需要
        logSizeDeque.addLast(size);
        bufferedLogNum++;
        // TODO, 初始化 firstLogIndex
        // TODO, 检查logIndexOffsetList size 如果大于maxRaftLogIndexSizeInMemory这块代码逻辑放到log delete逻辑中

        logIndexOffsetList.add(offsetOfTheCurrentLogDataOutputStream);
      } catch (BufferOverflowException e) {
        logger.info("Raft log buffer overflow!");
        logDataBuffer.reset();
        logIndexBuffer.reset();
        flushLogBuffer();
        checkCloseCurrentFile(log.getCurrLogIndex() - 1);
        logDataBuffer.putInt(logData.capacity());
        logDataBuffer.put(logData);
        logIndexBuffer.putLong(offsetOfTheCurrentLogDataOutputStream);
        logSizeDeque.addLast(size);
        bufferedLogNum++;
        // TODO, 初始化 firstLogIndex
        // TODO, 检查logIndexOffsetList size 如果大于maxRaftLogIndexSizeInMemory这块代码逻辑放到log delete逻辑中
        logIndexOffsetList.add(offsetOfTheCurrentLogDataOutputStream);
      }
    }
  }

  private void checkCloseCurrentFile(long commitIndex) {
    if (offsetOfTheCurrentLogDataOutputStream > maxRaftLogPersistDataSizePerFile) {
      try {
        closeCurrentFileAndCreateNewFile(commitIndex);
      } catch (IOException e) {
        logger.error("check close current file failed", e);
      }
      offsetOfTheCurrentLogDataOutputStream = 0;
    }
  }

  private void closeCurrentFileAndCreateNewFile(long commitIndex) throws IOException {
    lock.writeLock().lock();
    try {
      currentLogDataOutputStream.close();
      currentLogIndexOutputStream.close();
      currentLogDataOutputStream = null;
      currentLogIndexOutputStream = null;

      File currentLogDataFile = getCurrentLogDataFile();
      String newDataFileName = currentLogDataFile.getName()
          .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(commitIndex));
      File newCurrentLogDatFile = SystemFileFactory.INSTANCE
          .getFile(currentLogDataFile.getParent() + File.separator + newDataFileName);
      if (!currentLogDataFile.renameTo(newCurrentLogDatFile)) {
        logger.error("rename log data file={} failed", currentLogDataFile.getAbsoluteFile());
      }

      File currentLogIndexFile = getCurrentLogIndexFile();
      String newIndexFileName = currentLogIndexFile.getName()
          .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(commitIndex));
      File newCurrentLogIndexFile = SystemFileFactory.INSTANCE
          .getFile(currentLogIndexFile.getParent() + File.separator + newIndexFileName);
      if (!currentLogIndexFile.renameTo(newCurrentLogIndexFile)) {
        logger.error("rename log index file={} failed", currentLogIndexFile.getAbsoluteFile());
      }

      createNewLogFile(logDir);
    } finally {
      lock.writeLock().unlock();
    }
  }


  @Override
  public void flushLogBuffer() {
    lock.writeLock().lock();
    try {
      if (bufferedLogNum == 0) {
        return;
      }
      // write into disk
      try {
        checkStream();
        // 1. write to the log data file
        ReadWriteIOUtils
            .writeWithoutSize(logDataBuffer, 0, logDataBuffer.position(),
                currentLogDataOutputStream);
        ReadWriteIOUtils
            .writeWithoutSize(logIndexBuffer, 0, logIndexBuffer.position(),
                currentLogIndexOutputStream);
        if (ClusterDescriptor.getInstance().getConfig().getForceRaftLogPeriodInMS() == 0) {
          currentLogDataOutputStream.getChannel().force(true);
          currentLogIndexOutputStream.getChannel().force(true);
        }
      } catch (IOException e) {
        logger.error("Error in logs serialization: ", e);
        return;
      }
      logDataBuffer.clear();
      logIndexBuffer.clear();
      bufferedLogNum = 0;
      logger.debug("End flushing log buffer.");
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void forceFlushLogBuffer() {
    flushLogBuffer();
    checkCloseCurrentFile(meta.getCommitLogIndex());
    lock.writeLock().lock();
    try {
      if (currentLogDataOutputStream != null) {
        currentLogDataOutputStream.getChannel().force(true);
      }
    } catch (IOException e) {
      logger.error("Error when force flushing logs serialization: ", e);
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
    // do nothing
  }

  public Deque<Integer> getLogSizeDeque() {
    return logSizeDeque;
  }

  private void initMetaAndLogFiles() {
    recoverMetaFile();
    recoverMeta();
    try {
      recoverLogFiles();

      logDataFileList.sort(
          this::comparePersistLogFileName);

      logIndexFileList.sort(
          this::comparePersistLogFileName);

      // add init log file
      if (logDataFileList.isEmpty()) {
        createNewLogFile(metaFile.getParentFile().getPath());
      }

    } catch (IOException e) {
      logger.error("Error in init log file: ", e);
    }
  }

  /**
   * The file name rules are as follows: ${startLogIndex}-${endLogIndex}-${version}.data
   */
  private void recoverLogFiles() {
    // 1. recover the log data file
    recoverLogFiles(LOG_DATA_FILE_SUFFIX);
    // 2. recover the log index file
    recoverLogFiles(LOG_INDEX_FILE_SUFFIX);
  }

  private void recoverLogFiles(String logFileType) {
    FileFilter logFilter = pathname -> {
      String s = pathname.getName();
      return s.endsWith(logFileType);
    };

    List<File> logFiles = Arrays.asList(metaFile.getParentFile().listFiles(logFilter));
    if (logger.isInfoEnabled()) {
      logger.info("Find log type ={} log files {}", logFileType, logFiles);
    }

    for (File file : logFiles) {
      if (checkLogFile(file, logFileType)) {
        switch (logFileType) {
          case LOG_DATA_FILE_SUFFIX:
            logDataFileList.add(file);
            break;
          case LOG_INDEX_FILE_SUFFIX:
            logIndexFileList.add(file);
            break;
          default:
            logger.error("unknown file type={}", logFileType);
        }
      }
    }
  }

  /**
   * Check that the file is legal or not
   *
   * @param file     file needs to be check
   * @param fileType {@link SyncLogDequeSerializer#LOG_DATA_FILE_SUFFIX} or  {@link
   *                 SyncLogDequeSerializer#LOG_INDEX_FILE_SUFFIX}
   * @return true if the file legal otherwise false
   */
  private boolean checkLogFile(File file, String fileType) {
    if (file.length() == 0 || !file.getName().endsWith(fileType)) {
      try {
        if (file.exists() && !file.isDirectory() && file.length() == 0) {
          Files.delete(file.toPath());
        }
      } catch (IOException e) {
        logger.warn("Cannot delete empty log file {}", file, e);
      }
      return false;
    }

    long fileVersion = getFileVersion(file);
    // this means system down between save meta and data
    if (fileVersion <= minAvailableVersion || fileVersion >= maxAvailableVersion) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete outdated log file {}", file);
      }
      return false;
    }
    return true;
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
    if (currentLogDataOutputStream == null) {
      currentLogDataOutputStream = new FileOutputStream(getCurrentLogDataFile(), true);
    }

    if (currentLogIndexOutputStream == null) {
      currentLogIndexOutputStream = new FileOutputStream(getCurrentLogIndexFile(), true);
    }
  }

  /**
   * for unclosed file, the file name is ${startTime}-${Long.MAX_VALUE}-{version}
   */
  private void createNewLogFile(String dirName) throws IOException {
    lock.writeLock().lock();
    try {
      long nextVersion = versionController.nextVersion();
      long startLogIndex = meta.getCommitLogIndex() + 1;
      long endLogIndex = Long.MAX_VALUE;

      String fileNamePrefix =
          dirName + File.separator + startLogIndex + FILE_NAME_SEPARATOR + endLogIndex
              + FILE_NAME_SEPARATOR + nextVersion + FILE_NAME_SEPARATOR;
      File logDataFile = SystemFileFactory.INSTANCE
          .getFile(fileNamePrefix + LOG_DATA_FILE_SUFFIX);
      File logIndexFile = SystemFileFactory.INSTANCE
          .getFile(fileNamePrefix + LOG_INDEX_FILE_SUFFIX);

      if (!logDataFile.createNewFile()) {
        logger.warn("Cannot create new log data file {}", logDataFile);
      }

      if (!logIndexFile.createNewFile()) {
        logger.warn("Cannot create new log index file {}", logDataFile);
      }
      logDataFileList.add(logDataFile);
      logIndexFileList.add(logIndexFile);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private File getCurrentLogDataFile() {
    return logDataFileList.get(logDataFileList.size() - 1);
  }

  private File getCurrentLogIndexFile() {
    return logIndexFileList.get(logIndexFileList.size() - 1);
  }
//
//  public List<Log> recoverLog() {
//    List<Log> result = new ArrayList<>();
//    // skip removal file
//    boolean shouldSkip = true;
//
//    for (File logFile : logDataFileList) {
//      int logNumInFile = 0;
//      if (logFile.length() == 0) {
//        continue;
//      }
//
//      try (FileInputStream logReader = new FileInputStream(logFile);
//          FileChannel logChannel = logReader.getChannel()) {
//
//        if (shouldSkip) {
//          long actuallySkippedBytes = logReader.skip(removedLogSize);
//          if (actuallySkippedBytes != removedLogSize) {
//            logger.info(
//                "Error in log serialization, skipped file length isn't consistent with removedLogSize!");
//            return result;
//          }
//          shouldSkip = false;
//        }
//
//        while (logChannel.position() < logFile.length()) {
//          // actual log
//          Log log = readLog(logReader);
//          result.add(log);
//          logNumInFile++;
//        }
//      } catch (IOException e) {
//        logger.error("Error in recovering logs from {}: ", logFile, e);
//      }
//      logger.info("Recovered {} logs from {}", logNumInFile, logFile);
//    }
//    logger.info("Recovered {} logs totally", result.size());
//    return result;
//  }

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

  private void recoverMeta() {
    if (meta != null) {
      return;
    }

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
    logger
        .info("Recovered log meta: {}, firstLogPos: {}, removedLogSize: {}, availableVersion: [{},"
                + " {}], state: {}",
            meta, firstLogPosition, removedLogSize, minAvailableVersion, maxAvailableVersion,
            state);
  }

  private void serializeMeta(LogManagerMeta meta) {
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
    forceFlushLogBuffer();
    lock.writeLock().lock();
    try {
      if (currentLogDataOutputStream != null) {
        currentLogDataOutputStream.close();
        currentLogDataOutputStream = null;
      }

      if (persistLogDeleteExecutorService != null) {
        persistLogDeleteExecutorService.shutdownNow();
        persistLogDeleteLogFuture.cancel(true);
        try {
          persistLogDeleteExecutorService.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Close persist log delete thread interrupted");
        }
        persistLogDeleteExecutorService = null;
      }
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * get file version from file The file name structure is as follows：
   * {startLogIndex}-{endLogIndex}-{version}-data)
   *
   * @param file file
   * @return version from file
   */
  private long getFileVersion(File file) {
    return Long.parseLong(file.getName().split(FILE_NAME_SEPARATOR)[2]);
  }

  private void checkDeletePersistRaftLog() {
    if (logIndexOffsetList.size() > maxRaftLogIndexSizeInMemory) {
      int compactIndex = logIndexOffsetList.size() - maxRaftLogIndexSizeInMemory;
      logIndexOffsetList.subList(0, compactIndex).clear();
      firstLogIndex = logIndexOffsetList.get(0);
    }

    // 1. check the persist log file number
    while (logDataFileList.size() > maxNumberOfPersistRaftLogFiles) {
      File firstFile = logDataFileList.get(0);
      try {
        lock.writeLock().lock();
        Files.delete(firstFile.toPath());
        Files.delete(logIndexFileList.get(0).toPath());
      } catch (IOException e) {
        logger.error("delete file={} failed", firstFile.getAbsoluteFile());
      } finally {
        lock.writeLock().unlock();
      }
    }

    // 2. check the persist log index number
    while (!logDataFileList.isEmpty()) {
      File firstFile = logDataFileList.get(0);
      String[] splits = firstFile.getName().split(FILE_NAME_SEPARATOR);
      if (meta.getCommitLogIndex() - Long.parseLong(splits[1]) > maxPersistRaftLogNumberOnDisk) {
        try {
          Files.delete(firstFile.toPath());
          Files.delete(logIndexFileList.get(0).toPath());
        } catch (IOException e) {
          logger.error("delete file={} failed", firstFile.getAbsoluteFile());
        }
      } else {
        return;
      }
    }
  }

  /**
   * The file name structure is as follows： {startLogIndex}-{endLogIndex}-{version}-data)
   *
   * @param file1 File to compare
   * @param file2 File to compare
   */
  private int comparePersistLogFileName(File file1, File file2) {
    String[] items1 = file1.getName().split(FILE_NAME_SEPARATOR);
    String[] items2 = file2.getName().split(FILE_NAME_SEPARATOR);
    if (items1.length != FILE_NAME_PART_LENGTH || items2.length != FILE_NAME_PART_LENGTH) {
      logger.error(
          "file1={}, file2={} name should be in the following format: startLogIndex-endLogIndex-version-data",
          file1.getAbsoluteFile(), file2.getAbsoluteFile());
    }
    long startLogIndex1 = Long.parseLong(items1[0]);
    long startLogIndex2 = Long.parseLong(items2[0]);
    int res = Long.compare(startLogIndex1, startLogIndex2);
    if (res == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    }
    return res;
  }

  /**
   * @param startIndex the log start index
   * @param endIndex   the log end index
   * @return the raft log which index between [startIndex, endIndex] or empty if not found
   */
  public List<Log> getLogs(long startIndex, long endIndex) {
    List<Log> result = new ArrayList<>();

    Map<File, Pair<Long, Long>> logDataFileAndOffsetMap = getLogDataFileAndOffset(startIndex,
        endIndex);
    if (logDataFileAndOffsetMap == null) {
      return result;
    }

    for (Entry<File, Pair<Long, Long>> entry : logDataFileAndOffsetMap.entrySet()) {
      result.addAll(getLogsFromOneLogDataFile(entry.getKey(), entry.getValue()));
    }

    return result;
  }


  /**
   * @param logIndex the log's index
   * @return The offset of the data file corresponding to the log index, -1 if not found
   */
  private long getOffsetAccordingToLogIndex(long logIndex) {
    long offset = -1;
    // 1. first find in memory
    if (logIndex >= firstLogIndex) {
      int arrayIndex = (int) (logIndex - firstLogIndex);
      if (arrayIndex < logIndexOffsetList.size()) {
        offset = logIndexOffsetList.get(arrayIndex);
        return offset;
      }
    }

    // 2. second read the log index file
    Pair<File, Pair<Long, Long>> fileWithStartAndEndIndex = getLogIndexFile(logIndex);
    if (fileWithStartAndEndIndex == null) {
      return -1;
    }
    File file = fileWithStartAndEndIndex.left;
    Pair<Long, Long> startAndEndIndex = fileWithStartAndEndIndex.right;
    try (FileInputStream inputStream = new FileInputStream(file)) {
      long bytesNeedToSkip = (logIndex - startAndEndIndex.left) * (Long.BYTES);
      long bytesActuallySkip = inputStream.skip(bytesNeedToSkip);
      if (bytesNeedToSkip != bytesActuallySkip) {
        logger.error("read file={} failed, should skip={}, actually skip={}",
            file.getAbsoluteFile(), bytesNeedToSkip, bytesActuallySkip);
      }
      offset = ReadWriteIOUtils.readLong(inputStream);
      return offset;
    } catch (IOException e) {
      logger.error("can not read the log index file={}", file.getAbsoluteFile(), e);
    }
    return offset;
  }

  /**
   * @param startIndex the log start index
   * @param endIndex   the log end index
   * @return key-> the log data file, value-> the left value is the start offset of the file, the
   * right is the end offset of the file
   */
  private Map<File, Pair<Long, Long>> getLogDataFileAndOffset(long startIndex,
      long endIndex) {
    Map<File, Pair<Long, Long>> fileNameWithStartAndEndOffset = new HashMap<>();
    // 1. get the start offset with the startIndex
    long startOffset = getOffsetAccordingToLogIndex(startIndex);
    if (startOffset == -1) {
      return null;
    }
    Pair<File, Pair<Long, Long>> logDataFileWithStartAndEndLogIndex = getLogDataFile(startIndex);
    if (logDataFileWithStartAndEndLogIndex == null) {
      return null;
    }
    long fileEndLogIndex = logDataFileWithStartAndEndLogIndex.right.right;
    // 2. judge whether the fileEndLogIndex>=endIndex
    while (endIndex > fileEndLogIndex) {
      //  this means the endIndex's offset can not be found in the file
      //  logDataFileWithStartAndEndLogIndex.left; and should be find in the next log data file.
      //3. get the file's end offset
      long endOffset = getOffsetAccordingToLogIndex(fileEndLogIndex);
      fileNameWithStartAndEndOffset
          .put(logDataFileWithStartAndEndLogIndex.left, new Pair<>(startOffset, endOffset));

      //4. search the next file to get the log index of fileEndLogIndex + 1
      startOffset = getOffsetAccordingToLogIndex(fileEndLogIndex + 1);
      if (startOffset == -1) {
        return null;
      }
      logDataFileWithStartAndEndLogIndex = getLogDataFile(fileEndLogIndex + 1);
      if (logDataFileWithStartAndEndLogIndex == null) {
        return null;
      }
      fileEndLogIndex = logDataFileWithStartAndEndLogIndex.right.right;
    }
    // this means the endIndex's offset can not be found in the file logDataFileWithStartAndEndLogIndex.left
    long endOffset = getOffsetAccordingToLogIndex(endIndex);
    fileNameWithStartAndEndOffset
        .put(logDataFileWithStartAndEndLogIndex.left, new Pair<>(startOffset, endOffset));

    return fileNameWithStartAndEndOffset;
  }

  /**
   * @param startIndex the start log index
   * @return the first value of the pair is the log index file which contains the start index; the
   * second pair's first value is the file's start log index. the second pair's second value is the
   * file's end log index. null if not found
   */
  private Pair<File, Pair<Long, Long>> getLogIndexFile(long startIndex) {
    try {
      lock.readLock().lock();
      for (File file : logIndexFileList) {
        String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
        if (splits.length != FILE_NAME_PART_LENGTH) {
          logger.error(
              "file={} name should be in the following format: startLogIndex-endLogIndex-version-idx",
              file.getAbsoluteFile());
        }
        if (Long.parseLong(splits[0]) <= startIndex && startIndex <= Long.parseLong(splits[1])) {
          return new Pair<>(file,
              new Pair<>(Long.parseLong(splits[0]), Long.parseLong(splits[1])));
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    logger.error("can not found the log index file for startIndex={}", startIndex);
    return null;
  }

  /**
   * @param startIndex the start log index
   * @return the first value of the pair is the log data file which contains the start index; the
   * second pair's first value is the file's start log index. the second pair's second value is the
   * file's end log index. null if not found
   */
  private Pair<File, Pair<Long, Long>> getLogDataFile(long startIndex) {
    try {
      lock.readLock().lock();
      for (File file : logDataFileList) {
        String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
        if (splits.length != FILE_NAME_PART_LENGTH) {
          logger.error(
              "file={} name should be in the following format: startLogIndex-endLogIndex-version-data",
              file.getAbsoluteFile());
        }
        if (Long.parseLong(splits[0]) <= startIndex && startIndex <= Long.parseLong(splits[1])) {
          return new Pair<>(file,
              new Pair<>(Long.parseLong(splits[0]), Long.parseLong(splits[1])));
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    logger.error("can not found the log data file for startIndex={}", startIndex);
    return null;
  }

  /**
   * @param file              the log data file
   * @param startAndEndOffset the left value is the start offset of the file,  the right is the end
   *                          offset of the file
   * @return the logs between start offset and end offset
   */

  private List<Log> getLogsFromOneLogDataFile(File file, Pair<Long, Long> startAndEndOffset) {
    List<Log> result = new ArrayList<>();
    try (FileInputStream inputStream = new FileInputStream(file)) {
      long bytesSkip = inputStream.skip(startAndEndOffset.left);
      if (bytesSkip != startAndEndOffset.left) {
        logger.error("read file={} failed when skip {} bytes, actual skip bytes={}",
            file.getAbsoluteFile(), startAndEndOffset.left, bytesSkip);
        return result;
      }
      long currentReadLength = bytesSkip;
      // because we want to get all the logs whose offset between [startAndEndOffset.left, startAndEndOffset.right]
      // which means, the last offset's value should be still read, in other words,
      // the first log index of the offset starting with startAndEndOffset.right also needs to be read.
      while (currentReadLength <= startAndEndOffset.right) {
        int logSize = ReadWriteIOUtils.readInt(inputStream);
        Log log = null;
        try {
          log = parser.parse(ByteBuffer.wrap(ReadWriteIOUtils.readBytes(inputStream, logSize)));
          result.add(log);
        } catch (UnknownLogTypeException e) {
          logger.error("Unknown log detected ", e);
        }
        currentReadLength = currentReadLength + Integer.BYTES + logSize;
      }
    } catch (IOException e) {
      logger.error("Cannot read log from file={} ", file.getAbsoluteFile(), e);
    }
    return result;
  }
}
