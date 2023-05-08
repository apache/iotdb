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
package org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.consensus.natraft.exception.UnknownLogTypeException;
import org.apache.iotdb.consensus.natraft.protocol.HardState;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.LogParser;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization.SyncLogDequeSerializer.VersionController.SimpleFileVersionController;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

public class SyncLogDequeSerializer implements StableEntryManager {

  private static final Logger logger = LoggerFactory.getLogger(SyncLogDequeSerializer.class);
  private static final String LOG_DATA_FILE_SUFFIX = "data";
  private static final String LOG_INDEX_FILE_SUFFIX = "idx";

  /** the log data files */
  private List<File> logDataFileList;

  /** the log index files */
  private List<IndexFileDescriptor> logIndexFileList;

  private LogParser parser = LogParser.getINSTANCE();
  private File metaFile;
  private FileOutputStream currentLogDataOutputStream;
  private FileOutputStream currentLogIndexOutputStream;
  private LogManagerMeta meta;
  private HardState state;
  private String name;

  /** min version of available log */
  private long minAvailableVersion = 0;

  /** max version of available log */
  private long maxAvailableVersion = Long.MAX_VALUE;

  private String logDir;

  private VersionController versionController;

  private ByteBuffer logDataBuffer;
  private ByteBuffer logIndexBuffer;
  private ByteBuffer flushingLogDataBuffer;
  private ByteBuffer flushingLogIndexBuffer;

  private long offsetOfTheCurrentLogDataOutputStream = 0;

  private int maxNumberOfLogsPerFetchOnDisk;

  private static final String LOG_META = "logMeta";
  private static final String LOG_META_TMP = "logMeta.tmp";

  /**
   * file name pattern:
   *
   * <p>for log data file: ${startLogIndex}-${endLogIndex}-{version}-data
   *
   * <p>for log index file: ${startLogIndex}-${endLogIndex}-{version}-idx
   */
  static final int FILE_NAME_PART_LENGTH = 4;

  private int maxRaftLogIndexSizeInMemory;

  private int maxRaftLogPersistDataSizePerFile;

  private int maxNumberOfPersistRaftLogFiles;

  private int maxPersistRaftLogNumberOnDisk;

  private ScheduledExecutorService persistLogExecutorService;
  private ScheduledFuture<?> persistLogDeleteLogFuture;
  private ExecutorService flushingLogExecutorService;
  private volatile Future<?> flushingLogFuture;

  /**
   * indicate the first raft log's index of {@link SyncLogDequeSerializer#logIndexOffsetList}, for
   * example, if firstLogIndex=1000, then the offset of the log index 1000 equals
   * logIndexOffsetList[0], the offset of the log index 1001 equals logIndexOffsetList[1], and so
   * on.
   */
  private long firstLogIndex = 0;

  private long lastLogIndex = 0;
  private long persistedLogIndex = 0;

  /**
   * the index and file offset of the log, for example, the first pair is the offset of index
   * ${firstLogIndex}, the second pair is the offset of index ${firstLogIndex+1}
   */
  private List<Pair<Long, Long>> logIndexOffsetList;

  private static final int LOG_DELETE_CHECK_INTERVAL_SECOND = 5;

  /** the lock uses when change the log data files or log index files */
  private final Lock lock = new ReentrantLock();

  private volatile boolean isClosed = false;
  private RaftConfig config;
  private ICompressor compressor = ICompressor.getCompressor(CompressionType.SNAPPY);
  private IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.SNAPPY);

  private void initCommonProperties() {
    logDataBuffer = ByteBuffer.allocate(config.getRaftLogBufferSize());
    logIndexBuffer = ByteBuffer.allocate(config.getRaftLogBufferSize());
    flushingLogDataBuffer = ByteBuffer.allocate(config.getRaftLogBufferSize());
    flushingLogIndexBuffer = ByteBuffer.allocate(config.getRaftLogBufferSize());

    maxNumberOfLogsPerFetchOnDisk = config.getMaxNumberOfLogsPerFetchOnDisk();
    maxRaftLogIndexSizeInMemory = config.getMaxRaftLogIndexSizeInMemory();
    maxNumberOfPersistRaftLogFiles = config.getMaxNumberOfPersistRaftLogFiles();
    maxPersistRaftLogNumberOnDisk = config.getMaxPersistRaftLogNumberOnDisk();
    maxRaftLogPersistDataSizePerFile = config.getMaxRaftLogPersistDataSizePerFile();

    compressor = ICompressor.getCompressor(config.getLogPersistCompressor());
    unCompressor = IUnCompressor.getUnCompressor(config.getLogPersistCompressor());

    this.logDataFileList = new ArrayList<>();
    this.logIndexFileList = new ArrayList<>();
    this.logIndexOffsetList = new ArrayList<>(maxRaftLogIndexSizeInMemory);
    try {
      versionController = new SimpleFileVersionController(logDir);
    } catch (IOException e) {
      logger.error("log serializer build version controller failed", e);
    }
    this.persistLogExecutorService =
        new ScheduledThreadPoolExecutor(
            1,
            new BasicThreadFactory.Builder()
                .namingPattern("persist-log-delete-" + logDir)
                .daemon(true)
                .build());
    this.flushingLogExecutorService =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, name + "-flushRaftLog"));

    this.persistLogDeleteLogFuture =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            persistLogExecutorService,
            this::checkPersistRaftLog,
            LOG_DELETE_CHECK_INTERVAL_SECOND,
            LOG_DELETE_CHECK_INTERVAL_SECOND,
            TimeUnit.SECONDS);
  }

  /**
   * log in disk is [size of log1 | log1 buffer] [size of log2 | log2 buffer]
   *
   * <p>build serializer with node id
   */
  public SyncLogDequeSerializer(ConsensusGroupId groupId, RaftConfig config) {
    this.config = config;
    name = groupId.toString();
    logDir = getLogDir(groupId);
    initCommonProperties();
    initMetaAndLogFiles();
  }

  public String getLogDir(ConsensusGroupId groupId) {
    String systemDir = config.getStorageDir();
    return systemDir + File.separator + groupId + File.separator + "raftLog" + File.separator;
  }

  /** for log tools */
  @Override
  public LogManagerMeta getMeta() {
    return meta;
  }

  @Override
  public void updateMeta(long commitIndex, long applyIndex) {
    meta.setCommitLogIndex(commitIndex);
    meta.setLastAppliedIndex(applyIndex);
  }

  /** Recover all the logs in disk. This function will be called once this instance is created. */
  @Override
  public List<Entry> getAllEntriesAfterAppliedIndex() {
    logger.debug(
        "getAllEntriesBeforeAppliedIndex, maxHaveAppliedCommitIndex={}",
        meta.getLastAppliedIndex());
    return getEntries(meta.getLastAppliedIndex(), Long.MAX_VALUE, false);
  }

  /**
   * When raft log files flushed,meta would not be flushed synchronously.So data has flushed to disk
   * is uncommitted for persistent LogManagerMeta(meta's info is stale).We need to recover these
   * already persistent logs.
   *
   * <p>For example,commitIndex is 5 in persistent LogManagerMeta,But the log file has actually been
   * flushed to 7,when we restart cluster,we need to recover 6 and 7.
   *
   * <p>Maybe,we can extract getAllEntriesAfterAppliedIndex and getAllEntriesAfterCommittedIndex
   * into getAllEntriesByIndex,but now there are too many test cases using it.
   */
  @Override
  public List<Entry> getAllEntriesAfterCommittedIndex() {
    long lastIndex = firstLogIndex + logIndexOffsetList.size() - 1;
    logger.debug(
        "getAllEntriesAfterCommittedIndex, firstUnCommitIndex={}, lastIndexBeforeStart={}",
        meta.getCommitLogIndex() + 1,
        lastIndex);
    if (meta.getCommitLogIndex() >= lastIndex) {
      return Collections.emptyList();
    }
    return getEntries(meta.getCommitLogIndex() + 1, lastIndex, false);
  }

  @Override
  public void append(List<Entry> entries, long commitIndex, long maxHaveAppliedCommitIndex)
      throws IOException {
    lock.lock();
    try {
      putLogs(entries);
      Entry entry = entries.get(entries.size() - 1);
      meta.setCommitLogIndex(commitIndex);
      meta.setLastLogIndex(entry.getCurrLogIndex());
      meta.setLastLogTerm(entry.getCurrLogTerm());
      meta.setLastAppliedIndex(maxHaveAppliedCommitIndex);
      logger.debug(
          "maxHaveAppliedCommitIndex={}, commitLogIndex={},lastLogIndex={}",
          maxHaveAppliedCommitIndex,
          meta.getCommitLogIndex(),
          meta.getLastLogIndex());
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "otherwise, please increase the JVM memory",
          e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Put each log in entries to local buffer. If the buffer overflows, flush the buffer to the disk,
   * and try to push the log again.
   *
   * @param entries logs to put to buffer
   */
  private void putLogs(List<Entry> entries) {
    for (Entry log : entries) {
      logDataBuffer.mark();
      logIndexBuffer.mark();
      ByteBuffer logData = log.serialize();
      try {
        logDataBuffer.put(logData);
        lastLogIndex = log.getCurrLogIndex();
      } catch (BufferOverflowException e) {
        logger.debug("Raft log buffer overflow!");
        logDataBuffer.reset();
        logIndexBuffer.reset();
        flushLogBuffer();
        logDataBuffer.put(logData);
        lastLogIndex = log.getCurrLogIndex();
      }
    }
  }

  private void checkCloseCurrentFile(long fileEndIndex) {
    if (offsetOfTheCurrentLogDataOutputStream > maxRaftLogPersistDataSizePerFile) {
      try {
        closeCurrentFile(fileEndIndex);
        serializeMeta(meta);
        createNewLogFile(logDir, fileEndIndex + 1);
      } catch (IOException e) {
        logger.error("check close current file failed", e);
      }
    }
  }

  private void closeCurrentFile(long fileEndIndex) throws IOException {
    if (currentLogDataOutputStream != null) {
      currentLogDataOutputStream.close();
      logger.info("{}: Closed a log data file {}", this, getCurrentLogDataFile());
      currentLogDataOutputStream = null;

      File currentLogDataFile = getCurrentLogDataFile();
      String newDataFileName =
          currentLogDataFile
              .getName()
              .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(fileEndIndex));
      File newCurrentLogDatFile =
          SystemFileFactory.INSTANCE.getFile(
              currentLogDataFile.getParent() + File.separator + newDataFileName);
      if (!currentLogDataFile.renameTo(newCurrentLogDatFile)) {
        logger.error(
            "rename log data file={} to {} failed",
            currentLogDataFile.getAbsoluteFile(),
            newCurrentLogDatFile);
      }
      logDataFileList.set(logDataFileList.size() - 1, newCurrentLogDatFile);

      logger.debug(
          "rename data file={} to file={}",
          currentLogDataFile.getAbsoluteFile(),
          newCurrentLogDatFile.getAbsoluteFile());
    }

    if (currentLogIndexOutputStream != null) {
      currentLogIndexOutputStream.close();
      logger.info("{}: Closed a log index file {}", this, getCurrentLogIndexFileDescriptor());
      currentLogIndexOutputStream = null;

      IndexFileDescriptor currentLogIndexFile = getCurrentLogIndexFileDescriptor();
      String newIndexFileName =
          currentLogIndexFile
              .file
              .getName()
              .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(fileEndIndex));
      File newCurrentLogIndexFile =
          SystemFileFactory.INSTANCE.getFile(
              currentLogIndexFile.file.getParent() + File.separator + newIndexFileName);
      if (!currentLogIndexFile.file.renameTo(newCurrentLogIndexFile)) {
        logger.error("rename log index file={} failed", currentLogIndexFile.file.getAbsoluteFile());
      }
      logger.debug(
          "rename index file={} to file={}",
          currentLogIndexFile.file.getAbsoluteFile(),
          newCurrentLogIndexFile.getAbsoluteFile());

      logIndexFileList.get(logIndexFileList.size() - 1).file = newCurrentLogIndexFile;
      logIndexFileList.get(logIndexFileList.size() - 1).endIndex = fileEndIndex;
    }

    offsetOfTheCurrentLogDataOutputStream = 0;
  }

  @Override
  public void flushLogBuffer() {
    if (isClosed || logDataBuffer.position() == 0) {
      return;
    }
    if (flushingLogFuture != null) {
      try {
        flushingLogFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        logger.error("Unexpected exception when flushing log in {}", name);
        throw new RuntimeException(e);
      }
    } else {
      switchBuffer();
      flushingLogFuture = flushingLogExecutorService.submit(() -> flushLogBufferTask(lastLogIndex));
    }
  }

  private void switchBuffer() {
    ByteBuffer temp = logDataBuffer;
    logDataBuffer = flushingLogIndexBuffer;
    flushingLogDataBuffer = temp;
    temp = logIndexBuffer;
    logIndexBuffer = flushingLogIndexBuffer;
    flushingLogIndexBuffer = temp;
  }

  private void flushLogBufferTask(long currentLastIndex) {
    // write into disk
    try {
      checkStream();
      // 1. write to the log data file
      byte[] compressed =
          compressor.compress(flushingLogDataBuffer.array(), 0, flushingLogDataBuffer.position());
      ReadWriteIOUtils.write(compressed.length, currentLogDataOutputStream);
      logIndexOffsetList.add(new Pair<>(lastLogIndex, offsetOfTheCurrentLogDataOutputStream));
      flushingLogIndexBuffer.putLong(lastLogIndex);
      flushingLogIndexBuffer.putLong(offsetOfTheCurrentLogDataOutputStream);
      offsetOfTheCurrentLogDataOutputStream += Integer.BYTES + compressed.length;

      currentLogDataOutputStream.write(compressed);
      ReadWriteIOUtils.writeWithoutSize(
          logIndexBuffer, 0, logIndexBuffer.position(), currentLogIndexOutputStream);
      if (config.getFlushRaftLogThreshold() == 0) {
        currentLogDataOutputStream.getChannel().force(true);
        currentLogIndexOutputStream.getChannel().force(true);
      }
      persistedLogIndex = currentLastIndex;

      checkCloseCurrentFile();
    } catch (IOException e) {
      logger.error("Error in logs serialization: ", e);
      return;
    }

    flushingLogDataBuffer.clear();
    flushingLogIndexBuffer.clear();

    switchBuffer();
    logger.debug("End flushing log buffer.");
  }

  private void forceFlushLogBufferWithoutCloseFile() {
    if (isClosed) {
      return;
    }
    lock.lock();
    flushLogBuffer();
    serializeMeta(meta);
    try {
      if (currentLogDataOutputStream != null) {
        currentLogDataOutputStream.getChannel().force(true);
      }
      if (currentLogIndexOutputStream != null) {
        currentLogIndexOutputStream.getChannel().force(true);
      }
    } catch (ClosedByInterruptException e) {
      // ignore
    } catch (IOException e) {
      logger.error("Error when force flushing logs serialization: ", e);
    } finally {
      lock.unlock();
    }
  }

  /** flush the log buffer and check if the file needs to be closed */
  @Override
  public void forceFlushLogBuffer() {
    lock.lock();
    try {
      forceFlushLogBufferWithoutCloseFile();
      checkCloseCurrentFile(meta.getCommitLogIndex());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setHardStateAndFlush(HardState state) {
    this.state = state;
    // serializeMeta(meta);
  }

  @Override
  public HardState getHardState() {
    return state;
  }

  @Override
  public void removeCompactedEntries(long index) {
    // do nothing
  }

  private void initMetaAndLogFiles() {
    recoverMetaFile();
    recoverMeta();
    this.firstLogIndex = meta.getCommitLogIndex() + 1;
    try {
      recoverLogFiles();
      // add init log file
      if (logDataFileList.isEmpty()) {
        createNewLogFile(metaFile.getParentFile().getPath(), meta.getCommitLogIndex() + 1);
      }

    } catch (IOException e) {
      logger.error("Error in init log file: ", e);
    }
  }

  /** The file name rules are as follows: ${startLogIndex}-${endLogIndex}-${version}.data */
  private void recoverLogFiles() {
    // 1. first we should recover the log index file
    recoverLogFiles(LOG_INDEX_FILE_SUFFIX);

    // 2. recover the log data file
    recoverLogFiles(LOG_DATA_FILE_SUFFIX);

    // sort by name before recover
    logDataFileList.sort(this::comparePersistLogFileName);
    logIndexFileList.sort(
        (descriptor1, descriptor2) ->
            comparePersistLogFileName(descriptor1.file, descriptor2.file));

    // 3. recover the last log file in case of abnormal exit
    recoverTheLastLogFile();
  }

  private void recoverLogFiles(String logFileType) {
    FileFilter logFilter =
        pathname -> {
          String s = pathname.getName();
          return s.endsWith(logFileType);
        };

    List<File> logFiles = Arrays.asList(metaFile.getParentFile().listFiles(logFilter));
    logger.info("Find log type ={} log files {}", logFileType, logFiles);

    for (File file : logFiles) {
      if (checkLogFile(file, logFileType)) {
        switch (logFileType) {
          case LOG_DATA_FILE_SUFFIX:
            logDataFileList.add(file);
            break;
          case LOG_INDEX_FILE_SUFFIX:
            logIndexFileList.add(new IndexFileDescriptor(file));
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
   * @param file file needs to be check
   * @param fileType {@link SyncLogDequeSerializer#LOG_DATA_FILE_SUFFIX} or {@link
   *     SyncLogDequeSerializer#LOG_INDEX_FILE_SUFFIX}
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

    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    // start index should be smaller than end index
    if (Long.parseLong(splits[0]) > Long.parseLong(splits[1])) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete incorrect log file {}", file);
      }
      return false;
    }
    return true;
  }

  private void recoverTheLastLogFile() {
    if (logIndexFileList.isEmpty()) {
      logger.info("no log index file to recover");
      return;
    }

    IndexFileDescriptor lastIndexFileDescriptor = logIndexFileList.get(logIndexFileList.size() - 1);
    long endIndex = lastIndexFileDescriptor.endIndex;
    boolean success = true;
    if (endIndex != Long.MAX_VALUE) {
      logger.info(
          "last log index file={} no need to recover",
          lastIndexFileDescriptor.file.getAbsoluteFile());
    } else {
      success = recoverTheLastLogIndexFile(lastIndexFileDescriptor);
    }

    if (!success) {
      logger.error(
          "recover log index file failed, clear all logs in disk, {}",
          lastIndexFileDescriptor.file.getAbsoluteFile());
      forceDeleteAllLogFiles();
      clearFirstLogIndex();
      return;
    }

    File lastDataFile = logDataFileList.get(logDataFileList.size() - 1);
    endIndex = Long.parseLong(lastDataFile.getName().split(FILE_NAME_SEPARATOR)[1]);
    if (endIndex != Long.MAX_VALUE) {
      logger.info("last log data file={} no need to recover", lastDataFile.getAbsoluteFile());
      return;
    }

    success = recoverTheLastLogDataFile(logDataFileList.get(logDataFileList.size() - 1));
    if (!success) {
      logger.error(
          "recover log data file failed, clear all logs in disk,{}",
          lastDataFile.getAbsoluteFile());
      forceDeleteAllLogFiles();
      clearFirstLogIndex();
    }
  }

  private boolean recoverTheLastLogDataFile(File file) {
    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    long startIndex = Long.parseLong(splits[0]);
    IndexFileDescriptor descriptor = getLogIndexFile(startIndex);
    if (descriptor.startIndex == startIndex) {
      long endIndex = descriptor.endIndex;
      String newDataFileName =
          file.getName().replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(endIndex));
      File newLogDataFile =
          SystemFileFactory.INSTANCE.getFile(file.getParent() + File.separator + newDataFileName);
      if (!file.renameTo(newLogDataFile)) {
        logger.error("rename log data file={} failed when recover", file.getAbsoluteFile());
      }
      logDataFileList.remove(logDataFileList.size() - 1);
      logDataFileList.add(newLogDataFile);
      return true;
    }
    return false;
  }

  private boolean recoverTheLastLogIndexFile(IndexFileDescriptor descriptor) {
    logger.debug("start to recover the last log index file={}", descriptor.file.getAbsoluteFile());
    long startIndex = descriptor.startIndex;
    int longLength = 8;
    byte[] bytes = new byte[longLength];

    long index;
    long offset = 0;
    long endIndex = 0;
    try (FileInputStream fileInputStream = new FileInputStream(descriptor.file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      firstLogIndex = startIndex;
      while (bufferedInputStream.read(bytes) != -1) {
        index = BytesUtils.bytesToLong(bytes);
        bufferedInputStream.read(bytes);
        offset = BytesUtils.bytesToLong(bytes);
        logIndexOffsetList.add(new Pair<>(index, offset));
        endIndex = index;
      }
    } catch (IOException e) {
      logger.error("recover log index file failed,", e);
    }
    logger.debug(
        "recover log index file={}, startIndex={}, endIndex={}",
        descriptor.file.getAbsoluteFile(),
        startIndex,
        endIndex);

    if (endIndex < meta.getCommitLogIndex()) {
      logger.error(
          "due to the last abnormal exit, part of the raft logs are lost. "
              + "The commit index saved by the meta shall prevail, and all logs will be deleted"
              + "meta commitLogIndex={}, endIndex={}",
          meta.getCommitLogIndex(),
          endIndex);
      return false;
    }
    if (endIndex >= startIndex) {
      String newIndexFileName =
          descriptor
              .file
              .getName()
              .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(endIndex));
      File newLogIndexFile =
          SystemFileFactory.INSTANCE.getFile(
              descriptor.file.getParent() + File.separator + newIndexFileName);
      if (!descriptor.file.renameTo(newLogIndexFile)) {
        logger.error(
            "rename log index file={} failed when recover", descriptor.file.getAbsoluteFile());
      }
      descriptor.file = newLogIndexFile;
      descriptor.endIndex = endIndex;
    } else {
      logger.error("recover log index file failed,{}", descriptor.file.getAbsoluteFile());
      return false;
    }
    return true;
  }

  private void clearFirstLogIndex() {
    firstLogIndex = meta.getCommitLogIndex() + 1;
    logIndexOffsetList.clear();
  }

  private void recoverMetaFile() {
    metaFile = SystemFileFactory.INSTANCE.getFile(logDir + LOG_META);

    // build dir
    if (!metaFile.getParentFile().exists()) {
      metaFile.getParentFile().mkdirs();
    }

    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + LOG_META_TMP);
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
      try {
        Files.deleteIfExists(metaFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete file {}", metaFile);
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
      logger.info("{}: Opened a new log data file: {}", this, getCurrentLogDataFile());
    }

    if (currentLogIndexOutputStream == null) {
      currentLogIndexOutputStream =
          new FileOutputStream(getCurrentLogIndexFileDescriptor().file, true);
      logger.info("{}: Opened a new index data file: {}", this, getCurrentLogIndexFileDescriptor());
    }
  }

  /** for unclosed file, the file name is ${startIndex}-${Long.MAX_VALUE}-{version} */
  private void createNewLogFile(String dirName, long startLogIndex) throws IOException {
    lock.lock();
    try {
      long nextVersion = versionController.nextVersion();
      long endLogIndex = Long.MAX_VALUE;

      String fileNamePrefix =
          dirName
              + File.separator
              + startLogIndex
              + FILE_NAME_SEPARATOR
              + endLogIndex
              + FILE_NAME_SEPARATOR
              + nextVersion
              + FILE_NAME_SEPARATOR;
      File logDataFile = SystemFileFactory.INSTANCE.getFile(fileNamePrefix + LOG_DATA_FILE_SUFFIX);
      File logIndexFile =
          SystemFileFactory.INSTANCE.getFile(fileNamePrefix + LOG_INDEX_FILE_SUFFIX);

      if (!logDataFile.createNewFile()) {
        logger.warn("Cannot create new log data file {}", logDataFile);
      }

      if (!logIndexFile.createNewFile()) {
        logger.warn("Cannot create new log index file {}", logDataFile);
      }
      logDataFileList.add(logDataFile);
      logIndexFileList.add(new IndexFileDescriptor(logIndexFile, startLogIndex, endLogIndex));
    } finally {
      lock.unlock();
    }
  }

  private File getCurrentLogDataFile() {
    return logDataFileList.get(logDataFileList.size() - 1);
  }

  private IndexFileDescriptor getCurrentLogIndexFileDescriptor() {
    return logIndexFileList.get(logIndexFileList.size() - 1);
  }

  private void recoverMeta() {
    if (meta != null) {
      return;
    }

    if (metaFile.exists() && metaFile.length() > 0) {
      if (logger.isInfoEnabled()) {
        SimpleDateFormat format = new SimpleDateFormat();
        logger.info(
            "MetaFile {} exists, last modified: {}",
            metaFile.getPath(),
            format.format(new Date(metaFile.lastModified())));
      }
      try (FileInputStream fileInputStream = new FileInputStream(metaFile);
          BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
        minAvailableVersion = ReadWriteIOUtils.readLong(bufferedInputStream);
        maxAvailableVersion = ReadWriteIOUtils.readLong(bufferedInputStream);
        meta =
            LogManagerMeta.deserialize(
                ByteBuffer.wrap(
                    ReadWriteIOUtils.readBytesWithSelfDescriptionLength(bufferedInputStream)));
        state =
            HardState.deserialize(
                ByteBuffer.wrap(
                    ReadWriteIOUtils.readBytesWithSelfDescriptionLength(bufferedInputStream)));
      } catch (IOException e) {
        logger.error("Cannot recover log meta: ", e);
        meta = new LogManagerMeta();
        state = new HardState();
      }
    } else {
      meta = new LogManagerMeta();
      state = new HardState();
    }

    persistedLogIndex = meta.getLastLogIndex();
    logger.info(
        "Recovered log meta: {}, availableVersion: [{},{}], state: {}",
        meta,
        minAvailableVersion,
        maxAvailableVersion,
        state);
  }

  private void serializeMeta(LogManagerMeta meta) {
    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + LOG_META_TMP);
    tempMetaFile.getParentFile().mkdirs();
    logger.trace("Serializing log meta into {}", tempMetaFile.getPath());
    try (FileOutputStream tempMetaFileOutputStream = new FileOutputStream(tempMetaFile)) {
      ReadWriteIOUtils.write(minAvailableVersion, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(maxAvailableVersion, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(meta.serialize(), tempMetaFileOutputStream);
      ReadWriteIOUtils.write(state.serialize(), tempMetaFileOutputStream);

    } catch (IOException e) {
      logger.error("Error in serializing log meta: ", e);
    }
    // rename
    try {
      Files.deleteIfExists(metaFile.toPath());
    } catch (IOException e) {
      logger.warn("Cannot delete old log meta file {}", metaFile, e);
    }
    if (!tempMetaFile.renameTo(metaFile)) {
      logger.warn("Cannot rename new log meta file {}", tempMetaFile);
    }

    // rebuild meta stream
    this.meta = meta;
    logger.trace("Serialized log meta into {}", tempMetaFile.getPath());
  }

  @Override
  public void close() {
    logger.info("{} is closing", this);
    lock.lock();
    forceFlushLogBuffer();
    try {
      closeCurrentFile(lastLogIndex);
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    } finally {
      logger.info("{} is closed", this);
      isClosed = true;
      lock.unlock();
    }

    if (persistLogExecutorService != null) {
      persistLogExecutorService.shutdownNow();
      persistLogDeleteLogFuture.cancel(true);
      try {
        persistLogExecutorService.awaitTermination(20, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      persistLogExecutorService = null;
    }
  }

  @Override
  public void clearAllLogs(long commitIndex) {
    lock.lock();
    try {
      // 1. delete
      forceFlushLogBuffer();
      closeCurrentFile(meta.getCommitLogIndex());
      forceDeleteAllLogFiles();
      deleteMetaFile();

      logDataFileList.clear();
      logIndexFileList.clear();

      // 2. init
      if (!logIndexOffsetList.isEmpty()) {
        this.firstLogIndex = Math.max(commitIndex + 1, firstLogIndex + logIndexOffsetList.size());
      } else {
        this.firstLogIndex = commitIndex + 1;
      }
      this.logIndexOffsetList.clear();
      recoverMetaFile();
      meta = new LogManagerMeta();
      createNewLogFile(logDir, firstLogIndex);
      logger.info("{}, clean all logs success, the new firstLogIndex={}", this, firstLogIndex);
    } catch (IOException e) {
      logger.error("clear all logs failed,", e);
    } finally {
      lock.unlock();
    }
  }

  private void deleteMetaFile() {
    lock.lock();
    try {
      File tmpMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + LOG_META_TMP);
      Files.deleteIfExists(tmpMetaFile.toPath());
      File localMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + LOG_META);
      Files.deleteIfExists(localMetaFile.toPath());
    } catch (IOException e) {
      logger.error("{}: delete meta log files failed", this, e);
    } finally {
      lock.unlock();
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

  public void checkPersistRaftLog() {

    lock.lock();
    try {
      // 1. flush logs in buffer
      flushLogBuffer();

      // 2. check the log index offset list size
      if (logIndexOffsetList.size() > maxRaftLogIndexSizeInMemory) {
        int compactIndex = logIndexOffsetList.size() - maxRaftLogIndexSizeInMemory;
        logIndexOffsetList.subList(0, compactIndex).clear();
        firstLogIndex += compactIndex;
      }
    } finally {
      lock.unlock();
    }

    // 3. check the persist log file number
    lock.lock();
    try {
      while (logDataFileList.size() > maxNumberOfPersistRaftLogFiles) {
        deleteTheFirstLogDataAndIndexFile();
      }
    } finally {
      lock.unlock();
    }

    // 4. check the persisted log index number
    lock.lock();
    try {
      while (logDataFileList.size() > 1) {
        File firstFile = logDataFileList.get(0);
        String[] splits = firstFile.getName().split(FILE_NAME_SEPARATOR);
        if (meta.getCommitLogIndex() - Long.parseLong(splits[1]) > maxPersistRaftLogNumberOnDisk) {
          deleteTheFirstLogDataAndIndexFile();
        } else {
          return;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void forceDeleteAllLogDataFiles() {
    FileFilter logFilter =
        pathname -> {
          String s = pathname.getName();
          return s.endsWith(LOG_DATA_FILE_SUFFIX);
        };
    List<File> logFiles = Arrays.asList(metaFile.getParentFile().listFiles(logFilter));
    logger.info("get log data files {} when forcing delete all logs", logFiles);
    for (File logFile : logFiles) {
      try {
        FileUtils.forceDelete(logFile);
      } catch (IOException e) {
        logger.error("forcing delete log data file={} failed", logFile.getAbsoluteFile(), e);
      }
    }
    logDataFileList.clear();
  }

  private void forceDeleteAllLogIndexFiles() {
    FileFilter logIndexFilter =
        pathname -> {
          String s = pathname.getName();
          return s.endsWith(LOG_INDEX_FILE_SUFFIX);
        };

    List<File> logIndexFiles = Arrays.asList(metaFile.getParentFile().listFiles(logIndexFilter));
    logger.info("get log index files {} when forcing delete all logs", logIndexFiles);
    for (File logFile : logIndexFiles) {
      try {
        FileUtils.forceDelete(logFile);
      } catch (IOException e) {
        logger.error("forcing delete log index file={} failed", logFile.getAbsoluteFile(), e);
      }
    }
    logIndexFileList.clear();
  }

  private void forceDeleteAllLogFiles() {
    while (!logDataFileList.isEmpty()) {
      boolean success = deleteTheFirstLogDataAndIndexFile();
      if (!success) {
        forceDeleteAllLogDataFiles();
        forceDeleteAllLogIndexFiles();
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  private boolean deleteTheFirstLogDataAndIndexFile() {
    if (logDataFileList.isEmpty()) {
      return true;
    }

    File logDataFile = null;
    IndexFileDescriptor logIndexFile = null;

    lock.lock();
    try {
      logDataFile = logDataFileList.get(0);
      logIndexFile = logIndexFileList.get(0);
      if (logDataFile == null || logIndexFile == null) {
        logger.error("the log data or index file is null, some error occurred");
        return false;
      }
      Files.delete(logDataFile.toPath());
      Files.delete(logIndexFile.file.toPath());
      logDataFileList.remove(0);
      logIndexFileList.remove(0);
      logger.debug(
          "delete date file={}, index file={}",
          logDataFile.getAbsoluteFile(),
          logIndexFile.file.getAbsoluteFile());
    } catch (IOException e) {
      logger.error(
          "delete file failed, data file={}, index file={}",
          logDataFile.getAbsoluteFile(),
          logIndexFile.file.getAbsoluteFile());
      return false;
    } finally {
      lock.unlock();
    }
    return true;
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
          file1.getAbsoluteFile(),
          file2.getAbsoluteFile());
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
   * @param endIndex the log end index
   * @return the raft log which index between [startIndex, endIndex] or empty if not found
   */
  @Override
  public List<Entry> getEntries(long startIndex, long endIndex, boolean limitBatch) {
    if (startIndex > endIndex) {
      logger.error(
          "startIndex={} should be less than or equal to endIndex={}", startIndex, endIndex);
      return Collections.emptyList();
    }
    if (startIndex < 0) {
      startIndex = 0;
    }

    long newEndIndex;
    if (limitBatch && endIndex - startIndex > maxNumberOfLogsPerFetchOnDisk) {
      newEndIndex = startIndex + maxNumberOfLogsPerFetchOnDisk;
    } else {
      newEndIndex = endIndex;
    }
    logger.debug(
        "intend to get logs between[{}, {}], actually get logs between[{},{}]",
        startIndex,
        endIndex,
        startIndex,
        newEndIndex);

    // maybe the logs will be deleted during checkDeletePersistRaftLog or clearAllLogs,
    // use lock for two reasons:
    // 1.if the log file to read is the last log file, we need to get write lock to flush logBuffer,
    // 2.prevent these log files from being deleted
    lock.lock();
    try {
      List<Pair<File, Pair<Long, Long>>> logDataFileAndOffsetList =
          getLogDataFileAndOffset(startIndex, newEndIndex);
      if (logDataFileAndOffsetList.isEmpty()) {
        return Collections.emptyList();
      }

      List<Entry> result = new ArrayList<>();
      for (Pair<File, Pair<Long, Long>> pair : logDataFileAndOffsetList) {
        result.addAll(getLogsFromOneLogDataFile(pair.left, pair.right));
      }
      long finalStartIndex = startIndex;
      result.removeIf(
          e -> !(finalStartIndex <= e.getCurrLogIndex() && e.getCurrLogIndex() <= newEndIndex));

      return result;
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param logIndex the log's index
   * @return The offset of the data file corresponding to the log index, -1 if not found
   */
  public long getOffsetAccordingToLogIndex(long logIndex) {
    long offset = -1;

    // 1. first find in memory
    if (logIndex >= firstLogIndex) {
      for (Pair<Long, Long> indexOffset : logIndexOffsetList) {
        // end index
        if (indexOffset.left >= logIndex) {
          return indexOffset.right;
        }
      }
    }

    logger.debug(
        "can not found the offset in memory, logIndex={}, firstLogIndex={}, logIndexOffsetList size={}",
        logIndex,
        firstLogIndex,
        logIndexOffsetList.size());

    // 2. second read the log index file
    IndexFileDescriptor descriptor = getLogIndexFile(logIndex);
    if (descriptor == null) {
      return -1;
    }
    File file = descriptor.file;
    logger.debug(
        "start to read the log index file={} for log index={}, file size={}",
        file.getAbsoluteFile(),
        logIndex,
        file.length());
    long endIndex;
    try (FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      while (bufferedInputStream.available() > 0) {
        endIndex = ReadWriteIOUtils.readLong(bufferedInputStream);
        offset = ReadWriteIOUtils.readLong(bufferedInputStream);
        if (endIndex >= logIndex) {
          return offset;
        }
      }
    } catch (IOException e) {
      logger.error("can not read the log index file={}", file.getAbsoluteFile(), e);
    }
    return -1;
  }

  /**
   * @param startIndex the log start index
   * @param endIndex the log end index
   * @return first value-> the log data file, second value-> the left value is the start offset of
   *     the file, the right is the end offset of the file
   */
  private List<Pair<File, Pair<Long, Long>>> getLogDataFileAndOffset(
      long startIndex, long endIndex) {
    long startIndexInOneFile = startIndex;
    long endIndexInOneFile = 0;
    List<Pair<File, Pair<Long, Long>>> fileNameWithStartAndEndOffset = new ArrayList<>();
    // 1. get the start offset with the startIndex
    long startOffset = getOffsetAccordingToLogIndex(startIndexInOneFile);
    if (startOffset == -1) {
      return Collections.emptyList();
    }
    Pair<File, Pair<Long, Long>> logDataFileWithStartAndEndLogIndex =
        getLogDataFile(startIndexInOneFile);
    if (logDataFileWithStartAndEndLogIndex == null) {
      return Collections.emptyList();
    }
    endIndexInOneFile = logDataFileWithStartAndEndLogIndex.right.right;
    // 2. judge whether the fileEndLogIndex>=endIndex
    while (endIndex > endIndexInOneFile) {
      //  this means the endIndex's offset can not be found in the file
      //  logDataFileWithStartAndEndLogIndex.left; and should be find in the next log data file.
      // 3. get the file's end offset
      long endOffset = getOffsetAccordingToLogIndex(endIndexInOneFile);
      fileNameWithStartAndEndOffset.add(
          new Pair<>(logDataFileWithStartAndEndLogIndex.left, new Pair<>(startOffset, endOffset)));
      logger.debug(
          "get log data offset=[{},{}] according to log index=[{},{}], file={}",
          startOffset,
          endOffset,
          startIndexInOneFile,
          endIndexInOneFile,
          logDataFileWithStartAndEndLogIndex.left);
      logDataFileWithStartAndEndLogIndex = null;

      // 4. search the next file to get the log index of fileEndLogIndex + 1
      startIndexInOneFile = endIndexInOneFile + 1;
      startOffset = getOffsetAccordingToLogIndex(startIndexInOneFile);
      if (startOffset == -1) {
        break;
      }
      logDataFileWithStartAndEndLogIndex = getLogDataFile(startIndexInOneFile);
      endIndexInOneFile = logDataFileWithStartAndEndLogIndex.right.right;
    }

    if (logDataFileWithStartAndEndLogIndex != null) {
      // this means the endIndex's offset can not be found in the file
      // logDataFileWithStartAndEndLogIndex.left
      long endOffset =
          getOffsetAccordingToLogIndex(
              Math.min(endIndex, logDataFileWithStartAndEndLogIndex.right.right));
      fileNameWithStartAndEndOffset.add(
          new Pair<>(logDataFileWithStartAndEndLogIndex.left, new Pair<>(startOffset, endOffset)));
      logger.debug(
          "get log data offset=[{},{}] according to log index=[{},{}], file={}",
          startOffset,
          endOffset,
          startIndexInOneFile,
          endIndex,
          logDataFileWithStartAndEndLogIndex.left);
    }
    return fileNameWithStartAndEndOffset;
  }

  /**
   * @param startIndex the start log index
   * @return the first value of the pair is the log index file which contains the start index; the
   *     second pair's first value is the file's start log index. the second pair's second value is
   *     the file's end log index. null if not found
   */
  public IndexFileDescriptor getLogIndexFile(long startIndex) {
    for (IndexFileDescriptor descriptor : logIndexFileList) {
      if (descriptor.startIndex <= startIndex && startIndex <= descriptor.endIndex) {
        return descriptor;
      }
    }
    logger.debug("can not found the log index file for startIndex={}", startIndex);
    return null;
  }

  /**
   * @param startIndex the start log index
   * @return the first value of the pair is the log data file which contains the start index; the
   *     second pair's first value is the file's start log index. the second pair's second value is
   *     the file's end log index. null if not found
   */
  public Pair<File, Pair<Long, Long>> getLogDataFile(long startIndex) {
    for (File file : logDataFileList) {
      String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
      if (splits.length != FILE_NAME_PART_LENGTH) {
        logger.error(
            "file={} name should be in the following format: startLogIndex-endLogIndex-version-data",
            file.getAbsoluteFile());
      }
      if (Long.parseLong(splits[0]) <= startIndex && startIndex <= Long.parseLong(splits[1])) {
        return new Pair<>(file, new Pair<>(Long.parseLong(splits[0]), Long.parseLong(splits[1])));
      }
    }
    logger.debug("can not found the log data file for startIndex={}", startIndex);
    return null;
  }

  /**
   * @param file the log data file
   * @param startAndEndOffset the left value is the start offset of the file, the right is the end
   *     offset of the file
   * @return the logs between start offset and end offset
   */
  private List<Entry> getLogsFromOneLogDataFile(File file, Pair<Long, Long> startAndEndOffset) {
    List<Entry> result = new ArrayList<>();
    if (file.getName().equals(getCurrentLogDataFile().getName())) {
      forceFlushLogBufferWithoutCloseFile();
    }
    try (FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      long bytesSkip = bufferedInputStream.skip(startAndEndOffset.left);
      if (bytesSkip != startAndEndOffset.left) {
        logger.error(
            "read file={} failed when skip {} bytes, actual skip bytes={}",
            file.getAbsoluteFile(),
            startAndEndOffset.left,
            bytesSkip);
        return result;
      }

      logger.debug(
          "start to read file={} and skip {} bytes, startOffset={}, endOffset={}, fileLength={}",
          file.getAbsoluteFile(),
          bytesSkip,
          startAndEndOffset.left,
          startAndEndOffset.right,
          file.length());

      long currentReadOffset = bytesSkip;
      // because we want to get all the logs whose offset between [startAndEndOffset.left,
      // startAndEndOffset.right]
      // which means, the last offset's value should be still read, in other words,
      // the first log index of the offset starting with startAndEndOffset.right also needs to be
      // read.
      while (currentReadOffset <= startAndEndOffset.right) {
        logger.debug(
            "read file={}, currentReadOffset={}, end offset={}",
            file.getAbsoluteFile(),
            currentReadOffset,
            startAndEndOffset.right);
        int logBlockSize = ReadWriteIOUtils.readInt(bufferedInputStream);
        byte[] bytes = ReadWriteIOUtils.readBytes(bufferedInputStream, logBlockSize);
        ByteBuffer uncompressed = ByteBuffer.wrap(unCompressor.uncompress(bytes));
        while (uncompressed.remaining() > 0) {
          Entry e = parser.parse(uncompressed, null);
          result.add(e);
        }
        currentReadOffset = currentReadOffset + Integer.BYTES + logBlockSize;
      }
    } catch (UnknownLogTypeException e) {
      logger.error("Unknown log detected ", e);
    } catch (IOException e) {
      logger.error("Cannot read log from file={} ", file.getAbsoluteFile(), e);
    }
    return result;
  }

  @Override
  public long getPersistedLogIndex() {
    return persistedLogIndex;
  }

  /**
   * VersionController manages the version(a monotonically increasing long) of a storage group. We
   * define that each memtable flush, data deletion, or data update will generate a new version of
   * dataset. So upon the above actions are performed, a new version number is generated and
   * assigned to such actions. Additionally, we also assign versions to TsFiles in their file names,
   * so hopefully we will compare files directly across IoTDB replicas. NOTICE: Thread-safety should
   * be guaranteed by the caller.
   */
  public interface VersionController {

    /**
     * Get the next version number.
     *
     * @return the next version number.
     */
    long nextVersion();

    /**
     * Get the current version number.
     *
     * @return the current version number.
     */
    long currVersion();

    class SimpleFileVersionController implements VersionController {

      private static final Logger logger =
          LoggerFactory.getLogger(SimpleFileVersionController.class);
      public static final String FILE_PREFIX = "Version-";
      public static final String UPGRADE_DIR = "upgrade";
      /**
       * Every time currVersion - prevVersion >= saveInterval, currVersion is persisted and
       * prevVersion is set to currVersion. When recovering from file, the version number is
       * automatically increased by saveInterval to avoid conflicts.
       */
      private static long saveInterval = 100;
      /** time partition id to dividing time series into different storage group */
      private long timePartitionId;

      private long prevVersion;
      private long currVersion;
      private String directoryPath;

      public SimpleFileVersionController(String directoryPath, long timePartitionId)
          throws IOException {
        this.directoryPath = directoryPath + File.separator + timePartitionId;
        this.timePartitionId = timePartitionId;
        restore();
      }

      /** only used for upgrading */
      public SimpleFileVersionController(String directoryPath) throws IOException {
        this.directoryPath = directoryPath + File.separator + UPGRADE_DIR;
        restore();
      }

      public static long getSaveInterval() {
        return saveInterval;
      }

      // test only method
      public static void setSaveInterval(long saveInterval) {
        SimpleFileVersionController.saveInterval = saveInterval;
      }

      public long getTimePartitionId() {
        return timePartitionId;
      }

      public void setTimePartitionId(long timePartitionId) {
        this.timePartitionId = timePartitionId;
      }

      @Override
      public synchronized long nextVersion() {
        currVersion++;
        try {
          checkPersist();
        } catch (IOException e) {
          logger.error("Error occurred when getting next version.", e);
        }
        return currVersion;
      }

      /**
       * Test only method, no need for concurrency.
       *
       * @return the current version.
       */
      @Override
      public long currVersion() {
        return currVersion;
      }

      private void checkPersist() throws IOException {
        if ((currVersion - prevVersion) >= saveInterval) {
          persist();
        }
      }

      private void persist() throws IOException {
        File oldFile = SystemFileFactory.INSTANCE.getFile(directoryPath, FILE_PREFIX + prevVersion);
        File newFile = SystemFileFactory.INSTANCE.getFile(directoryPath, FILE_PREFIX + currVersion);
        if (oldFile.exists()) {
          FileUtils.moveFile(oldFile, newFile);
        }
        logger.info(
            "Version file updated, previous: {}, current: {}",
            oldFile.getAbsolutePath(),
            newFile.getAbsolutePath());
        prevVersion = currVersion;
      }

      /** recovery from disk */
      @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
      private void restore() throws IOException {
        File directory = SystemFileFactory.INSTANCE.getFile(directoryPath);
        if (!directory.exists()) {
          directory.mkdirs();
        }
        File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(FILE_PREFIX));
        File versionFile;
        if (versionFiles != null && versionFiles.length > 0) {
          long maxVersion = 0;
          int maxVersionIndex = 0;
          for (int i = 0; i < versionFiles.length; i++) {
            // extract version from "Version-123456"
            long fileVersion = Long.parseLong(versionFiles[i].getName().split("-")[1]);
            if (fileVersion > maxVersion) {
              maxVersion = fileVersion;
              maxVersionIndex = i;
            }
          }
          prevVersion = maxVersion;
          for (int i = 0; i < versionFiles.length; i++) {
            if (i != maxVersionIndex) {
              versionFiles[i].delete();
            }
          }
        } else {
          versionFile = SystemFileFactory.INSTANCE.getFile(directory, FILE_PREFIX + "0");
          prevVersion = 0;
          if (!versionFile.createNewFile()) {
            logger.warn("Cannot create new version file {}", versionFile);
          }
        }
        // prevent overlapping in case of failure
        currVersion = prevVersion + saveInterval;
        persist();
      }
    }
  }
}
