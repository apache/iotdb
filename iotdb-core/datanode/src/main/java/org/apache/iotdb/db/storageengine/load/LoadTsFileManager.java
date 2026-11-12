/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.load.LoadFileNotReadyException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.exception.write.PageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link LoadTsFileManager} is used for dealing with {@link LoadTsFilePieceNode} and {@link
 * LoadCommand}. This class turn the content of a piece of loading TsFile into a new TsFile. When
 * DataNode finish transfer pieces, this class will flush all TsFile and load them into IoTDB, or
 * delete all.
 *
 * <p>This class is the entry point of the staging area of one DataRegion and does not hold the
 * staging details itself: the physical state of one task lives in {@link TsFileWriterManager}, the
 * directories live in {@link LoadStagingDirs}, and the lifetime of the staged bytes of a finished
 * consensus task is decided by {@link LoadTaskRetention}.
 */
public class LoadTsFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileManager.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  static final String TERMINAL_HISTORY_DIR = ".finished";

  public static final Cache<String, String> MEASUREMENT_ID_CACHE =
      Caffeine.newBuilder()
          .maximumWeight(CONFIG.getLoadMeasurementIdCacheSizeInBytes())
          .weigher((String k, String v) -> v.length())
          .build();

  /** The staged state of every LOAD task this region is still building. */
  private final Map<String, TsFileWriterManager> uuid2WriterManager = new ConcurrentHashMap<>();

  /**
   * The slices received so far of the pieces this region is still assembling. A piece is sent in
   * slices when its serialized body does not fit into one internal RPC frame, and only the
   * reassembled body is written into the staged file.
   */
  private final Map<String, LoadTsFilePieceNodeAssembler> uuid2PieceNodeAssembler =
      new ConcurrentHashMap<>();

  /** The staged directories of the tasks that already reached COMMIT or ABORT. */
  private final LoadTaskRetention retention;

  /**
   * The tasks this region finished, most recent last, to answer repeated consensus commands.
   *
   * <p>This is only a bounded cache. Terminal records also live outside the staged task directories
   * for the lifetime of the region, and are included in its LOAD snapshot. A replica must still
   * recognize a command whose successful response was lost after staging cleanup or a restart;
   * treating that command as missing data would otherwise retry it forever.
   */
  private static final int FINISHED_TASK_HISTORY = 256;

  private final Map<String, LoadTsFileConsensusOp> finishedLoadIds =
      Collections.synchronizedMap(
          new LinkedHashMap<String, LoadTsFileConsensusOp>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(
                final Map.Entry<String, LoadTsFileConsensusOp> eldest) {
              return size() > FINISHED_TASK_HISTORY;
            }
          });

  private final DataRegion dataRegion;

  /** Pieces read, stop() and closing a writer write. Fair, so a stop is not starved. */
  private final ReentrantReadWriteLock writersLock = new ReentrantReadWriteLock(true);

  /** Fences COMMIT/ABORT against selection and copying of a data and LOAD-state snapshot. */
  private final ReentrantReadWriteLock snapshotBoundaryLock = new ReentrantReadWriteLock(true);

  /** Set under the write side, read as the fast path of a piece. */
  private volatile boolean stopped = false;

  public LoadTsFileManager(final DataRegion dataRegion) {
    this.dataRegion = Objects.requireNonNull(dataRegion);
    this.retention = new LoadTaskRetention(dataRegion);
    recover();
  }

  public void stop() {
    final Lock writeLock = writersLock.writeLock();
    writeLock.lock();
    try {
      if (stopped) {
        return;
      }
      stopped = true;
      new HashSet<>(uuid2WriterManager.keySet()).forEach(this::forceCloseWriterManager);
      uuid2PieceNodeAssembler.clear();
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isStopped() {
    return stopped;
  }

  /**
   * When the task last received a command, or -1 when this region does not know it. A command path
   * only ever asks about its own tasks, so -1 here means the task is gone.
   */
  long getLastCommandTimeMillis(final String loadId) {
    final TsFileWriterManager writerManager = uuid2WriterManager.get(loadId);
    return writerManager == null ? -1L : writerManager.getLastCommandTimeMillis();
  }

  /**
   * Reclaims the tasks of this region that received no command for the idle timeout. A LOAD command
   * of any task triggers it, so a task that went quiet is reclaimed without waiting for the sweep.
   */
  private void reclaimIdleTasks() {
    final LoadTsFileCleaner cleaner = LoadTsFileCleaner.getInstance();
    for (final Map.Entry<String, TsFileWriterManager> entry : uuid2WriterManager.entrySet()) {
      cleaner.reclaimIfIdle(
          dataRegion,
          entry.getKey(),
          entry.getValue().getTaskDir(),
          entry.getValue().getLastCommandTimeMillis());
    }
  }

  /**
   * Appends one slice of a piece. Slices are dispatched in order, so a slice that is not the first
   * one is rejected when the slices before it did not arrive, and the assembler is dropped as soon
   * as the piece is complete or one of its slices turned out to be invalid.
   */
  public LoadTsFilePieceNodeAssembler.Result appendPieceNodeSlice(
      final String uuid,
      final ByteBuffer body,
      final int sliceIndex,
      final int sliceCount,
      final int originBodySize) {
    synchronized (uuid2PieceNodeAssembler) {
      final LoadTsFilePieceNodeAssembler assembler;
      if (sliceIndex == 0) {
        assembler = new LoadTsFilePieceNodeAssembler(sliceCount, originBodySize);
        uuid2PieceNodeAssembler.put(uuid, assembler);
      } else {
        assembler = uuid2PieceNodeAssembler.get(uuid);
        if (assembler == null) {
          return LoadTsFilePieceNodeAssembler.Result.invalid(
              String.format(
                  StorageEngineMessages
                      .MESSAGE_MISSING_LOAD_TSFILE_ASSEMBLER_FOR_UUID_ARG_DATAREGION_ARG_SLICEINDEX_ARG_FF6EA463,
                  uuid,
                  dataRegion.getDataRegionIdString(),
                  sliceIndex));
        }
      }

      final LoadTsFilePieceNodeAssembler.Result result =
          assembler.append(body, sliceIndex, sliceCount, originBodySize);
      if (!result.isValid() || result.isComplete()) {
        uuid2PieceNodeAssembler.remove(uuid, assembler);
      }
      return result;
    }
  }

  /**
   * Picks up the staged directories a previous run left behind: a directory whose task already
   * reached COMMIT or ABORT carries that record in the tail of its progress files and is queued for
   * release, while any other directory belongs to a task that is still in progress and is resumed
   * from its progress files.
   */
  private void recover() {
    // A restart may leave staged directories that already reached COMMIT or ABORT behind, so the
    // DataNode level cleaner is started here as well: its scan reads the tail of the progress files
    // and deletes whatever the previous run did not get to delete.
    LoadTsFileCleaner.getInstance().start();
    final File[] baseDirs =
        Arrays.stream(LoadStagingDirs.configuredBaseDirs())
            .map(File::new)
            .map(this::getDataRegionLoadDir)
            .toArray(File[]::new);
    for (final File baseDir : baseDirs) {
      final File[] uuidDirs = baseDir.listFiles();
      if (uuidDirs == null) {
        continue;
      }
      for (final File uuidDir : uuidDirs) {
        if (!uuidDir.isDirectory() || TERMINAL_HISTORY_DIR.equals(uuidDir.getName())) {
          continue;
        }
        final String uuid = uuidDir.getName();
        try {
          final LoadTsFileProgress.TerminalRecord terminal = readTerminalHistory(uuid);
          if (terminal != null) {
            // The process may have stopped after publishing the decision but before closing or
            // marking staging. That directory belongs to a finished task, never an active writer.
            finishedLoadIds.put(uuid, terminal.op());
            retention.retain(uuid, uuidDir, terminal.op(), terminal.searchIndex());
            continue;
          }
          final LoadTsFileProgress.TerminalRecord retainedTerminal =
              LoadTsFileProgress.readTerminalOfTaskDir(uuidDir);
          if (retainedTerminal != null) {
            // Older versions kept the decision only in each staged file's progress tail.
            // Migrate it before registering a listener that may release the only staged copy.
            rememberTerminal(uuid, retainedTerminal.op(), retainedTerminal.searchIndex());
            retention.retain(uuid, uuidDir, retainedTerminal.op(), retainedTerminal.searchIndex());
            continue;
          }
          final TsFileWriterManager recovered =
              new TsFileWriterManager(dataRegion, uuidDir).recoverFromDisk();
          if (recovered.hasPendingTsFiles()) {
            uuid2WriterManager.put(uuid, recovered);
            LOGGER.info(
                StorageEngineMessages.LOG_RECOVERED_LOAD_WRITER_MANAGER_FOR_UUID_ARG_E0430FB8,
                uuid);
          }
        } catch (final Exception e) {
          LOGGER.warn(
              StorageEngineMessages.LOG_FAILED_TO_RECOVER_LOAD_WRITER_MANAGER_FOR_UUID_ARG_CBB34D4B,
              uuid,
              e);
        }
      }
    }
    retention.release();
  }

  public void writeToDataRegion(LoadTsFilePieceNode pieceNode, String uuid)
      throws IOException, PageException {
    writePiece(uuid, pieceNode.getAllTsFileData());
  }

  public List<LoadTsFileConsensusNode.PieceRef> writePiece(
      final String uuid, final List<TsFileData> tsFileDataList) throws IOException, PageException {
    if (stopped) {
      throw newStoppedIOException();
    }
    final Lock readLock = writersLock.readLock();
    readLock.lock();
    try {
      // One piece per staged file: a region applies the commands of a load in order.
      return getOrCreateWriterManager(uuid).writePiece(tsFileDataList);
    } finally {
      readLock.unlock();
    }
  }

  private TsFileWriterManager getOrCreateWriterManager(final String uuid) throws IOException {
    // The caller holds the read side, so no stop can be sweeping the writers here.
    if (stopped) {
      throw newStoppedIOException();
    }
    final AtomicReference<Exception> exception = new AtomicReference<>();
    final TsFileWriterManager writerManager =
        uuid2WriterManager.computeIfAbsent(
            uuid,
            o -> {
              try {
                return LoadStagingDirs.folderManager()
                    .getNextWithRetry(
                        folder ->
                            new TsFileWriterManager(
                                dataRegion,
                                new File(getDataRegionLoadDir(new File(folder)), uuid)));
              } catch (DiskSpaceInsufficientException e) {
                exception.set(e);
                return null;
              }
            });

    if (exception.get() != null || writerManager == null) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEWRITERMANAGER_FOR_UUID_S_BECAUSE_A0D68950,
              uuid),
          exception.get());
    }
    return writerManager;
  }

  private IOException newStoppedIOException() {
    return new IOException(
        String.format(
            StorageEngineMessages
                .STORAGE_EXCEPTION_THE_LOAD_MANAGER_OF_REGION_ARG_HAS_BEEN_STOPPED_67D2DBDE,
            dataRegion.getDatabaseName() + "-" + dataRegion.getDataRegionIdString()));
  }

  private File getDataRegionLoadDir(final File baseDir) {
    return LoadStagingDirs.regionLoadDir(
        baseDir, dataRegion.getDatabaseName(), dataRegion.getDataRegionIdString());
  }

  public List<LoadTsFileConsensusNode.PieceRef> writePiece(final LoadTsFileConsensusNode node)
      throws IOException, PageException {
    // Every chunk entry records the consensus index of the piece that brought it, so the progress
    // of
    // a staged file can be read back command by command.
    final List<LoadTsFileConsensusNode.PieceRef> refs;
    if (stopped) {
      throw newStoppedIOException();
    }
    if (node.isGeneratedByRemoteConsensusLeader() && getTerminalOp(node.getLoadId()) != null) {
      return Collections.emptyList();
    }
    final Lock readLock = writersLock.readLock();
    readLock.lock();
    try {
      refs =
          getOrCreateWriterManager(node.getLoadId())
              .writePiece(node.getTsFileDataList(), node.getSearchIndex());
    } finally {
      readLock.unlock();
    }
    if (node.getPieceRefs().isEmpty()) {
      // The chunks of this piece have just been written into the staged file, which recorded where
      // the payload of each of them landed. The WAL entry keeps everything the leader received -
      // device, headers, statistics and layout - and only replaces the payload with those
      // references, so a replica can be given the very same piece later on
      node.setPieceRefs(refs);
      logLoadNodeToWAL(node);
      dataRegion.insertSeparatorToWAL(node);
    }
    reclaimIdleTasks();
    return refs;
  }

  /**
   * The directories this DataNode stages the files of LOAD tasks in. They are the directories the
   * staged payload references point into, and the only ones it may be read back from.
   */
  public static String[] getLoadBaseDirs() {
    return LoadStagingDirs.baseDirs();
  }

  /**
   * The staging directories of the loads this region is currently building (the PIECE and PREPARE
   * phases). A snapshot must include their contents so a recovering replica can inherit the partial
   * physical state and the {@link LoadTsFileProgress} bitmaps of those loads.
   */
  public List<File> getActiveTaskDirs() {
    final List<File> result = new ArrayList<>(uuid2WriterManager.size());
    for (final TsFileWriterManager writerManager : uuid2WriterManager.values()) {
      result.add(writerManager.getTaskDir());
    }
    return result;
  }

  /** Holds the write side while a region snapshot selects its data and LOAD state. */
  public void lockSnapshotBoundary() {
    snapshotBoundaryLock.writeLock().lock();
  }

  public void unlockSnapshotBoundary() {
    snapshotBoundaryLock.writeLock().unlock();
  }

  /** Terminal metadata is snapshotted separately from active tasks and their staged bytes. */
  List<File> getTerminalHistoryDirs() {
    return getTerminalHistoryDirs(dataRegion.getDatabaseName(), dataRegion.getDataRegionIdString());
  }

  static List<File> getTerminalHistoryDirs(final String databaseName, final String dataRegionId) {
    final List<File> result = new ArrayList<>();
    for (final String baseDir : LoadStagingDirs.configuredBaseDirs()) {
      final File historyDir =
          new File(
              LoadStagingDirs.regionLoadDir(new File(baseDir), databaseName, dataRegionId),
              TERMINAL_HISTORY_DIR);
      if (historyDir.isDirectory()) {
        result.add(historyDir);
      }
    }
    return result;
  }

  static boolean hasTerminalHistory(
      final String databaseName, final String dataRegionId, final String loadId) {
    for (final File historyDir : getTerminalHistoryDirs(databaseName, dataRegionId)) {
      try {
        if (LoadTsFileProgress.readTerminal(
                LoadTsFileProgress.progressFileFor(new File(historyDir, loadId)))
            != null) {
          return true;
        }
      } catch (final IOException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
            historyDir.getPath(),
            e);
        return false;
      }
    }
    return false;
  }

  /** Region deletion ends consensus retries; ordinary shutdown must keep these records. */
  public static void clearTerminalHistory(final String databaseName, final String dataRegionId) {
    for (final String baseDir : LoadStagingDirs.configuredBaseDirs()) {
      LoadStagingDirs.deleteTaskDir(
          new File(
              LoadStagingDirs.regionLoadDir(new File(baseDir), databaseName, dataRegionId),
              TERMINAL_HISTORY_DIR));
    }
  }

  private void rememberTerminal(final LoadTsFileConsensusNode node, final LoadTsFileConsensusOp op)
      throws IOException {
    rememberTerminal(node.getLoadId(), op, node.getSearchIndex());
  }

  private void rememberTerminal(
      final String loadId, final LoadTsFileConsensusOp op, final long searchIndex)
      throws IOException {
    // Region deletion clears history after stop(), so no terminal publication may outlive it.
    final Lock readLock = writersLock.readLock();
    readLock.lock();
    try {
      if (stopped) {
        throw newStoppedIOException();
      }
      final File historyDir =
          new File(
              getDataRegionLoadDir(new File(LoadStagingDirs.configuredBaseDirs()[0])),
              TERMINAL_HISTORY_DIR);
      final LoadTsFileProgress terminal =
          new LoadTsFileProgress(new File(historyDir, loadId + ".copying." + UUID.randomUUID()));
      final File temporaryRecord = terminal.getProgressFile();
      final File record = LoadTsFileProgress.progressFileFor(new File(historyDir, loadId));
      try {
        terminal.recordTerminal(op, searchIndex);
        try {
          Files.move(
              temporaryRecord.toPath(),
              record.toPath(),
              StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        } catch (final AtomicMoveNotSupportedException e) {
          Files.move(
              temporaryRecord.toPath(), record.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        Files.deleteIfExists(temporaryRecord.toPath());
      }
      finishedLoadIds.put(loadId, op);
    } finally {
      readLock.unlock();
    }
  }

  private LoadTsFileProgress.TerminalRecord readTerminalHistory(final String loadId)
      throws IOException {
    for (final File historyDir : getTerminalHistoryDirs()) {
      final LoadTsFileProgress.TerminalRecord terminal =
          LoadTsFileProgress.readTerminal(
              LoadTsFileProgress.progressFileFor(new File(historyDir, loadId)));
      if (terminal != null) {
        return terminal;
      }
    }
    return null;
  }

  /** The terminal command this region already applied to the task, or null if it is unknown. */
  private LoadTsFileConsensusOp getTerminalOp(final String loadId) throws IOException {
    final LoadTsFileConsensusOp finishedOp = finishedLoadIds.get(loadId);
    if (finishedOp != null) {
      return finishedOp;
    }
    final LoadTsFileProgress.TerminalRecord history = readTerminalHistory(loadId);
    if (history != null) {
      finishedLoadIds.put(loadId, history.op());
      return history.op();
    }
    for (final String baseDir : LoadStagingDirs.configuredBaseDirs()) {
      final File taskDir =
          new File(
              LoadStagingDirs.regionLoadDir(
                  new File(baseDir),
                  dataRegion.getDatabaseName(),
                  dataRegion.getDataRegionIdString()),
              loadId);
      final File[] files = taskDir.listFiles(LoadTsFileProgress.PROGRESS_FILE_FILTER);
      if (files == null) {
        continue;
      }
      for (final File file : files) {
        final LoadTsFileProgress.TerminalRecord terminal;
        try {
          terminal = LoadTsFileProgress.readTerminal(file);
        } catch (final IOException e) {
          LOGGER.warn(
              StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
              file.getPath(),
              e);
          continue;
        }
        if (terminal != null) {
          rememberTerminal(loadId, terminal.op(), terminal.searchIndex());
          return terminal.op();
        }
      }
    }
    return null;
  }

  public boolean prepare(
      final LoadTsFileConsensusNode node,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    // A terminal decision also fences late pieces and retries from an older consensus batch.
    if (node.isGeneratedByRemoteConsensusLeader() && getTerminalOp(node.getLoadId()) != null) {
      return true;
    }
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      // A lost PREPARE acknowledgement can be retried after the follower applied COMMIT or ABORT.
      if (node.isGeneratedByRemoteConsensusLeader()) {
        throw new LoadFileNotReadyException();
      }
      return false;
    }
    if (!uuid2WriterManager.get(node.getLoadId()).hasStagedData()) {
      final String message =
          String.format(
              StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PREPARE_WITHOUT_STAGED_DATA_FE8ADC37,
              node.getLoadId());
      if (node.isGeneratedByRemoteConsensusLeader()) {
        throw new LoadFileNotReadyException(message);
      }
      throw new LoadFileException(message);
    }
    LOGGER.info(
        StorageEngineMessages
            .LOG_PREPARING_LOAD_TSFILE_ARG_ARG_PIECES_OF_ARG_BYTES_OF_STAGED_RESOURCES_5F00E102,
        node.getLoadId(),
        node.getPieceCount(),
        node.getTotalBytes());
    uuid2WriterManager
        .get(node.getLoadId())
        .prepare(
            node.isGeneratedByPipe(),
            timePartitionProgressIndexMap,
            node.getTimePartition2ExpectedDataEnd());
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    reclaimIdleTasks();
    return true;
  }

  public boolean loadAll(
      final LoadTsFileConsensusNode node,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    final Lock snapshotReadLock = snapshotBoundaryLock.readLock();
    snapshotReadLock.lock();
    final boolean mustRetainStagedFiles;
    try {
      final LoadTsFileConsensusOp terminalOp = getTerminalOp(node.getLoadId());
      if (terminalOp != null) {
        return terminalOp == LoadTsFileConsensusOp.COMMIT;
      }
      if (!uuid2WriterManager.containsKey(node.getLoadId())) {
        if (node.isGeneratedByRemoteConsensusLeader()) {
          throw new LoadFileNotReadyException();
        }
        return false;
      }
      LOGGER.info(
          StorageEngineMessages
              .LOG_COMMITTING_LOAD_TSFILE_ARG_LOADING_PREPARED_RESOURCES_INTO_DATAREGION_EA1D6335,
          node.getLoadId());
      // Whether the staged bytes are still needed has to be decided before they are imported: the
      // WAL entries of the pieces of this task only reference those bytes, and they are expanded on
      // this node for a replica that has not applied them yet. Importing them by moving the staged
      // file away would leave those entries unexpandable, so a copy is imported while the watermark
      // of the WAL has not got past this command yet.
      mustRetainStagedFiles = retention.mustRetain(node.getSearchIndex());
      uuid2WriterManager
          .get(node.getLoadId())
          .loadAll(
              node.isGeneratedByPipe(),
              !mustRetainStagedFiles && !node.isGeneratedByRemoteConsensusLeader());
      // Keep a follower's staged copy until the terminal answer is durable, even without a local
      // WAL watermark to retain it. A restart before this record can then resume the prepared task.
      rememberTerminal(node, LoadTsFileConsensusOp.COMMIT);
      finishConsensusTask(node, mustRetainStagedFiles, LoadTsFileConsensusOp.COMMIT);
    } finally {
      snapshotReadLock.unlock();
    }
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  public boolean deleteAll(final LoadTsFileConsensusNode node) throws IOException {
    final Lock snapshotReadLock = snapshotBoundaryLock.readLock();
    snapshotReadLock.lock();
    final boolean hasWriter;
    try {
      if (getTerminalOp(node.getLoadId()) != null) {
        return false;
      }
      hasWriter = uuid2WriterManager.containsKey(node.getLoadId());
      // ABORT may overtake every PIECE. Its success must still settle pending PREPARE retries.
      rememberTerminal(node, LoadTsFileConsensusOp.ABORT);
      if (hasWriter) {
        finishConsensusTask(
            node, retention.mustRetain(node.getSearchIndex()), LoadTsFileConsensusOp.ABORT);
      }
    } finally {
      snapshotReadLock.unlock();
    }
    if (!hasWriter) {
      logLoadNodeToWAL(node);
      dataRegion.insertSeparatorToWAL(node);
      return false;
    }
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  /**
   * Closes the staged resources of a consensus LOAD that reached COMMIT or ABORT.
   *
   * <p>A task that still has to be expanded for a replica keeps its staged bytes — for COMMIT they
   * are the copy the import was made from, for ABORT they are the files that were never imported —
   * and records that terminal in the tail of their progress files. They are released only once the
   * safe-deletion watermark of the WAL reports that every replica got past the COMMIT or ABORT
   * command, because until then a replica that catches up by reading this node's WAL still reads
   * those bytes back to build the request it receives. When no replica can need them any more the
   * staged files are deleted right away. The record makes a restart delete the directory instead of
   * resuming a finished task.
   *
   * @param mustRetainStagedFiles the retention decision taken before the task was imported, so that
   *     the decision and the import cannot disagree
   */
  private void finishConsensusTask(
      final LoadTsFileConsensusNode node,
      final boolean mustRetainStagedFiles,
      final LoadTsFileConsensusOp terminalOp)
      throws IOException {
    final TsFileWriterManager writerManager = uuid2WriterManager.remove(node.getLoadId());
    if (Objects.isNull(writerManager)) {
      // The command was applied before, or this replica never staged anything for that task.
      return;
    }
    final File taskDir = writerManager.getTaskDir();
    if (!mustRetainStagedFiles) {
      writerManager.close(false);
      return;
    }
    // The tail of every progress file records the command the task finished with and its consensus
    // index, so that a crash before the directory is released still makes the restart delete it
    // instead of resuming the finished task. A task that was aborted is garbage as soon as that
    // command was applied, while a committed one is kept until every replica got past it and the
    // staged files are known to be complete.
    //
    // The command is only recorded on files it can describe: complete for a commit, continuous for
    // an abort; a directory that does not qualify waits for the idle timeout instead.
    writerManager.recordTerminal(
        terminalOp, node.getSearchIndex(), node.getTimePartition2ExpectedDataEnd());
    writerManager.close(true);
    retention.retain(node.getLoadId(), taskDir, terminalOp, node.getSearchIndex());
  }

  private void logLoadNodeToWAL(final LoadTsFileConsensusNode node) throws IOException {
    final AtomicReference<IOException> failure = new AtomicReference<>();
    dataRegion
        .getWALNode()
        .ifPresent(
            wal -> {
              final WALFlushListener listener = wal.log(TsFileProcessor.MEMTABLE_NOT_EXIST, node);
              if (listener.waitForResult() == WALFlushListener.Status.FAILURE) {
                final Exception cause = listener.getCause();
                failure.set(
                    cause instanceof IOException ? (IOException) cause : new IOException(cause));
              }
            });
    if (failure.get() != null) {
      throw failure.get();
    }
  }

  private void forceCloseWriterManager(String uuid) {
    final Lock writeLock = writersLock.writeLock();
    writeLock.lock();
    try {
      final TsFileWriterManager writerManager = uuid2WriterManager.remove(uuid);
      if (writerManager != null) {
        writerManager.close();
      }
    } finally {
      writeLock.unlock();
    }
  }

  /** Drops the writers of a task whose staged files are being deleted, see LoadTsFileCleaner. */
  void dropTask(final String loadId) {
    forceCloseWriterManager(loadId);
  }

  public static void cleanTsFile(final File tsFile) {
    try {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      Files.deleteIfExists(ModificationFile.getExclusiveMods(tsFile).toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).toPath());
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.DELETE_AFTER_LOADING_ERROR, tsFile, e);
    }
  }
}
