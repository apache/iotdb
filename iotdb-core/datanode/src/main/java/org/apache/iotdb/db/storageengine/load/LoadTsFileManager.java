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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

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

  private final DataRegion dataRegion;

  public LoadTsFileManager(final DataRegion dataRegion) {
    this.dataRegion = Objects.requireNonNull(dataRegion);
    this.retention = new LoadTaskRetention(dataRegion);
    recover();
  }

  public void stop() {
    new HashSet<>(uuid2WriterManager.keySet()).forEach(this::forceCloseWriterManager);
    uuid2PieceNodeAssembler.clear();
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
   * reached COMMIT or ABORT carries a terminal marker and is queued for release, while any other
   * directory belongs to a task that is still in progress and is resumed from its progress files.
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
        if (!uuidDir.isDirectory()) {
          continue;
        }
        final String uuid = uuidDir.getName();
        if (retention.recoverTerminalTask(uuid, uuidDir)) {
          continue;
        }
        try {
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
    return getOrCreateWriterManager(uuid).writePiece(tsFileDataList);
  }

  private TsFileWriterManager getOrCreateWriterManager(final String uuid) throws IOException {
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

  private File getDataRegionLoadDir(final File baseDir) {
    return LoadStagingDirs.regionLoadDir(
        baseDir, dataRegion.getDatabaseName(), dataRegion.getDataRegionIdString());
  }

  public List<LoadTsFileConsensusNode.PieceRef> writePiece(final LoadTsFileConsensusNode node)
      throws IOException, PageException {
    // Every chunk entry records the consensus index of the piece that brought it, so the progress
    // of
    // a staged file can be read back command by command.
    final List<LoadTsFileConsensusNode.PieceRef> refs =
        getOrCreateWriterManager(node.getLoadId())
            .writePiece(node.getTsFileDataList(), node.getSearchIndex());
    if (node.getPieceRefs().isEmpty()) {
      // The chunks of this piece have just been written into the staged file, which recorded where
      // the payload of each of them landed. The WAL entry keeps everything the leader received -
      // device, headers, statistics and layout - and only replaces the payload with those
      // references, so a replica can be given the very same piece later on
      node.setPieceRefs(refs);
      logLoadNodeToWAL(node);
      dataRegion.insertSeparatorToWAL(node);
    }
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
   * The staging directories of the loads this region is currently building (BEGIN/PIECE/PREPARE
   * stage). A snapshot must include their contents so a recovering replica can inherit the partial
   * physical state and the {@link LoadTsFileProgress} bitmaps of those loads.
   */
  public List<File> getActiveTaskDirs() {
    final List<File> result = new ArrayList<>();
    for (final TsFileWriterManager writerManager : uuid2WriterManager.values()) {
      result.add(writerManager.getTaskDir());
    }
    return result;
  }

  public boolean prepare(
      final LoadTsFileConsensusNode node,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      return false;
    }
    if (!uuid2WriterManager.get(node.getLoadId()).hasStagedData()) {
      throw new LoadFileException(
          String.format(
              StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PREPARE_WITHOUT_STAGED_DATA_FE8ADC37,
              node.getLoadId()));
    }
    LOGGER.info(
        StorageEngineMessages.LOG_PREPARING_LOAD_TSFILE_ARG_SEALING_STAGED_RESOURCES_1FDF1866,
        node.getLoadId());
    uuid2WriterManager
        .get(node.getLoadId())
        .prepare(node.isGeneratedByPipe(), timePartitionProgressIndexMap);
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  public boolean loadAll(
      final LoadTsFileConsensusNode node,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      return false;
    }
    LOGGER.info(
        StorageEngineMessages
            .LOG_COMMITTING_LOAD_TSFILE_ARG_LOADING_PREPARED_RESOURCES_INTO_DATAREGION_EA1D6335,
        node.getLoadId());
    // Whether the staged bytes are still needed has to be decided before they are imported: the WAL
    // entries of the pieces of this task only reference those bytes, and they are expanded on this
    // node for a replica that has not applied them yet. Importing them by moving the staged file
    // away would leave those entries unexpandable, so a copy is imported while the watermark of the
    // WAL has not got past this command yet.
    final boolean mustRetainStagedFiles = retention.mustRetain(node.getSearchIndex());
    uuid2WriterManager
        .get(node.getLoadId())
        .loadAll(node.isGeneratedByPipe(), !mustRetainStagedFiles);
    finishConsensusTask(node, mustRetainStagedFiles, LoadTsFileConsensusOp.COMMIT);
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  public boolean deleteAll(final LoadTsFileConsensusNode node) throws IOException {
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      return false;
    }
    finishConsensusTask(
        node, retention.mustRetain(node.getSearchIndex()), LoadTsFileConsensusOp.ABORT);
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  /**
   * Closes the staged resources of a consensus LOAD that reached COMMIT or ABORT.
   *
   * <p>A task that still has to be expanded for a replica keeps its staged bytes — for COMMIT they
   * are the copy the import was made from, for ABORT they are the files that were never imported —
   * and gets a terminal marker next to them. They are released only once the safe-deletion
   * watermark of the WAL reports that every replica got past the COMMIT or ABORT command, because
   * until then a replica that catches up by reading this node's WAL still reads those bytes back to
   * build the request it receives. When no replica can need them any more the staged files are
   * deleted right away. The marker makes a restart delete the directory instead of resuming a
   * finished task.
   *
   * @param mustRetainStagedFiles the retention decision taken before the task was imported, so that
   *     the decision and the import cannot disagree
   */
  private void finishConsensusTask(
      final LoadTsFileConsensusNode node,
      final boolean mustRetainStagedFiles,
      final LoadTsFileConsensusOp terminalOp) {
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
    // index, before the progress files are dropped, so that a crash in between still makes the
    // restart delete the directory instead of resuming the finished task. A task that was aborted
    // is garbage as soon as that command was applied, while a committed one is kept until every
    // replica got past it and the staged files are known to be complete.
    retention.markTerminal(taskDir, terminalOp, node.getSearchIndex());
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
    final TsFileWriterManager writerManager = uuid2WriterManager.remove(uuid);
    if (Objects.nonNull(writerManager)) {
      writerManager.close();
    }
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
