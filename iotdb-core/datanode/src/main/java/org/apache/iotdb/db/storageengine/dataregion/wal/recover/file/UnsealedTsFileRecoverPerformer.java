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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover.file;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndexCacheRecorder;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

/**
 * This class is used to help recover all unsealed TsFiles at zero level. There are 3 main
 * procedures: start recovery, redo logs, and end recovery, you must call them in order. Notice:
 * This class doesn't guarantee concurrency safety.
 */
public class UnsealedTsFileRecoverPerformer extends AbstractTsFileRecoverPerformer {

  private static final Logger logger =
      LoggerFactory.getLogger(UnsealedTsFileRecoverPerformer.class);

  // sequence file or not
  private final boolean sequence;
  // add recovered TsFile back to data region
  private final Consumer<UnsealedTsFileRecoverPerformer> callbackAfterUnsealedTsFileRecovered;
  // redo wal log to recover TsFile
  private final TsFilePlanRedoer walRedoer;
  // trace result of this recovery
  private final WALRecoverListener recoverListener;
  private final String databaseName;
  private final String dataRegionId;

  public UnsealedTsFileRecoverPerformer(
      TsFileResource tsFileResource,
      boolean sequence,
      Consumer<UnsealedTsFileRecoverPerformer> callbackAfterUnsealedTsFileRecovered) {
    super(tsFileResource);
    this.databaseName = tsFileResource.getDatabaseName();
    this.dataRegionId = tsFileResource.getDataRegionId();
    this.sequence = sequence;
    this.callbackAfterUnsealedTsFileRecovered = callbackAfterUnsealedTsFileRecovered;
    this.walRedoer = new TsFilePlanRedoer(tsFileResource);
    this.recoverListener = new WALRecoverListener(tsFileResource.getTsFilePath());
  }

  /**
   * Make preparation for recovery, including load .resource file (reconstruct when necessary) and
   * truncate the file to remaining corrected data.
   *
   * @throws DataRegionException when failing to recover.
   * @throws IOException when failing to recover.
   */
  public void startRecovery() throws DataRegionException, IOException {
    super.recoverWithWriter();

    if (hasCrashed()) {
      // tsfile has crashed due to failure,
      // the last ChunkGroup may contain the same data as the WALs,
      // so the time map must be updated first to avoid duplicated insertion
      constructResourceFromTsFile();
    }
  }

  private void constructResourceFromTsFile() {
    Collection<ModEntry> modificationsForResource = tsFileResource.getAllModEntries();
    Map<IDeviceID, List<ChunkMetadata>> deviceChunkMetaDataMap = writer.getDeviceChunkMetadataMap();
    for (Map.Entry<IDeviceID, List<ChunkMetadata>> entry : deviceChunkMetaDataMap.entrySet()) {
      IDeviceID deviceId = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();

      // measurement -> ChunkMetadataList
      Map<String, List<ChunkMetadata>> measurementToChunkMetadatas = new HashMap<>();
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        List<ChunkMetadata> list =
            measurementToChunkMetadatas.computeIfAbsent(
                chunkMetadata.getMeasurementUid(), n -> new ArrayList<>());
        list.add(chunkMetadata);
      }
      for (ChunkMetadata chunkMetaData : chunkMetadataList) {
        // calculate startTime and endTime according to chunkMetaData and modifications
        long startTime = chunkMetaData.getStartTime();
        long endTime = chunkMetaData.getEndTime();
        // exist deletion for current measurement
        for (ModEntry modification : modificationsForResource) {
          if (!(modification.affects(deviceId, startTime, endTime)
              && modification.affects(chunkMetaData.getMeasurementUid()))) {
            continue;
          }
          // deletion is valid for current chunk
          long modsStartTime = modification.getStartTime();
          long modsEndTime = modification.getEndTime();
          if (startTime >= modsStartTime && endTime <= modsEndTime) {
            startTime = Long.MAX_VALUE;
            endTime = Long.MIN_VALUE;
          } else if (startTime >= modsStartTime && startTime <= modsEndTime) {
            startTime = modsEndTime + 1;
          } else if (endTime >= modsStartTime && endTime <= modsEndTime) {
            endTime = modsStartTime - 1;
          }
        }

        tsFileResource.updateStartTime(deviceId, startTime);
        tsFileResource.updateEndTime(deviceId, endTime);
      }
    }
    tsFileResource.updatePlanIndexes(writer.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(writer.getMaxPlanIndex());
  }

  /** Redo log. */
  public void redoLog(WALEntry walEntry) {
    // skip redo wal log when this TsFile is not crashed
    if (!hasCrashed()) {
      logger.info(
          "This TsFile {} isn't crashed, no need to redo wal log.", tsFileResource.getTsFilePath());
      return;
    }
    try {
      switch (walEntry.getType()) {
        case MEMORY_TABLE_SNAPSHOT:
        case OLD_MEMORY_TABLE_SNAPSHOT:
          IMemTable memTable = (IMemTable) walEntry.getValue();
          if (!memTable.isSignalMemTable()) {
            if (tsFileResource != null) {
              // delete data already flushed in the MemTable to avoid duplicates
              for (IDeviceID device : tsFileResource.getDevices()) {
                if (device.isTableModel()) {
                  memTable.delete(
                      new TableDeletionEntry(
                          new DeletionPredicate(device.getTableName(), new FullExactMatch(device)),
                          new TimeRange(
                              tsFileResource.getStartTime(device),
                              tsFileResource.getEndTime(device))));
                } else {
                  memTable.delete(
                      new TreeDeletionEntry(
                          new MeasurementPath(device, "*"),
                          tsFileResource.getStartTime(device),
                          tsFileResource.getEndTime(device)));
                }
              }
            }
            for (IDeviceID deviceID : memTable.getMemTableMap().keySet()) {
              if (deviceID.isTableModel()) {
                registerToTsFile(deviceID.getTableName());
              }
            }
            walRedoer.resetRecoveryMemTable(memTable);
          }
          // update memtable's database and dataRegionId
          memTable.setDatabaseAndDataRegionId(databaseName, dataRegionId);
          break;
        case INSERT_ROW_NODE:
        case INSERT_TABLET_NODE:
          registerToTsFile(((InsertNode) walEntry.getValue()).getTableName());
          walRedoer.redoInsert((InsertNode) walEntry.getValue());
          break;
        case INSERT_ROWS_NODE:
          registerToTsFile(((InsertRowsNode) walEntry.getValue()).getTableName());
          walRedoer.redoInsertRows((InsertRowsNode) walEntry.getValue());
          break;
        case DELETE_DATA_NODE:
          walRedoer.redoDelete((DeleteDataNode) walEntry.getValue());
          break;
        case RELATIONAL_DELETE_DATA_NODE:
          walRedoer.redoDelete(((RelationalDeleteDataNode) walEntry.getValue()));
          break;
        case CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR_NODE:
          // The CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR_NODE doesn't need redo
          break;
        default:
          throw new RuntimeException("Unsupported type " + walEntry.getType());
      }
    } catch (Exception e) {
      if (tsFileResource != null) {
        logger.warn("meet error when redo wal of {}", tsFileResource.getTsFile(), e);
      }
    }
  }

  private void registerToTsFile(String tableName) {
    if (tableName != null) {
      writer
          .getSchema()
          .getTableSchemaMap()
          .computeIfAbsent(
              tableName,
              t ->
                  TableSchema.of(DataNodeTableCache.getInstance().getTable(databaseName, t))
                      .toTsFileTableSchemaNoAttribute());
    }
  }

  /**
   * Run last procedures to end this recovery.
   *
   * @throws WALRecoverException when failing to flush the recovered memTable.
   */
  public void endRecovery() throws WALRecoverException {
    // skip update info when this TsFile is not crashed
    if (hasCrashed()) {
      IMemTable recoveryMemTable = walRedoer.getRecoveryMemTable();
      // update time map
      Map<IDeviceID, IWritableMemChunkGroup> memTableMap = recoveryMemTable.getMemTableMap();
      for (Map.Entry<IDeviceID, IWritableMemChunkGroup> deviceEntry : memTableMap.entrySet()) {
        IDeviceID deviceId = deviceEntry.getKey();
        for (Map.Entry<String, IWritableMemChunk> measurementEntry :
            deviceEntry.getValue().getMemChunkMap().entrySet()) {
          IWritableMemChunk memChunk = measurementEntry.getValue();
          tsFileResource.updateStartTime(deviceId, memChunk.getFirstPoint());
          tsFileResource.updateEndTime(deviceId, memChunk.getLastPoint());
        }
      }
      // flush memTable
      try {
        if (!recoveryMemTable.isEmpty() && recoveryMemTable.getSeriesNumber() != 0) {
          MemTableFlushTask tableFlushTask =
              new MemTableFlushTask(
                  recoveryMemTable,
                  writer,
                  databaseName + FILE_NAME_SEPARATOR + dataRegionId,
                  dataRegionId);
          tableFlushTask.syncFlushMemTable();
          tsFileResource.updatePlanIndexes(recoveryMemTable.getMinPlanIndex());
          tsFileResource.updatePlanIndexes(recoveryMemTable.getMaxPlanIndex());
        }

        // set recover progress index for pipe
        PipeDataNodeAgent.runtime().assignProgressIndexForTsFileRecovery(tsFileResource);

        try {
          long memTableSize = recoveryMemTable.memSize();
          double compressionRatio = ((double) memTableSize) / writer.getPos();
          logger.info(
              "The compression ratio of tsfile {} is {}, totalMemTableSize: {}, the file size: {}",
              writer.getFile().getAbsolutePath(),
              String.format("%.2f", compressionRatio),
              memTableSize,
              writer.getPos());
          CompressionRatio.getInstance().updateRatio(memTableSize, writer.getPos());
        } catch (IOException e) {
          logger.error(
              "{}: {} update compression ratio failed",
              databaseName,
              tsFileResource.getTsFile().getName(),
              e);
        }

        // if we put following codes in the 'if' clause above, this file can be continued writing
        // into it
        // currently, we close this file anyway
        writer.endFile();
        tsFileResource.serialize();
        FileTimeIndexCacheRecorder.getInstance().logFileTimeIndex(tsFileResource);
      } catch (IOException | ExecutionException e) {
        throw new WALRecoverException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new WALRecoverException(e);
      }
    }

    // return this TsFile back to vsg processor
    callbackAfterUnsealedTsFileRecovered.accept(this);
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  public RestorableTsFileIOWriter getWriter() {
    return writer;
  }

  public boolean isSequence() {
    return sequence;
  }

  public WALRecoverListener getRecoverListener() {
    return recoverListener;
  }

  public String getTsFileAbsolutePath() {
    return tsFileResource.getTsFile().getAbsolutePath();
  }
}
