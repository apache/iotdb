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
package org.apache.iotdb.db.wal.recover.file;

import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.exception.WALRecoverException;
import org.apache.iotdb.db.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

  /** sequence file or not */
  private final boolean sequence;
  /** add recovered TsFile back to data region */
  private final Consumer<UnsealedTsFileRecoverPerformer> callbackAfterUnsealedTsFileRecovered;
  /** redo wal log to recover TsFile */
  private final TsFilePlanRedoer walRedoer;
  /** trace result of this recovery */
  private final WALRecoverListener recoverListener;

  public UnsealedTsFileRecoverPerformer(
      TsFileResource tsFileResource,
      boolean sequence,
      IDTable idTable,
      Consumer<UnsealedTsFileRecoverPerformer> callbackAfterUnsealedTsFileRecovered) {
    super(tsFileResource);
    this.sequence = sequence;
    this.callbackAfterUnsealedTsFileRecovered = callbackAfterUnsealedTsFileRecovered;
    this.walRedoer = new TsFilePlanRedoer(tsFileResource, sequence, idTable);
    this.recoverListener = new WALRecoverListener(tsFileResource.getTsFilePath());
  }

  /**
   * Make preparation for recovery, including load .resource file (reconstruct when necessary) and
   * truncate the file to remaining corrected data.
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
    Map<String, Map<String, List<Deletion>>> modificationsForResource =
        loadModificationsForResource();
    Map<String, List<ChunkMetadata>> deviceChunkMetaDataMap = writer.getDeviceChunkMetadataMap();
    for (Map.Entry<String, List<ChunkMetadata>> entry : deviceChunkMetaDataMap.entrySet()) {
      String deviceId = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();

      // measurement -> ChunkMetadataList
      Map<String, List<ChunkMetadata>> measurementToChunkMetadatas = new HashMap<>();
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        List<ChunkMetadata> list =
            measurementToChunkMetadatas.computeIfAbsent(
                chunkMetadata.getMeasurementUid(), n -> new ArrayList<>());
        list.add(chunkMetadata);
      }

      for (List<ChunkMetadata> metadataList : measurementToChunkMetadatas.values()) {
        TSDataType dataType = metadataList.get(metadataList.size() - 1).getDataType();
        for (ChunkMetadata chunkMetaData : chunkMetadataList) {
          if (!chunkMetaData.getDataType().equals(dataType)) {
            continue;
          }

          // calculate startTime and endTime according to chunkMetaData and modifications
          long startTime = chunkMetaData.getStartTime();
          long endTime = chunkMetaData.getEndTime();
          long chunkHeaderOffset = chunkMetaData.getOffsetOfChunkHeader();
          if (modificationsForResource.containsKey(deviceId)
              && modificationsForResource
                  .get(deviceId)
                  .containsKey(chunkMetaData.getMeasurementUid())) {
            // exist deletion for current measurement
            for (Deletion modification :
                modificationsForResource.get(deviceId).get(chunkMetaData.getMeasurementUid())) {
              long fileOffset = modification.getFileOffset();
              if (chunkHeaderOffset < fileOffset) {
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
            }
          }
          tsFileResource.updateStartTime(deviceId, startTime);
          tsFileResource.updateEndTime(deviceId, endTime);
        }
      }
    }
    tsFileResource.updatePlanIndexes(writer.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(writer.getMaxPlanIndex());
  }

  // load modifications for recovering tsFileResource
  private Map<String, Map<String, List<Deletion>>> loadModificationsForResource() {
    Map<String, Map<String, List<Deletion>>> modificationsForResource = new HashMap<>();
    ModificationFile modificationFile = tsFileResource.getModFile();
    if (modificationFile.exists()) {
      List<Modification> modifications = (List<Modification>) modificationFile.getModifications();
      for (Modification modification : modifications) {
        if (modification.getType().equals(Modification.Type.DELETION)) {
          String deviceId = modification.getPath().getDevice();
          String measurementId = modification.getPath().getMeasurement();
          Map<String, List<Deletion>> measurementModsMap =
              modificationsForResource.computeIfAbsent(deviceId, n -> new HashMap<>());
          List<Deletion> list =
              measurementModsMap.computeIfAbsent(measurementId, n -> new ArrayList<>());
          list.add((Deletion) modification);
        }
      }
    }
    return modificationsForResource;
  }

  /** Redo log */
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
          IMemTable memTable = (IMemTable) walEntry.getValue();
          if (!memTable.isSignalMemTable()) {
            walRedoer.resetRecoveryMemTable(memTable);
          }
          break;
        case INSERT_ROW_NODE:
        case INSERT_TABLET_NODE:
          walRedoer.redoInsert((InsertNode) walEntry.getValue());
          break;
        case DELETE_DATA_NODE:
          walRedoer.redoDelete((DeleteDataNode) walEntry.getValue());
          break;
        default:
          throw new RuntimeException("Unsupported type " + walEntry.getType());
      }
    } catch (Exception e) {
      logger.warn("meet error when redo wal of {}", tsFileResource.getTsFile(), e);
    }
  }

  /** Run last procedures to end this recovery */
  public void endRecovery() throws WALRecoverException {
    // skip update info when this TsFile is not crashed
    if (hasCrashed()) {
      IMemTable recoveryMemTable = walRedoer.getRecoveryMemTable();
      // update time map
      Map<IDeviceID, IWritableMemChunkGroup> memTableMap = recoveryMemTable.getMemTableMap();
      for (Map.Entry<IDeviceID, IWritableMemChunkGroup> deviceEntry : memTableMap.entrySet()) {
        String deviceId = deviceEntry.getKey().toStringID();
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
          String dataRegionId =
              tsFileResource.getTsFile().getParentFile().getParentFile().getName();
          String databaseName =
              tsFileResource.getTsFile().getParentFile().getParentFile().getParentFile().getName();
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

        // if we put following codes in if clause above, this file can be continued writing into it
        // currently, we close this file anyway
        writer.endFile();
        tsFileResource.serialize();
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
