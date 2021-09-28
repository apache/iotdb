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

package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.exception.NotCompatibleTsFileException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;

/**
 * TsFileRecoverPerformer recovers a SeqTsFile to correct status, redoes the WALs since last crash
 * and removes the redone logs.
 */
public class TsFileRecoverPerformer {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRecoverPerformer.class);

  private final String filePath;
  private final String logNodePrefix;
  private final TsFileResource tsFileResource;
  private final boolean sequence;

  /** @param isLastFile whether this TsFile is the last file of its partition */
  public TsFileRecoverPerformer(
      String logNodePrefix,
      TsFileResource currentTsFileResource,
      boolean sequence,
      boolean isLastFile) {
    this.filePath = currentTsFileResource.getTsFilePath();
    this.logNodePrefix = logNodePrefix;
    this.tsFileResource = currentTsFileResource;
    this.sequence = sequence;
  }

  /**
   * 1. recover the TsFile by RestorableTsFileIOWriter and truncate the file to remaining corrected
   * data 2. redo the WALs to recover unpersisted data 3. flush and close the file 4. clean WALs
   *
   * @return a RestorableTsFileIOWriter and a list of RestorableTsFileIOWriter of vmfiles, if the
   *     file and the vmfiles are not closed before crash, so these writers can be used to continue
   *     writing
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public RestorableTsFileIOWriter recover(
      boolean needRedoWal, Supplier<ByteBuffer[]> supplier, Consumer<ByteBuffer[]> consumer)
      throws StorageGroupProcessorException {

    File file = FSFactoryProducer.getFSFactory().getFile(filePath);
    if (!file.exists()) {
      logger.error("TsFile {} is missing, will skip its recovery.", filePath);
      return null;
    }

    // remove corrupted part of the TsFile
    RestorableTsFileIOWriter restorableTsFileIOWriter;
    try {
      restorableTsFileIOWriter = new RestorableTsFileIOWriter(file);
    } catch (NotCompatibleTsFileException e) {
      boolean result = file.delete();
      logger.warn("TsFile {} is incompatible. Delete it successfully {}", filePath, result);
      throw new StorageGroupProcessorException(e);
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    // judge whether tsfile is complete
    if (!restorableTsFileIOWriter.hasCrashed()) {
      try {
        recoverResource();
        return restorableTsFileIOWriter;
      } catch (IOException e) {
        throw new StorageGroupProcessorException(
            "recover the resource file failed: " + filePath + RESOURCE_SUFFIX + e);
      }
    }

    // tsfile has crashed
    // due to failure, the last ChunkGroup may contain the same data as the WALs, so the time
    // map must be updated first to avoid duplicated insertion
    recoverResourceFromWriter(restorableTsFileIOWriter);

    // redo logs
    if (needRedoWal) {
      redoLogs(restorableTsFileIOWriter, supplier);

      // clean logs
      try {
        MultiFileLogNodeManager.getInstance()
            .deleteNode(
                logNodePrefix + SystemFileFactory.INSTANCE.getFile(filePath).getName(), consumer);
      } catch (IOException e) {
        throw new StorageGroupProcessorException(e);
      }
    }
    return restorableTsFileIOWriter;
  }

  private void recoverResource() throws IOException {
    if (tsFileResource.resourceFileExists()) {
      // .resource file exists, deserialize it
      recoverResourceFromFile();
    } else {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader =
          new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath())) {
        FileLoaderUtils.updateTsFileResource(reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    }
  }

  private void recoverResourceFromFile() throws IOException {
    try {
      tsFileResource.deserialize();
    } catch (IOException e) {
      logger.warn(
          "Cannot deserialize TsFileResource {}, construct it using " + "TsFileSequenceReader",
          tsFileResource.getTsFile(),
          e);
      recoverResourceFromReader();
    }
  }

  private void recoverResourceFromReader() throws IOException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath(), true)) {
      for (Entry<String, List<TimeseriesMetadata>> entry :
          reader.getAllTimeseriesMetadata().entrySet()) {
        for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
          tsFileResource.updateStartTime(
              entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
          tsFileResource.updateEndTime(
              entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
        }
      }
    }
    // write .resource file
    tsFileResource.serialize();
  }

  private void recoverResourceFromWriter(RestorableTsFileIOWriter restorableTsFileIOWriter) {
    Map<String, Map<String, List<Deletion>>> modificationsForResource =
        loadModificationsForResource();
    Map<String, List<ChunkMetadata>> deviceChunkMetaDataMap =
        restorableTsFileIOWriter.getDeviceChunkMetadataMap();
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
    tsFileResource.updatePlanIndexes(restorableTsFileIOWriter.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(restorableTsFileIOWriter.getMaxPlanIndex());
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

  private void redoLogs(
      RestorableTsFileIOWriter restorableTsFileIOWriter, Supplier<ByteBuffer[]> supplier)
      throws StorageGroupProcessorException {
    IMemTable recoverMemTable = new PrimitiveMemTable();
    LogReplayer logReplayer =
        new LogReplayer(
            logNodePrefix,
            filePath,
            tsFileResource.getModFile(),
            tsFileResource,
            recoverMemTable,
            sequence);
    logReplayer.replayLogs(supplier);
    try {
      if (!recoverMemTable.isEmpty()) {
        // flush logs
        MemTableFlushTask tableFlushTask =
            new MemTableFlushTask(
                recoverMemTable,
                restorableTsFileIOWriter,
                tsFileResource.getTsFile().getParentFile().getParentFile().getName());
        tableFlushTask.syncFlushMemTable();
        tsFileResource.updatePlanIndexes(recoverMemTable.getMinPlanIndex());
        tsFileResource.updatePlanIndexes(recoverMemTable.getMaxPlanIndex());
      }

      restorableTsFileIOWriter.endFile();
      tsFileResource.serialize();

      // otherwise this file is not closed before crush, do nothing so we can continue writing
      // into it
    } catch (IOException | ExecutionException e) {
      throw new StorageGroupProcessorException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageGroupProcessorException(e);
    }
  }
}
