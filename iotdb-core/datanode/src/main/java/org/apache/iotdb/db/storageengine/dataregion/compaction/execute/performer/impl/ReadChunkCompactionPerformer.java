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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchemaCollector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.BatchedReadChunkAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.SingleSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ReadChunkCompactionPerformer implements ISeqCompactionPerformer {
  private List<TsFileResource> seqFiles;
  private List<TsFileResource> targetResources;
  private CompactionTaskSummary summary;
  private CompactionTsFileWriter currentWriter;
  private long endedFileSize = 0;
  private int currentTargetFileIndex = 0;
  // memory budget for file writer is 5% of per compaction task memory budget
  private final long memoryBudgetForFileWriter =
      (long)
          ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
              / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
              * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
  private Schema schema = null;

  public ReadChunkCompactionPerformer(List<TsFileResource> sourceFiles, TsFileResource targetFile) {
    this(sourceFiles, Collections.singletonList(targetFile));
  }

  public ReadChunkCompactionPerformer(
      List<TsFileResource> sourceFiles, List<TsFileResource> targetFiles) {
    setSourceFiles(sourceFiles);
    setTargetFiles(targetFiles);
  }

  public ReadChunkCompactionPerformer(List<TsFileResource> sourceFiles) {
    setSourceFiles(sourceFiles);
  }

  public ReadChunkCompactionPerformer() {}

  @Override
  public void perform()
      throws IOException,
          MetadataException,
          InterruptedException,
          StorageEngineException,
          PageException {
    try (MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(seqFiles)) {
      schema =
          CompactionTableSchemaCollector.collectSchema(seqFiles, deviceIterator.getReaderMap());
      while (deviceIterator.hasNextDevice()) {
        currentWriter = getAvailableCompactionWriter();
        Pair<IDeviceID, Boolean> deviceInfo = deviceIterator.nextDevice();
        IDeviceID device = deviceInfo.left;
        boolean aligned = deviceInfo.right;

        if (aligned) {
          compactAlignedSeries(
              device, targetResources.get(currentTargetFileIndex), currentWriter, deviceIterator);
        } else {
          compactNotAlignedSeries(
              device, targetResources.get(currentTargetFileIndex), currentWriter, deviceIterator);
        }
        // update temporal file metrics
        summary.setTemporaryFileSize(endedFileSize + currentWriter.getPos());
      }

      for (TsFileResource tsFileResource : seqFiles) {
        for (TsFileResource targetResource : targetResources) {
          targetResource.updatePlanIndexes(tsFileResource);
        }
      }
    } finally {
      for (int i = currentTargetFileIndex + 1; i < targetResources.size(); i++) {
        targetResources.get(i).forceMarkDeleted();
      }
      if (currentWriter == null) {
        targetResources.get(currentTargetFileIndex).forceMarkDeleted();
      } else {
        currentWriter.endFile();
        if (currentWriter.isEmptyTargetFile()) {
          targetResources.get(currentTargetFileIndex).forceMarkDeleted();
        }
      }
    }
  }

  private CompactionTsFileWriter getAvailableCompactionWriter() throws IOException {
    if (currentWriter == null) {
      useNewWriter();
      return currentWriter;
    }
    boolean shouldSwitchToNextWriter =
        currentWriter.getPos()
                >= IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize()
            && (currentTargetFileIndex != targetResources.size() - 1);
    if (shouldSwitchToNextWriter) {
      rollCompactionFileWriter();
    }
    return currentWriter;
  }

  private void rollCompactionFileWriter() throws IOException {
    currentWriter.endFile();
    endedFileSize += currentWriter.getFile().length();
    if (currentWriter.isEmptyTargetFile()) {
      targetResources.get(currentTargetFileIndex).forceMarkDeleted();
    }
    currentWriter = null;

    currentTargetFileIndex++;
    useNewWriter();
  }

  private void useNewWriter() throws IOException {
    currentWriter =
        new CompactionTsFileWriter(
            targetResources.get(currentTargetFileIndex).getTsFile(),
            memoryBudgetForFileWriter,
            CompactionType.INNER_SEQ_COMPACTION);
    currentWriter.setSchema(CompactionTableSchemaCollector.copySchema(schema));
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    this.targetResources = targetFiles;
  }

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    this.summary = summary;
  }

  private void compactAlignedSeries(
      IDeviceID device,
      TsFileResource targetResource,
      CompactionTsFileWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, InterruptedException, IllegalPathException, PageException {
    checkThreadInterrupted();
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList =
        deviceIterator.getReaderAndChunkMetadataForCurrentAlignedSeries();
    if (!checkAlignedSeriesExists(readerAndChunkMetadataList)) {
      return;
    }
    writer.startChunkGroup(device);
    BatchedReadChunkAlignedSeriesCompactionExecutor compactionExecutor =
        new BatchedReadChunkAlignedSeriesCompactionExecutor(
            device, targetResource, readerAndChunkMetadataList, writer, summary);
    compactionExecutor.execute();
    for (ChunkMetadata chunkMetadata : writer.getChunkMetadataListOfCurrentDeviceInMemory()) {
      if (chunkMetadata.getMeasurementUid().isEmpty()) {
        targetResource.updateStartTime(device, chunkMetadata.getStartTime());
        targetResource.updateEndTime(device, chunkMetadata.getEndTime());
      }
    }
    writer.checkMetadataSizeAndMayFlush();
    writer.endChunkGroup();
  }

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.interrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format("[Compaction] compaction for target files %s abort", targetResources));
    }
  }

  private boolean checkAlignedSeriesExists(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
          readerAndChunkMetadataList) {
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair :
        readerAndChunkMetadataList) {
      if (!readerListPair.right.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("squid:S1135")
  private void compactNotAlignedSeries(
      IDeviceID device,
      TsFileResource targetResource,
      CompactionTsFileWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, MetadataException, InterruptedException {
    writer.startChunkGroup(device);
    MultiTsFileDeviceIterator.MultiTsFileNonAlignedMeasurementMetadataListIterator seriesIterator =
        deviceIterator.iterateNotAlignedSeriesAndChunkMetadataListOfCurrentDevice();
    while (seriesIterator.hasNextSeries()) {
      checkThreadInterrupted();
      String measurement = seriesIterator.nextSeries();
      // TODO: we can provide a configuration item to enable concurrent between each series
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList =
          seriesIterator.getMetadataListForCurrentSeries();
      // remove the chunk metadata whose data type not match the data type of last chunk
      readerAndChunkMetadataList =
          filterDataTypeNotMatchedChunkMetadata(readerAndChunkMetadataList);
      SingleSeriesCompactionExecutor compactionExecutorOfCurrentTimeSeries =
          new SingleSeriesCompactionExecutor(
              device, measurement, readerAndChunkMetadataList, writer, targetResource, summary);
      compactionExecutorOfCurrentTimeSeries.execute();
    }
    writer.endChunkGroup();
  }

  private LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
      filterDataTypeNotMatchedChunkMetadata(
          LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList) {
    if (readerAndChunkMetadataList.isEmpty()) {
      return readerAndChunkMetadataList;
    }
    LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> result = new LinkedList<>();
    // find correct data type
    TSDataType correctDataType = null;
    for (int i = readerAndChunkMetadataList.size() - 1; i >= 0 && correctDataType == null; i--) {
      List<ChunkMetadata> chunkMetadataList = readerAndChunkMetadataList.get(i).getRight();
      if (chunkMetadataList == null || chunkMetadataList.isEmpty()) {
        continue;
      }
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        if (chunkMetadata == null) {
          continue;
        }
        correctDataType = chunkMetadata.getDataType();
        break;
      }
    }
    if (correctDataType == null) {
      return readerAndChunkMetadataList;
    }
    // check data type consistent and skip compact files with wrong data type
    for (Pair<TsFileSequenceReader, List<ChunkMetadata>> tsFileSequenceReaderListPair :
        readerAndChunkMetadataList) {
      boolean dataTypeConsistent = true;
      for (ChunkMetadata chunkMetadata : tsFileSequenceReaderListPair.getRight()) {
        if (chunkMetadata != null && chunkMetadata.getDataType() != correctDataType) {
          dataTypeConsistent = false;
          break;
        }
      }
      if (!dataTypeConsistent) {
        continue;
      }
      result.add(tsFileSequenceReaderListPair);
    }
    return result;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }
}
