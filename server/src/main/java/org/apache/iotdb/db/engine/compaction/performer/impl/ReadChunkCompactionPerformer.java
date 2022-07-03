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
package org.apache.iotdb.db.engine.compaction.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.inner.utils.AlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.inner.utils.SingleSeriesCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ReadChunkCompactionPerformer implements ISeqCompactionPerformer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private TsFileResource targetResource;
  private List<TsFileResource> seqFiles;
  private CompactionTaskSummary summary;

  public ReadChunkCompactionPerformer(List<TsFileResource> sourceFiles, TsFileResource targetFile) {
    this.seqFiles = sourceFiles;
    this.targetResource = targetFile;
  }

  public ReadChunkCompactionPerformer(List<TsFileResource> sourceFiles) {
    this.seqFiles = sourceFiles;
  }

  public ReadChunkCompactionPerformer() {}

  @Override
  public void perform()
      throws IOException, MetadataException, InterruptedException, StorageEngineException {
    try (MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(seqFiles);
        TsFileIOWriter writer = new TsFileIOWriter(targetResource.getTsFile())) {
      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;

        writer.startChunkGroup(device);
        if (aligned) {
          compactAlignedSeries(device, targetResource, writer, deviceIterator);
        } else {
          compactNotAlignedSeries(device, targetResource, writer, deviceIterator);
        }
        writer.endChunkGroup();
      }

      for (TsFileResource tsFileResource : seqFiles) {
        targetResource.updatePlanIndexes(tsFileResource);
      }
      writer.endFile();
      targetResource.close();
    }
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    if (targetFiles.size() != 1) {
      throw new RuntimeException(
          String.format(
              "Current performer only supports for one target file while getting %d target files",
              targetFiles.size()));
    }
    this.targetResource = targetFiles.get(0);
  }

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    this.summary = summary;
  }

  private void compactAlignedSeries(
      String device,
      TsFileResource targetResource,
      TsFileIOWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, InterruptedException {
    checkThreadInterrupted();
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList =
        deviceIterator.getReaderAndChunkMetadataForCurrentAlignedSeries();
    AlignedSeriesCompactionExecutor compactionExecutor =
        new AlignedSeriesCompactionExecutor(
            device, targetResource, readerAndChunkMetadataList, writer);
    compactionExecutor.execute();
  }

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.interrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", targetResource.toString()));
    }
  }

  private void compactNotAlignedSeries(
      String device,
      TsFileResource targetResource,
      TsFileIOWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, MetadataException, InterruptedException {
    MultiTsFileDeviceIterator.MeasurementIterator seriesIterator =
        deviceIterator.iterateNotAlignedSeries(device, true);
    while (seriesIterator.hasNextSeries()) {
      checkThreadInterrupted();
      // TODO: we can provide a configuration item to enable concurrent between each series
      PartialPath p = new PartialPath(device, seriesIterator.nextSeries());
      IMeasurementSchema measurementSchema;
      // TODO: seriesIterator needs to be refactor.
      // This statement must be called before next hasNextSeries() called, or it may be trapped in a
      // dead-loop.
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList =
          seriesIterator.getMetadataListForCurrentSeries();
      SingleSeriesCompactionExecutor compactionExecutorOfCurrentTimeSeries =
          new SingleSeriesCompactionExecutor(p, readerAndChunkMetadataList, writer, targetResource);
      compactionExecutorOfCurrentTimeSeries.execute();
    }
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }
}
