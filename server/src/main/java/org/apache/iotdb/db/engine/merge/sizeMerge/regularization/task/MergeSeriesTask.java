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

package org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task;

import static org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.NaivePathSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.recover.RegularizationMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      MergeSeriesTask.class);

  private RegularizationMergeLogger mergeLogger;

  private String taskName;
  private MergeResource resource;
  private MergeContext mergeContext;
  private long timeBlock;
  private List<Path> unmergedSeries;
  private int concurrentMergeSeriesNum;

  private List<Pair<RestorableTsFileIOWriter, TsFileResource>> newTsFilePairs;

  MergeSeriesTask(MergeContext context, String taskName, RegularizationMergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries, int concurrentMergeSeriesNum) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.timeBlock = IoTDBDescriptor.getInstance().getConfig().getMergeFileTimeBlock();
    this.newTsFilePairs = new ArrayList<>();
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
  }

  List<TsFileResource> mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge series", taskName);
    }
    long startTime = System.currentTimeMillis();

    int writerIdx = 0;
    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = getFileWriter(writerIdx);
    RestorableTsFileIOWriter nowFileWriter = newTsFilePair.left;
    TsFileResource nowResource = newTsFilePair.right;

    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      Iterator<List<Path>> pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        List<Path> paths = pathSelector.next();
        String deviceId = paths.get(0).getDevice();
        nowFileWriter.startChunkGroup(paths.get(0).getDevice());
        Long nowResourceStartTime = null;
        Long nowResourceEndTime = null;
        for (TsFileResource currTsFile : resource.getSeqFiles()) {
          Long currDeviceMinTime = currTsFile.getStartTimeMap().get(deviceId);
          Long currDeviceMaxTime = currTsFile.getEndTimeMap().get(deviceId);
          if (currDeviceMinTime == null || currDeviceMaxTime == null) {
            break;
          }
          if (nowResourceStartTime == null || currDeviceMinTime < nowResourceStartTime) {
            nowResourceStartTime = currDeviceMinTime;
          }
          if (nowResourceEndTime == null || currDeviceMaxTime > nowResourceEndTime) {
            nowResourceEndTime = currDeviceMaxTime;
          }
          mergePaths(currTsFile, paths, nowFileWriter);
          if (nowResourceEndTime - nowResourceStartTime > timeBlock) {
            nowResource.getStartTimeMap().put(deviceId, nowResourceStartTime);
            nowResource.getEndTimeMap().put(deviceId, nowResourceEndTime);
            resource.flushChunks(nowFileWriter);
            nowFileWriter.endChunkGroup();

            writerIdx++;
            newTsFilePair = getFileWriter(writerIdx);
            nowFileWriter = newTsFilePair.left;
            nowResource = newTsFilePair.right;
          }
        }
        resource.flushChunks(nowFileWriter);
        nowFileWriter.endChunkGroup();
      }
    }
    List<TsFileResource> newResources = new ArrayList<>();
    for (Pair<RestorableTsFileIOWriter, TsFileResource> tsFilePair : newTsFilePairs) {
      newResources.add(tsFilePair.right);
      tsFilePair.left.endFile();
    }
    mergeLogger.logAllTsEnd();

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }

    return newResources;
  }

  private void mergePaths(TsFileResource currTsFile, List<Path> paths,
      RestorableTsFileIOWriter nowFileWriter) throws IOException {
    TsFileSequenceReader seqReader = resource.getFileReader(currTsFile);
    for (Path path : paths) {
      MeasurementSchema measurementSchema = resource.getChunkWriter(path).getMeasurementSchema();
      nowFileWriter.addSchema(path, measurementSchema);
      List<ChunkMetadata> chunkMetadataList = seqReader.getChunkMetadataList(path);
      writeMergedChunkGroup(chunkMetadataList, seqReader, nowFileWriter);
    }
  }

  private void writeMergedChunkGroup(List<ChunkMetadata> chunkMetadataList,
      TsFileSequenceReader reader, RestorableTsFileIOWriter nowFileWriter)
      throws IOException {
    // start merging a device
    long maxVersion = 0;
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      nowFileWriter.writeChunk(chunk, chunkMetaData);
      maxVersion =
          chunkMetaData.getVersion() > maxVersion ? chunkMetaData.getVersion() : maxVersion;
      mergeContext.incTotalPointWritten(chunkMetaData.getNumOfPoints());
    }
    nowFileWriter.addVersionPair(new Pair<>(nowFileWriter.getPos(), maxVersion + 1));
  }

  private Pair<RestorableTsFileIOWriter, TsFileResource> getFileWriter(int idx) throws IOException {
    // use the minimum version as the version of the new file
    long currFileVersion =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[1]);
    long prevMergeNum =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[2]);
    File parent = resource.getSeqFiles().get(0).getFile().getParentFile();
    File newFile = FSFactoryProducer.getFSFactory().getFile(parent,
        System.currentTimeMillis() + TSFILE_SEPARATOR + currFileVersion + TSFILE_SEPARATOR + (
            prevMergeNum + 1) + TSFILE_SUFFIX + MERGE_SUFFIX);
    if (idx < newTsFilePairs.size()) {
      return newTsFilePairs.get(idx);
    } else {
      Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = new Pair<>(
          new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
      mergeLogger.logNewFile(newTsFilePair.right);
      newTsFilePairs.add(newTsFilePair);
      return newTsFilePair;
    }
  }
}
