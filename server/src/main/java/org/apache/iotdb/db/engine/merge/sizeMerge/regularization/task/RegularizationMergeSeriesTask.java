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
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseMergeSeriesTask;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegularizationMergeSeriesTask extends BaseMergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      RegularizationMergeSeriesTask.class);

  // for Regularization merge, when merged tsfile reaches threshold, get a fileName order by its timestamp
  private PriorityQueue<File> mergeFileNameHeap = new PriorityQueue<>(((o1, o2) -> {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);

    //TODO: for test only
    try {
      Long.parseLong(items1[0]);
    } catch (NumberFormatException e) {
      return items1[0].compareTo(items2[0]);
    }

    return Long.compare(Long.parseLong(items1[0]), Long.parseLong(items2[0]));
  }));

  public RegularizationMergeSeriesTask(MergeContext context, String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    super(context, taskName, mergeLogger, mergeResource, unmergedSeries);
  }

  public List<TsFileResource> mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      long totalChunkPoint = 0;
      long chunkNum = 0;
      for (TsFileResource seqFile : resource.getSeqFiles()) {
        List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(seqFile);
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          chunkNum++;
          totalChunkPoint += chunkMetadata.getNumOfPoints();
        }
      }
      logger.info("merge before seqFile chunk large = {}", totalChunkPoint * 1.0 / chunkNum);
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge series", taskName);
    }
    long startTime = System.currentTimeMillis();

    this.constructMergeFileNameHeap();
    List<TsFileResource> newResources = new ArrayList<>();

    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = createNewFileWriter(
        mergeFileNameHeap, MERGE_SUFFIX);
    currentFileWriter = newTsFilePair.left;
    currentMergeResource = newTsFilePair.right;
    newResources.add(currentMergeResource);

    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      mergePaths(pathList);
      mergedSeriesCnt += pathList.size();
      logMergeProgress();
      if (!mergeFileNameHeap.isEmpty() && currentMergeResource.getFileSize() >=
          IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold()) {
        currentFileWriter.endFile();
        newTsFilePair = createNewFileWriter(mergeFileNameHeap, MERGE_SUFFIX);
        currentFileWriter = newTsFilePair.left;
        currentMergeResource = newTsFilePair.right;
        newResources.add(currentMergeResource);
      }
    }
    currentFileWriter.endFile();
    mergeLogger.logAllTsEnd();

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
      long totalChunkPoint = 0;
      long chunkNum = 0;
      for (TsFileResource seqFile : resource.getSeqFiles()) {
        List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(seqFile);
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          chunkNum++;
          totalChunkPoint += chunkMetadata.getNumOfPoints();
        }
      }
      logger.info("merge after seqFile chunk large = {}", totalChunkPoint * 1.0 / chunkNum);
    }

    for (TsFileResource tsFileResource : newResources) {
      File oldTsFile = tsFileResource.getFile();
      File newTsFile = new File(oldTsFile.getParent(),
          oldTsFile.getName().replace(MERGE_SUFFIX, ""));
      oldTsFile.renameTo(newTsFile);
      tsFileResource.setFile(newTsFile);
      tsFileResource.serialize();
    }
    return newResources;
  }

  private void constructMergeFileNameHeap() {
    for (TsFileResource seqFile : this.resource.getSeqFiles()) {
      mergeFileNameHeap.add(seqFile.getFile());
    }
  }
}
