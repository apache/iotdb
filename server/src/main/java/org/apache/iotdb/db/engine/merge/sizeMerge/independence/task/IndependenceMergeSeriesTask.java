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

package org.apache.iotdb.db.engine.merge.sizeMerge.independence.task;

import static org.apache.iotdb.db.engine.merge.sizeMerge.independence.task.IndependenceMergeTask.MERGE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.merge.BaseMergeSeriesTask;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndependenceMergeSeriesTask extends BaseMergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      IndependenceMergeSeriesTask.class);

  public IndependenceMergeSeriesTask(MergeContext context, String taskName, MergeLogger mergeLogger,
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

    List<Pair<RestorableTsFileIOWriter, TsFileResource>> newTsFilePairs = new ArrayList<>();
    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = createNewFileWriter(
          MERGE_SUFFIX);
      newFileWriter = newTsFilePair.left;
      newResource = newTsFilePair.right;
      newTsFilePairs.add(newTsFilePair);
      mergePaths(pathList);
      mergedSeriesCnt += pathList.size();
      logMergeProgress();
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
    if (logger.isInfoEnabled()) {
      long totalChunkPoint = 0;
      long chunkNum = 0;
      for (TsFileResource seqFile : newResources) {
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
}
