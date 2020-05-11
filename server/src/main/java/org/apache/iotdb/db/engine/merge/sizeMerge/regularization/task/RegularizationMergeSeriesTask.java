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

import java.io.File;
import java.io.IOException;
import java.util.List;
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

  public RegularizationMergeSeriesTask(MergeContext context, String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    super(context, taskName, mergeLogger, mergeResource, unmergedSeries);
  }

  public TsFileResource mergeSeries() throws IOException {
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

    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = createNewFileWriter(
        MERGE_SUFFIX);
    newFileWriter = newTsFilePair.left;
    newResource = newTsFilePair.right;

    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      mergePaths(pathList);
      mergedSeriesCnt += pathList.size();
      logMergeProgress();
    }
    newFileWriter.endFile();
    mergeLogger.logAllTsEnd();

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
    if (logger.isInfoEnabled()) {
      long totalChunkPoint = 0;
      long chunkNum = 0;
      List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(newResource);
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        chunkNum++;
        totalChunkPoint += chunkMetadata.getNumOfPoints();
      }
      logger.info("merge after seqFile chunk large = {}", totalChunkPoint * 1.0 / chunkNum);
    }

    File oldTsFile = newResource.getFile();
    File newTsFile = new File(oldTsFile.getParent(),
        oldTsFile.getName().replace(RegularizationMergeTask.MERGE_SUFFIX, ""));
    oldTsFile.renameTo(newTsFile);
    newResource.setFile(newTsFile);
    newResource.serialize();
    return newResource;
  }
}
