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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader.AsyncThreadExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader.SingleSequenceFileTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader.TaskSummary;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class TimePartitionProcessTask {
  private final String timePartition;
  private final Pair<List<String>, List<String>> timePartitionFiles;
  private long sequenceSpaceCost = 0;
  private long unsequenceSpaceCost = 0;

  public TimePartitionProcessTask(
      String timePartition, Pair<List<String>, List<String>> timePartitionFiles) {
    this.timePartition = timePartition;
    this.timePartitionFiles = timePartitionFiles;
  }

  public OverlapStatistic processTimePartition(AsyncThreadExecutor fileTaskExecutor) {
    long startTime = System.currentTimeMillis();
    UnseqSpaceStatistics unseqSpaceStatistics = buildUnseqSpaceStatistics(timePartitionFiles.right);
    OverlapStatistic partialRet =
        processSequenceSpaceAsync(fileTaskExecutor, unseqSpaceStatistics, timePartitionFiles.left);
    // 更新并打印进度
    OverlapStatisticTool.outputInfolock.lock();
    System.out.println();
    OverlapStatisticTool.processedTimePartitionCount += 1;
    OverlapStatisticTool.processedSeqFileCount += partialRet.totalSequenceFile;
    PrintUtil.printOneStatistics(partialRet, timePartition);
    System.out.printf("Sequence file num: %d, Sequence file size: %.2fM\n", partialRet.totalSequenceFile, ((double) partialRet.totalSequenceFileSize / 1024 / 1024));
    System.out.printf("Unsequence file num: %d, Unsequence file size: %.2fM\n", partialRet.totalUnsequenceFile, (double) partialRet.totalUnsequenceFileSize / 1024 / 1024);
    System.out.printf(
        Thread.currentThread().getName()
            + " Time cost: %.2fs, Sequence space cost: %.2fs, Build unsequence space cost: %.2fs.\n",
        ((double) System.currentTimeMillis() - startTime) / 1000,
        ((double) sequenceSpaceCost / 1000),
        ((double) unsequenceSpaceCost / 1000));

    OverlapStatisticTool.outputInfolock.unlock();
    return partialRet;
  }

  private UnseqSpaceStatistics buildUnseqSpaceStatistics(List<String> unseqFiles) {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();

    long startTime = System.currentTimeMillis();
    for (String unseqFile : unseqFiles) {
      File f = new File(unseqFile);
      if (!f.exists()) {
        continue;
      }
      unseqSpaceStatistics.unsequenceFileSize += f.length();
      try (TsFileStatisticReader reader = new TsFileStatisticReader(unseqFile)) {
        List<TsFileStatisticReader.ChunkGroupStatistics> chunkGroupStatisticsList =
            reader.getChunkGroupStatisticsList();

        for (TsFileStatisticReader.ChunkGroupStatistics statistics : chunkGroupStatisticsList) {
          long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;
          for (ChunkMetadata chunkMetadata : statistics.getChunkMetadataList()) {
            deviceStartTime = Math.min(deviceStartTime, chunkMetadata.getStartTime());
            deviceEndTime = Math.max(deviceEndTime, chunkMetadata.getEndTime());
            if (chunkMetadata.getStartTime() > chunkMetadata.getEndTime()) {
              continue;
            }
            unseqSpaceStatistics.updateMeasurement(
                statistics.getDeviceID(),
                chunkMetadata.getMeasurementUid(),
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          }
          if (deviceStartTime > deviceEndTime) {
            continue;
          }
          unseqSpaceStatistics.updateDevice(
              statistics.getDeviceID(), new Interval(deviceStartTime, deviceEndTime));
        }
      } catch (IOException e) {
        if (e instanceof NoSuchFileException) {
          System.out.println(((NoSuchFileException) e).getFile() + " is not exist");
          continue;
        }
        e.printStackTrace();
      }
    }
    unsequenceSpaceCost += (System.currentTimeMillis() - startTime);
    unseqSpaceStatistics.unsequenceFileNum += unseqFiles.size();
    return unseqSpaceStatistics;
  }

  public OverlapStatistic processSequenceSpaceAsync(
      AsyncThreadExecutor executor,
      UnseqSpaceStatistics unseqSpaceStatistics,
      List<String> seqFiles) {
    long startTime = System.currentTimeMillis();
    OverlapStatistic overlapStatistic = new OverlapStatistic();
    List<Future<TaskSummary>> futures = new ArrayList<>();
    for (String seqFile : seqFiles) {
      futures.add(executor.submit(new SingleSequenceFileTask(unseqSpaceStatistics, seqFile)));
    }
    for (Future<TaskSummary> future : futures) {
      try {
        TaskSummary taskSummary = future.get();
        overlapStatistic.overlappedChunkGroupsInSequenceFile += taskSummary.overlapChunkGroup;
        overlapStatistic.totalChunkGroupsInSequenceFile += taskSummary.totalChunkGroups;
        overlapStatistic.overlappedChunksInSequenceFile += taskSummary.overlapChunk;
        overlapStatistic.totalChunksInSequenceFile += taskSummary.totalChunks;

        overlapStatistic.totalSequenceFileSize += taskSummary.fileSize;
        if (taskSummary.overlapChunkGroup > 0) {
          overlapStatistic.overlappedSequenceFiles++;
        }
      } catch (Exception e) {
        e.printStackTrace();
        // todo
      }
    }
    overlapStatistic.totalSequenceFile += seqFiles.size();
    overlapStatistic.totalUnsequenceFile += unseqSpaceStatistics.unsequenceFileNum;
    overlapStatistic.totalUnsequenceFileSize += unseqSpaceStatistics.unsequenceFileSize;

    sequenceSpaceCost += (System.currentTimeMillis() - startTime);
    return overlapStatistic;
  }
}
