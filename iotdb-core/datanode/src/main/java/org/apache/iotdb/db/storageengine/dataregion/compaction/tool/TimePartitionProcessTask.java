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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class TimePartitionProcessTask {
  private final String timePartition;
  private final Pair<List<String>, List<String>> timePartitionFiles;
  private long sequenceSpaceCost = 0;
  private long unsequenceSpaceCost = 0;

  private AsyncThreadExecutor executor;

  public TimePartitionProcessTask(
      String timePartition, Pair<List<String>, List<String>> timePartitionFiles) {
    this.timePartition = timePartition;
    this.timePartitionFiles = timePartitionFiles;
  }

  public OverlapStatistic processTimePartition() {
    long startTime = System.currentTimeMillis();
    OverlapStatistic partialRet =
        processOneTimePartitionAsync(timePartitionFiles.left, timePartitionFiles.right);
    // 更新并打印进度
    OverlapStatisticTool.outputInfolock.lock();
    OverlapStatisticTool.processedTimePartitionCount += 1;
    OverlapStatisticTool.processedSeqFileCount += partialRet.totalFiles;
    PrintUtil.printOneStatistics(partialRet, timePartition);
    OverlapStatisticTool.outputInfolock.unlock();
    System.out.printf("Unsequence file num: %d\n", timePartitionFiles.getRight().size());
    System.out.printf(
        Thread.currentThread().getName()
            + " Time cost: %.2fs, Sequence space cost: %.2fs, Build unsequence space cost: %.2fs.\n",
        ((double) System.currentTimeMillis() - startTime) / 1000,
        ((double) sequenceSpaceCost / 1000),
        ((double) unsequenceSpaceCost / 1000));

    return partialRet;
  }

  private UnseqSpaceStatistics buildUnseqSpaceStatistics(List<String> unseqFiles) {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();

    long startTime = System.currentTimeMillis();
    for (String unseqFile : unseqFiles) {
      try (TsFileStatisticReader reader = new TsFileStatisticReader(unseqFile)) {
        List<TsFileStatisticReader.ChunkGroupStatistics> chunkGroupStatisticsList =
            reader.getChunkGroupStatistics();

        for (TsFileStatisticReader.ChunkGroupStatistics statistics : chunkGroupStatisticsList) {
          long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;
          for (ChunkMetadata chunkMetadata : statistics.getChunkMetadataList()) {
            deviceStartTime = Math.min(deviceStartTime, chunkMetadata.getStartTime());
            deviceEndTime = Math.max(deviceEndTime, chunkMetadata.getEndTime());
            unseqSpaceStatistics.updateMeasurement(
                statistics.getDeviceID(),
                chunkMetadata.getMeasurementUid(),
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          }
          unseqSpaceStatistics.updateDevice(
              statistics.getDeviceID(), new Interval(deviceStartTime, deviceEndTime));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    unsequenceSpaceCost += (System.currentTimeMillis() - startTime);
    return unseqSpaceStatistics;
  }

  public OverlapStatistic processOneTimePartitionAsync(
      List<String> seqFiles, List<String> unseqFiles) {
    UnseqSpaceStatistics unseqSpaceStatistics = buildUnseqSpaceStatistics(unseqFiles);
    long startTime = System.currentTimeMillis();
    OverlapStatistic overlapStatistic = new OverlapStatistic();
    overlapStatistic.totalFiles += seqFiles.size();
    executor = new AsyncThreadExecutor(10);
    List<Future<TaskSummary>> futures = new ArrayList<>();
    for (String seqFile : seqFiles) {
      futures.add(executor.submit(new SingleSequenceFileTask(unseqSpaceStatistics, seqFile)));
    }
    for (Future<TaskSummary> future : futures) {
      try {
        TaskSummary taskSummary = future.get();
        overlapStatistic.overlappedChunkGroups += taskSummary.overlapChunkGroup;
        overlapStatistic.totalChunkGroups += taskSummary.totalChunkGroups;
        overlapStatistic.overlappedChunks += taskSummary.overlapChunk;
        overlapStatistic.totalChunks += taskSummary.totalChunks;
        if (taskSummary.overlapChunkGroup > 0) {
          overlapStatistic.overlappedFiles++;
        }
      } catch (Exception e) {
        // todo
      }
    }
    executor.shutdown();
    sequenceSpaceCost += (System.currentTimeMillis() - startTime);
    return overlapStatistic;
  }
}
