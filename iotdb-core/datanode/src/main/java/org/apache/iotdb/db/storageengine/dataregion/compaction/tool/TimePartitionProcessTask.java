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

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.List;

public class TimePartitionProcessTask {
  private final String timePartition;
  private final Pair<List<String>, List<String>> timePartitionFiles;

  public TimePartitionProcessTask(
      String timePartition, Pair<List<String>, List<String>> timePartitionFiles) {
    this.timePartition = timePartition;
    this.timePartitionFiles = timePartitionFiles;
  }

  public OverlapStatistic processTimePartition() {
    long startTime = System.currentTimeMillis();
    OverlapStatistic partialRet =
        processOneTimePartition(timePartitionFiles.left, timePartitionFiles.right);
    // 更新并打印进度
    OverlapStatisticTool.outputInfolock.lock();
    OverlapStatisticTool.processedTimePartitionCount += 1;
    OverlapStatisticTool.processedSeqFileCount += partialRet.totalFiles;
    PrintUtil.printOneStatistics(partialRet, timePartition);
    OverlapStatisticTool.outputInfolock.unlock();
    System.out.printf(
        Thread.currentThread().getName() + " Time cost: %.2fs\n",
        ((double) System.currentTimeMillis() - startTime) / 1000);

    return partialRet;
  }

  private UnseqSpaceStatistics buildUnseqSpaceStatistics(List<String> unseqFiles) {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();

    for (String unseqFile : unseqFiles) {
      try (TsFileStatisticReader reader = new TsFileStatisticReader(unseqFile)) {
        List<TsFileStatisticReader.ChunkGroupStatistics> chunkGroupStatisticsList =
            reader.getChunkGroupStatistics();
        for (TsFileStatisticReader.ChunkGroupStatistics statistics : chunkGroupStatisticsList) {
          for (ChunkMetadata chunkMetadata : statistics.getChunkMetadataList()) {
            unseqSpaceStatistics.updateMeasurement(
                statistics.getDeviceID(),
                chunkMetadata.getMeasurementUid(),
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          }
          if (statistics.getStartTime() < statistics.getEndTime()) {
            unseqSpaceStatistics.updateDevice(
                statistics.getDeviceID(),
                new Interval(statistics.getStartTime(), statistics.getEndTime()));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return unseqSpaceStatistics;
  }

  public OverlapStatistic processOneTimePartition(List<String> seqFiles, List<String> unseqFiles) {
    // 1. 根据 timePartition，获取所有数据目录下的的乱序文件，构造 UnseqSpaceStatistics
    UnseqSpaceStatistics unseqSpaceStatistics = buildUnseqSpaceStatistics(unseqFiles);

    // 2. 遍历该时间分区下的所有顺序文件，获取每一个 chunk 的信息，依次进行 overlap 检查，并更新统计信息
    OverlapStatistic overlapStatistic = new OverlapStatistic();
    overlapStatistic.totalFiles += seqFiles.size();
    for (String seqFile : seqFiles) {
      boolean isFileOverlap = false;
      try (TsFileStatisticReader reader = new TsFileStatisticReader(seqFile)) {
        // 统计顺序文件的信息并更新到 overlapStatistic
        List<TsFileStatisticReader.ChunkGroupStatistics> chunkGroupStatisticsList =
            reader.getChunkGroupStatistics();
        for (TsFileStatisticReader.ChunkGroupStatistics chunkGroupStatistics :
            chunkGroupStatisticsList) {
          overlapStatistic.totalChunks += chunkGroupStatistics.getTotalChunkNum();
          String deviceId = chunkGroupStatistics.getDeviceID();
          int overlapChunkNum = 0;

          long deviceStartTime = chunkGroupStatistics.getStartTime(),
              deviceEndTime = chunkGroupStatistics.getEndTime();
          if (deviceStartTime >= deviceEndTime) {
            // skip empty chunk group
            continue;
          }
          Interval deviceInterval = new Interval(deviceStartTime, deviceEndTime);
          if (!unseqSpaceStatistics.chunkGroupHasOverlap(deviceId, deviceInterval)) {
            // if chunk group is not overlapped, all chunk of it is not overlapped
            continue;
          }
          isFileOverlap = true;
          overlapStatistic.overlappedChunkGroups++;

          for (ChunkMetadata chunkMetadata : chunkGroupStatistics.getChunkMetadataList()) {
            if (chunkMetadata.getStartTime() >= chunkMetadata.getEndTime()) {
              // skip empty chunk
              continue;
            }
            Interval interval =
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime());
            String measurementId = chunkMetadata.getMeasurementUid();
            if (unseqSpaceStatistics.chunkHasOverlap(deviceId, measurementId, interval)) {
              overlapChunkNum++;
            }
          }
          overlapStatistic.overlappedChunks += overlapChunkNum;
        }
        overlapStatistic.totalChunkGroups += chunkGroupStatisticsList.size();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (isFileOverlap) {
        overlapStatistic.overlappedFiles += 1;
      }
    }
    return overlapStatistic;
  }
}
