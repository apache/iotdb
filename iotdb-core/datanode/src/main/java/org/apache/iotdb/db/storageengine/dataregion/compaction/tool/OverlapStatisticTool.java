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

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.TsFileStatisticReader.ChunkGroupStatistics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

import java.io.IOException;
import java.util.List;

public class OverlapStatisticTool {
  private List<Long> timePartitions;
  private long seqFileCount;

  private long processedTimePartitionCount;
  private long processedSeqFileCount;

  public static void main(String args[]) {

    // 1. 处理参数，从输入中获取数据目录的路径

    // 2. 进行计算
    OverlapStatisticTool tool = new OverlapStatisticTool();
    tool.process(null);
  }

  public void process(List<String> dataDirs) {
    // 0. 预处理
    processDataDirs();

    // 1. 构造最终结果集
    OverlapStatistic statistic = new OverlapStatistic();

    // 2. 根据时间分区的信息
    for (Long timePartition : timePartitions) {
      OverlapStatistic partialRet = processOneTimePartiton(timePartition, dataDirs);
      // 将该时间分区的结果集更新到最终结果集

      // 更新并打印进度

    }
  }

  private void updateProcessAndPrint(OverlapStatistic partialRet) {
    processedTimePartitionCount += 1;
    processedSeqFileCount += partialRet.totalFiles;

    // 打印进度
  }

  private void processDataDirs() {
    // 1. 遍历所有的时间分区，构造 timePartitions

    // 2. 统计顺序文件的总数
  }

  private OverlapStatistic processOneTimePartiton(long timePartition, List<String> dataDirs) {
    // 1. 根据 timePartition，获取所有数据目录下的的乱序文件，构造 UnseqSpaceStatistics
    UnseqSpaceStatistics unseqSpaceStatistics = buildUnseqSpaceStatistics(timePartition, dataDirs);

    // 2. 遍历该时间分区下的所有顺序文件，获取每一个 chunk 的信息，依次进行 overlap 检查，并更新统计信息
    OverlapStatistic overlapStatistic = new OverlapStatistic();
    List<String> seqFiles = getFilesInOnePartition(timePartition, dataDirs, true);
    for (String seqFile : seqFiles) {
      try (TsFileStatisticReader reader = new TsFileStatisticReader(seqFile)) {
        // 统计顺序文件的信息并更新到 overlapStatistic
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return overlapStatistic;
  }

  private UnseqSpaceStatistics buildUnseqSpaceStatistics(
      long timePartition, List<String> dataDirs) {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();

    List<String> unseqFiles = getFilesInOnePartition(timePartition, dataDirs, false);
    for (String unseqFile : unseqFiles) {
      try (TsFileStatisticReader reader = new TsFileStatisticReader(unseqFile)) {
        List<ChunkGroupStatistics> chunkGroupStatisticsList = reader.getChunkGroupStatistics();
        for (ChunkGroupStatistics statistics : chunkGroupStatisticsList) {
          for (ChunkMetadata chunkMetadata : statistics.getChunkMetadataList()) {
            unseqSpaceStatistics.update(
                statistics.getDeviceID(),
                chunkMetadata.getMeasurementUid(),
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return unseqSpaceStatistics;
  }

  private List<String> getFilesInOnePartition(
      long timePartition, List<String> dataDirs, boolean isSeq) {
    return null;
  }

  private static class OverlapStatistic {
    private long totalFiles;
    private long totalChunkGroups;
    private long totalChunks;

    private long overlappedFiles;
    private long overlappedChunkGroups;
    private long overlappedChunks;
  }
}
