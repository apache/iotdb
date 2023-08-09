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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.tool.AlignedBorderTablePrinter.printTable;

public class OverlapStatisticTool {
  private long seqFileCount;

  private long processedTimePartitionCount;
  private long processedSeqFileCount;
  private final Map<String, Pair<List<String>, List<String>>> timePartitionFileMap =
      new HashMap<>();

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Please input data dir paths.");
      return;
    }
    OverlapStatisticTool tool = new OverlapStatisticTool();
    long startTime = System.currentTimeMillis();
    // 1. 处理参数，从输入中获取数据目录的路径
    List<String> dataDirs = tool.getDataDirsFromArgs(args);
    // 2. 进行计算
    tool.process(dataDirs);
    System.out.printf(
        "Total time cost: %.2fs\n", ((double) System.currentTimeMillis() - startTime) / 1000);
  }

  private List<String> getDataDirsFromArgs(String[] args) {
    return new ArrayList<>(Arrays.asList(args));
  }

  public void process(List<String> dataDirs) {
    // 0. 预处理
    processDataDirs(dataDirs);

    // 1. 构造最终结果集
    OverlapStatistic statistic = new OverlapStatistic();
    for (Map.Entry<String, Pair<List<String>, List<String>>> timePartitionFilesEntry :
        timePartitionFileMap.entrySet()) {
      String timePartition = timePartitionFilesEntry.getKey();
      Pair<List<String>, List<String>> timePartitionFiles = timePartitionFilesEntry.getValue();
      OverlapStatistic partialRet =
          processOneTimePartition(timePartitionFiles.left, timePartitionFiles.right);

      // 更新并打印进度
      processedTimePartitionCount += 1;
      processedSeqFileCount += partialRet.totalFiles;
      printOneStatistics(partialRet, timePartition);

      // 2. 根据时间分区的信息
      // 将该时间分区的结果集更新到最终结果集
      statistic.merge(partialRet);
    }
    printOneStatistics(statistic, "All EXECUTED");
  }

  private void printOneStatistics(OverlapStatistic overlapStatistic, String label) {
    printTableLog(overlapStatistic);
    printProgressLog(label);
  }

  private void printProgressLog(String label) {
    System.out.printf(
        "All progress: %s\n" + "File progress: %d/%d\n" + "Partition progress: %d/%d %s",
        label,
        processedSeqFileCount,
        seqFileCount,
        processedTimePartitionCount,
        timePartitionFileMap.size(),
        System.getProperty("line.separator"));
  }

  private void printTableLog(OverlapStatistic overlapStatistic) {
    double overlappedSeqFilePercentage =
        calculatePercentage(overlapStatistic.overlappedFiles, overlapStatistic.totalFiles);
    double overlappedChunkGroupPercentage =
        calculatePercentage(
            overlapStatistic.overlappedChunkGroups, overlapStatistic.totalChunkGroups);
    double overlappedChunkPercentage =
        calculatePercentage(overlapStatistic.overlappedChunks, overlapStatistic.totalChunks);
    String[][] log = {
      {
        "File",
        overlapStatistic.totalFiles + "",
        overlapStatistic.overlappedFiles + "",
        overlappedSeqFilePercentage + ""
      },
      {
        "ChunkGroup",
        overlapStatistic.totalChunkGroups + "",
        overlapStatistic.overlappedChunkGroups + "",
        overlappedChunkGroupPercentage + ""
      },
      {
        "Chunk",
        overlapStatistic.totalChunkGroups + "",
        overlapStatistic.overlappedChunks + "",
        overlappedChunkPercentage + ""
      }
    };
    printTable(log);
  }

  private double calculatePercentage(long numerator, long denominator) {
    return denominator != 0 ? (double) numerator / denominator * 100 : 0;
  }

  private void processDataDirs(List<String> dataDirs) {
    // 1. 遍历所有的时间分区，构造 timePartitions
    // 2. 统计顺序文件的总数
    for (String dataDirPath : dataDirs) {
      File dataDir = new File(dataDirPath);
      if (!dataDir.exists() || !dataDir.isDirectory()) {
        continue;
      }
      processDataDirWithIsSeq(dataDirPath, true);
      processDataDirWithIsSeq(dataDirPath, false);
    }
  }

  private void processDataDirWithIsSeq(String dataDirPath, boolean isSeq) {
    String dataDirWithIsSeq;
    if (isSeq) {
      dataDirWithIsSeq = dataDirPath + File.separator + "sequence";
    } else {
      dataDirWithIsSeq = dataDirPath + File.separator + "unsequence";
    }
    File dataDirWithIsSequence = new File(dataDirWithIsSeq);
    if (!dataDirWithIsSequence.exists() || !dataDirWithIsSequence.isDirectory()) {
      System.out.println(dataDirWithIsSequence + " is not a correct path");
      return;
    }

    for (File storageGroupDir : Objects.requireNonNull(dataDirWithIsSequence.listFiles())) {
      if (!storageGroupDir.isDirectory()) {
        continue;
      }
      String storageGroup = storageGroupDir.getName();
      for (File dataRegionDir : Objects.requireNonNull(storageGroupDir.listFiles())) {
        if (!dataRegionDir.isDirectory()) {
          continue;
        }
        String dataRegion = dataRegionDir.getName();
        for (File timePartitionDir : Objects.requireNonNull(dataRegionDir.listFiles())) {
          if (!timePartitionDir.isDirectory()) {
            continue;
          }

          String timePartitionKey =
              calculateTimePartitionKey(storageGroup, dataRegion, timePartitionDir.getName());
          Pair<List<String>, List<String>> timePartitionFiles =
              timePartitionFileMap.computeIfAbsent(
                  timePartitionKey, v -> new Pair<>(new ArrayList<>(), new ArrayList<>()));
          for (File file : Objects.requireNonNull(timePartitionDir.listFiles())) {
            if (!file.isFile()) {
              continue;
            }
            if (!file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
              continue;
            }
            String resourceFilePath = file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;
            if (!new File(resourceFilePath).exists()) {
              System.out.println(
                  resourceFilePath
                      + " is not exist, the tsfile is skipped because it is not closed.");
              continue;
            }
            String filePath = file.getAbsolutePath();
            if (isSeq) {
              timePartitionFiles.left.add(filePath);
              seqFileCount++;
            } else {
              timePartitionFiles.right.add(filePath);
            }
          }
        }
      }
    }
  }

  private String calculateTimePartitionKey(
      String storageGroup, String dataRegion, String timePartition) {
    return storageGroup + "-" + dataRegion + "-" + timePartition;
  }

  private OverlapStatistic processOneTimePartition(List<String> seqFiles, List<String> unseqFiles) {
    // 1. 根据 timePartition，获取所有数据目录下的的乱序文件，构造 UnseqSpaceStatistics
    UnseqSpaceStatistics unseqSpaceStatistics = buildUnseqSpaceStatistics(unseqFiles);

    // 2. 遍历该时间分区下的所有顺序文件，获取每一个 chunk 的信息，依次进行 overlap 检查，并更新统计信息
    OverlapStatistic overlapStatistic = new OverlapStatistic();
    overlapStatistic.totalFiles += seqFiles.size();
    for (String seqFile : seqFiles) {
      boolean isFileOverlap = false;
      try (TsFileStatisticReader reader = new TsFileStatisticReader(seqFile)) {
        // 统计顺序文件的信息并更新到 overlapStatistic
        List<ChunkGroupStatistics> chunkGroupStatisticsList = reader.getChunkGroupStatistics();
        for (ChunkGroupStatistics chunkGroupStatistics : chunkGroupStatisticsList) {
          overlapStatistic.totalChunks += chunkGroupStatistics.getTotalChunkNum();
          String deviceId = chunkGroupStatistics.getDeviceID();
          int overlapChunkNum = 0;

          long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;
          for (ChunkMetadata chunkMetadata : chunkGroupStatistics.getChunkMetadataList()) {
            deviceStartTime = Math.min(deviceStartTime, chunkMetadata.getStartTime());
            deviceEndTime = Math.max(deviceEndTime, chunkMetadata.getEndTime());
            Interval interval =
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime());
            String measurementId = chunkMetadata.getMeasurementUid();
            if (unseqSpaceStatistics.chunkHasOverlap(deviceId, measurementId, interval)) {
              overlapChunkNum++;
            }
          }
          overlapStatistic.overlappedChunks += overlapChunkNum;

          Interval deviceInterval = new Interval(deviceStartTime, deviceEndTime);
          if (unseqSpaceStatistics.chunkGroupHasOverlap(deviceId, deviceInterval)) {
            isFileOverlap = true;
            overlapStatistic.overlappedChunkGroups++;
          }
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

  private UnseqSpaceStatistics buildUnseqSpaceStatistics(List<String> unseqFiles) {
    UnseqSpaceStatistics unseqSpaceStatistics = new UnseqSpaceStatistics();

    for (String unseqFile : unseqFiles) {
      try (TsFileStatisticReader reader = new TsFileStatisticReader(unseqFile)) {
        List<ChunkGroupStatistics> chunkGroupStatisticsList = reader.getChunkGroupStatistics();
        for (ChunkGroupStatistics statistics : chunkGroupStatisticsList) {
          long deviceStartTime = Long.MAX_VALUE, deviceEndTime = Long.MIN_VALUE;
          for (ChunkMetadata chunkMetadata : statistics.getChunkMetadataList()) {
            unseqSpaceStatistics.updateMeasurement(
                statistics.getDeviceID(),
                chunkMetadata.getMeasurementUid(),
                new Interval(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
            deviceStartTime = Math.min(deviceStartTime, chunkMetadata.getStartTime());
            deviceEndTime = Math.max(deviceEndTime, chunkMetadata.getEndTime());
          }
          unseqSpaceStatistics.updateDevice(
              statistics.getDeviceID(), new Interval(deviceStartTime, deviceEndTime));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return unseqSpaceStatistics;
  }

  private static class OverlapStatistic {
    private long totalFiles;
    private long totalChunkGroups;
    private long totalChunks;

    private long overlappedFiles;
    private long overlappedChunkGroups;
    private long overlappedChunks;

    private void merge(OverlapStatistic other) {
      this.totalFiles += other.totalFiles;
      this.totalChunkGroups += other.totalChunkGroups;
      this.totalChunks += other.totalChunks;
      this.overlappedFiles += other.overlappedFiles;
      this.overlappedChunkGroups += other.overlappedChunkGroups;
      this.overlappedChunks += other.overlappedChunks;
    }
  }
}
