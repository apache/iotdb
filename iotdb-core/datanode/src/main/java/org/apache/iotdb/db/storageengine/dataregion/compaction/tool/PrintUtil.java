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

import static org.apache.iotdb.db.storageengine.dataregion.compaction.tool.AlignedBorderTablePrinter.printTable;

public class PrintUtil {

  public static void printOneStatistics(OverlapStatistic overlapStatistic, String label) {
    printTableLog(overlapStatistic);
    printProgressLog(label);
  }

  private static void printProgressLog(String label) {
    System.out.printf(
        "Progress: %s\n" + "Sequence File progress: %d/%d\n" + "Partition progress: %d/%d %s",
        label,
        OverlapStatisticTool.processedSeqFileCount,
        OverlapStatisticTool.seqFileCount,
        OverlapStatisticTool.processedTimePartitionCount,
        OverlapStatisticTool.timePartitionFileMap.size(),
        System.getProperty("line.separator"));
  }

  private static void printTableLog(OverlapStatistic overlapStatistic) {
    double overlappedSeqFilePercentage =
        calculatePercentage(overlapStatistic.overlappedSequenceFiles, overlapStatistic.totalSequenceFile);
    double overlappedChunkGroupPercentage =
        calculatePercentage(
            overlapStatistic.overlappedChunkGroupsInSequenceFile, overlapStatistic.totalChunkGroupsInSequenceFile);
    double overlappedChunkPercentage =
        calculatePercentage(overlapStatistic.overlappedChunksInSequenceFile, overlapStatistic.totalChunksInSequenceFile);
    String[][] log = {
      {
        "Sequence File",
        overlapStatistic.totalSequenceFile + "",
        overlapStatistic.overlappedSequenceFiles + "",
        String.format("%.2f%%", overlappedSeqFilePercentage)
      },
      {
        "ChunkGroup In Sequence File",
        overlapStatistic.totalChunkGroupsInSequenceFile + "",
        overlapStatistic.overlappedChunkGroupsInSequenceFile + "",
        String.format("%.2f%%", overlappedChunkGroupPercentage)
      },
      {
        "Chunk In Sequence File",
        overlapStatistic.totalChunksInSequenceFile + "",
        overlapStatistic.overlappedChunksInSequenceFile + "",
        String.format("%.2f%%", overlappedChunkPercentage)
      }
    };
    printTable(log);
  }

  private static double calculatePercentage(long numerator, long denominator) {
    return denominator != 0 ? (double) numerator / denominator * 100 : 0;
  }
}
