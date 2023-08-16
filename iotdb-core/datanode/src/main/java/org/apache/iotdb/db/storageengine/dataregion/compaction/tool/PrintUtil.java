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

class PrintUtil {
  static String[] header = {"", "Total", "Overlap", "Overlap/Total"};

  public static void printOneStatistics(OverlapStatistic overlapStatistic, String label) {
    printTableLog(overlapStatistic);
    printProgressLog(label, overlapStatistic);
  }

  private static void printProgressLog(String label, OverlapStatistic statistic) {
    System.out.printf(
        "Progress: %s\n" + "Sequence File progress: %d/%d\n" + "Partition progress: %d/%d %s",
        label,
        OverlapStatisticTool.processedSeqFileCount,
        OverlapStatisticTool.seqFileCount,
        OverlapStatisticTool.processedTimePartitionCount,
        OverlapStatisticTool.timePartitionFileMap.size(),
        System.getProperty("line.separator"));
    System.out.printf(
        "Sequence file num: %d, Sequence file size: %.2fM\n",
        statistic.totalSequenceFile, ((double) statistic.totalSequenceFileSize / 1024 / 1024));
    System.out.printf(
        "Unsequence file num: %d, Unsequence file size: %.2fM\n",
        statistic.totalUnsequenceFile, (double) statistic.totalUnsequenceFileSize / 1024 / 1024);
  }

  private static void printTableLog(OverlapStatistic overlapStatistic) {
    double overlappedSeqFilePercentage =
        calculatePercentage(
            overlapStatistic.overlappedSequenceFiles, overlapStatistic.totalSequenceFile);
    double overlappedChunkGroupPercentage =
        calculatePercentage(
            overlapStatistic.overlappedChunkGroupsInSequenceFile,
            overlapStatistic.totalChunkGroupsInSequenceFile);
    double overlappedChunkPercentage =
        calculatePercentage(
            overlapStatistic.overlappedChunksInSequenceFile,
            overlapStatistic.totalChunksInSequenceFile);
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

  public static void printTable(String[][] data) {
    int numRows = data.length;
    int numCols = data[0].length;
    int[] maxCellWidths = calculateMaxCellWidths(header, data);

    printTopBorder(maxCellWidths);
    printRow(header, maxCellWidths);

    for (int row = 0; row < numRows; row++) {
      printSeparator(maxCellWidths);
      printRow(data[row], maxCellWidths);
    }

    printBottomBorder(maxCellWidths);
  }

  private static int[] calculateMaxCellWidths(String[] header, String[][] data) {
    int numCols = header.length;
    int[] maxCellWidths = new int[numCols];

    for (int col = 0; col < numCols; col++) {
      maxCellWidths[col] = header[col].length();
      for (String[] row : data) {
        maxCellWidths[col] = Math.max(maxCellWidths[col], row[col].length());
      }
    }

    return maxCellWidths;
  }

  private static void printTopBorder(int[] maxCellWidths) {
    System.out.print("┌");
    for (int width : maxCellWidths) {
      printRepeat("─", width + 2);
      System.out.print("┬");
    }
    System.out.println();
  }

  private static void printSeparator(int[] maxCellWidths) {
    System.out.print("├");
    for (int width : maxCellWidths) {
      printRepeat("─", width + 2);
      System.out.print("┼");
    }
    System.out.println();
  }

  private static void printBottomBorder(int[] maxCellWidths) {
    System.out.print("└");
    for (int width : maxCellWidths) {
      printRepeat("─", width + 2);
      System.out.print("┴");
    }
    System.out.println();
  }

  private static void printRow(String[] row, int[] maxCellWidths) {
    for (int col = 0; col < row.length; col++) {
      System.out.printf("│ %-" + maxCellWidths[col] + "s ", row[col]);
    }
    System.out.println("│");
  }

  private static void printRepeat(String value, int times) {
    for (int i = 0; i < times; i++) {
      System.out.print(value);
    }
  }
}
