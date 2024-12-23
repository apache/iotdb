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
  static String[] header_1 = {"", "Total", "Overlap", "Overlap/Total"};
  static String[] header_2 = {"", "Total", "Sequence", "UnSequence", "UnSequence/Total"};

  static long MSize = 1024 * 1024L;

  public static void printOneStatistics(OverlapStatistic overlapStatistic, String label) {
    System.out.println();
    printTableLog(overlapStatistic);
    printProgressLog(label, overlapStatistic);
  }

  private static void printProgressLog(String label, OverlapStatistic statistic) {
    String[][] log = {
      {
        "File Number",
        statistic.totalSequenceFile + statistic.totalUnsequenceFile + "",
        statistic.totalSequenceFile + "",
        statistic.totalUnsequenceFile + "",
        String.format(
            "%.2f%%",
            statistic.totalUnsequenceFile
                * 100d
                / (statistic.totalSequenceFile + statistic.totalUnsequenceFile))
      },
      {
        "File Size(MB)",
        (statistic.totalSequenceFileSize + statistic.totalUnsequenceFileSize) / MSize + "",
        statistic.totalSequenceFileSize / MSize + "",
        statistic.totalUnsequenceFileSize / MSize + "",
        String.format(
            "%.2f%%",
            statistic.totalUnsequenceFileSize
                * 100d
                / (statistic.totalSequenceFileSize + statistic.totalUnsequenceFileSize))
      },
      {
        "Duration",
        Math.max(statistic.sequenceMaxEndTime, statistic.unSequenceMaxEndTime)
            - Math.min(statistic.sequenceMinStartTime, statistic.unSequenceMinStartTime)
            + "",
        statistic.sequenceMaxEndTime - statistic.sequenceMinStartTime + "",
        statistic.unSequenceMaxEndTime - statistic.unSequenceMinStartTime + "",
        String.format(
            "%.2f%%",
            (statistic.unSequenceMaxEndTime - statistic.unSequenceMinStartTime)
                * 100d
                / (Math.max(statistic.sequenceMaxEndTime, statistic.unSequenceMaxEndTime)
                    - Math.min(statistic.sequenceMinStartTime, statistic.unSequenceMinStartTime)))
      }
    };
    System.out.println(System.getProperty("line.separator") + "Data Table:");
    printStaticsTable(log);

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
    System.out.println("Overlap Table:");
    printOverlapTable(log);
  }

  private static double calculatePercentage(long numerator, long denominator) {
    return denominator != 0 ? (double) numerator / denominator * 100 : 0;
  }

  public static void printOverlapTable(String[][] data) {
    int numRows = data.length;
    int[] maxCellWidths = calculateMaxCellWidths(header_1, data);

    printTopBorder(maxCellWidths);
    printRow(header_1, maxCellWidths);

    for (int row = 0; row < numRows; row++) {
      printSeparator(maxCellWidths);
      printRow(data[row], maxCellWidths);
    }

    printBottomBorder(maxCellWidths);
  }

  public static void printStaticsTable(String[][] data) {
    int numRows = data.length;
    int[] maxCellWidths = calculateMaxCellWidths(header_2, data);

    printTopBorder(maxCellWidths);
    printRow(header_2, maxCellWidths);

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
