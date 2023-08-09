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

public class AlignedBorderTablePrinter {
  static String[] header = {"", "Total", "Overlap", "Overlap/Total"};

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
