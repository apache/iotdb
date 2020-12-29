package org.apache.iotdb.cli.utils;

import java.io.PrintStream;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * @Description IOTPrinter
 * @Author weizihan0110
 * @Date 2020-12-29 15:37
 * @Version 1.0
 */

public class IOTPrinter {
  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);

  public static void printf(String format, Object... args) {
    SCREEN_PRINTER.printf(format, args);
  }

  public static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  public static void println() {
    SCREEN_PRINTER.println();
  }

  public static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }

  public static void printBlockLine(List<Integer> maxSizeList) {
    StringBuilder blockLine = new StringBuilder();
    for (Integer integer : maxSizeList) {
      blockLine.append("+").append(StringUtils.repeat("-", integer));
    }
    blockLine.append("+");
    println(blockLine.toString());
  }

  public static void printRow(List<List<String>> lists, int i, List<Integer> maxSizeList) {
    printf("|");
    for (int j = 0; j < maxSizeList.size(); j++) {
      printf("%" + maxSizeList.get(j) + "s|", lists.get(j).get(i));
    }
    println();
  }

  public static void printCount(int cnt) {
    if (cnt == 0) {
      println("Empty set.");
    } else {
      println("Total line number = " + cnt);
    }
  }

}
