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

package org.apache.iotdb.cli.utils;

import org.apache.tsfile.external.commons.lang3.StringUtils;

import java.io.PrintStream;
import java.lang.Character.UnicodeScript;
import java.util.List;

public class IoTPrinter {
  private final PrintStream screenPrinter;

  public IoTPrinter(PrintStream screenPrinter) {
    this.screenPrinter = screenPrinter;
  }

  public void printf(String format, Object... args) {
    screenPrinter.printf(format, args);
  }

  public void print(String msg) {
    screenPrinter.print(msg);
  }

  public void printException(Exception msg) {
    screenPrinter.println(msg);
  }

  public void println() {
    screenPrinter.println();
  }

  public void println(String msg) {
    screenPrinter.println(msg);
  }

  public void printBlockLine(List<Integer> maxSizeList) {
    StringBuilder blockLine = new StringBuilder();
    for (Integer integer : maxSizeList) {
      blockLine.append("+").append(StringUtils.repeat("-", integer));
    }
    blockLine.append("+");
    println(blockLine.toString());
  }

  public void printRow(List<List<String>> lists, int i, List<Integer> maxSizeList) {
    printf("|");
    int count;
    int maxSize;
    String element;
    StringBuilder paddingStr;
    for (int j = 0; j < maxSizeList.size(); j++) {
      maxSize = maxSizeList.get(j);
      element = lists.get(j).get(i);
      count = computeHANCount(element);

      if (count > 0) {
        int remain = maxSize - (element.length() + count);
        if (remain > 0) {
          paddingStr = padding(remain);
          maxSize = maxSize - count;
          element = paddingStr.append(element).toString();
        } else if (remain == 0) {
          maxSize = maxSize - count;
        }
      }

      printf("%" + maxSize + "s|", element);
    }
    println();
  }

  public void printCount(int cnt) {
    if (cnt == 0) {
      println("Empty set.");
    } else {
      println("Total line number = " + cnt);
    }
  }

  public StringBuilder padding(int count) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < count; k++) {
      sb.append(' ');
    }

    return sb;
  }

  /** compute the number of Chinese characters included in the String */
  public int computeHANCount(String s) {
    return (int)
        s.codePoints()
            .filter(codePoint -> UnicodeScript.of(codePoint) == UnicodeScript.HAN)
            .count();
  }
}
