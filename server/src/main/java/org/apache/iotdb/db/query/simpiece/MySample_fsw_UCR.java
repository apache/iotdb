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


package org.apache.iotdb.db.query.simpiece;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class MySample_fsw_UCR {

  public static void main(String[] args) {
    String fileDir = "D:\\3\\tmp\\";
    File folder = new File(fileDir);
    // 调用递归函数遍历文件夹
    listFiles(folder);
  }

  public static void listFiles(File folder) {
    File[] files = folder.listFiles();

    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          // 如果是文件夹，递归遍历
          listFiles(file);
        } else if (file.isFile() && file.getName().endsWith(".csv") && !file.getName()
            .contains("-segment")) {
          // 如果是csv文件，读取内容
          System.out.println("Reading file: " + file.getAbsolutePath());
          readCSV(file);
        }
      }
    }
  }

  public static void readCSV(File file) {
    int start = 0;
    int N = -1;
    boolean hasHeader = false;
    try (FileInputStream inputStream = new FileInputStream(file)) {
      String delimiter = ",";
      TimeSeries ts =
          TimeSeriesReader.getMyTimeSeries(
              inputStream, delimiter, false, N, start, hasHeader, true);
      int n = ts.length();

      int nout = n / 2;
      nout = (nout / 4) * 4; // for M4 requires nout to be four integer multiples

      nout = Math.min(800, nout);

      double epsilon = MySample_fsw2.getFSWParam(nout, ts, 1e-6);
      List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
      System.out.println(
          file
              + ": n="
              + n
              + ",target m="
              + nout
              + ",epsilon="
              + epsilon
              + ",actual m="
              + reducedPoints.size());

      String newFileName = file.getName().replace(".csv", "-segment-fsw.csv");
      File newFile = new File(file.getParent(), newFileName);
      try (PrintWriter writer = new PrintWriter(new FileWriter(newFile))) {
        for (Point p : reducedPoints) {
          writer.println(p.getTimestamp() + "," + p.getValue());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
