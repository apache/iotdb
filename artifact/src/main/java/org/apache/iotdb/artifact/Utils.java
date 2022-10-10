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
package org.apache.iotdb.artifact;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {

  /**
   * load data from a given file
   *
   * @param f given file
   * @return DoubleArrayList loaded data
   * @throws FileNotFoundException
   */
  public static DoubleArrayList loadFile(File f) throws FileNotFoundException {
    Scanner sc = new Scanner(f);
    DoubleArrayList data = new DoubleArrayList();
    while (sc.hasNext()) {
      data.add(sc.nextDouble());
    }
    sc.close();
    return data;
  }

  /**
   * print the table into a CSV file
   *
   * @param path name of the CSV file
   * @param table table content in the M * N array
   * @param columns array of columns whose length is N
   * @param rows array of rows whose length is M
   */
  public static void printTable(String path, double[][] table, TSEncoding[] columns, File[] rows) {
    try {
      File f = new File(path);
      PrintWriter writer = new PrintWriter(f);
      // print the header
      for (TSEncoding r : columns) {
        writer.print("," + r);
      }
      writer.println();
      // print the body
      for (int i = 0; i < table.length; i++) {
        writer.print(rows[i]);
        for (int j = 0; j < table[i].length; j++) {
          writer.print("," + table[i][j]);
        }
        writer.println();
      }
      writer.close();
    } catch (IOException ex) {
      Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
