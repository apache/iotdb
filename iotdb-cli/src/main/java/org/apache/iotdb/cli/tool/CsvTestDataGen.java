/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.cli.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class CsvTestDataGen {

  private static String[] iso = {
      "Time,root.fit.p.s1,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3",
      "1970-01-01T08:00:00.001+08:00,,1,pass,1,1", "1970-01-01T08:00:00.002+08:00,,2,pass,,",
      "1970-01-01T08:00:00.003+08:00,,3,pass,,", "1970-01-01T08:00:00.004+08:00,4,,,4,4"};
  private static String[] defaultLong = {
      "Time,root.fit.p.s1,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3",
      "1,,1,pass,1,1",
      "2,,2,pass,,", "1970-01-01T08:00:00.003+08:00,,3,pass,,", "3,4,,,4,4"};
  private static String[] userSelfDefine = {
      "Time,root.fit.p.s1,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3",
      "1971,,1,pass,1,1",
      "1972,,2,pass,,", "1973-01-01T08:00:00.003+08:00,,3,pass,,", "1974,4,,,4,4"};
  private static BufferedWriter bw = null;

  /**
   * generate iso.csv data.
   *
   * @return path
   */
  public static String isoDataGen() {
    String path = System.getProperties().getProperty("user.dir") + "/src/test/resources/iso.csv";
    File file = new File(path);

    try {
      if (!file.exists()) {
        file.createNewFile();
      }
      bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
      for (String str : iso) {
        bw.write(str + "\n");
      }
      bw.flush();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      try {
        bw.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return path;
  }

  /**
   * generate default long data file: defaultLong.csv .
   *
   * @return path
   */
  public static String defaultLongDataGen() {
    String path =
        System.getProperties().getProperty("user.dir") + "/src/test/resources/defaultLong.csv";
    File file = new File(path);
    try {
      if (!file.exists()) {
        file.createNewFile();
      }
      bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
      for (String str : defaultLong) {
        bw.write(str + "\n");
      }
      bw.flush();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      try {
        bw.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return path;
  }

  /**
   * generate user defined data: userSelfDefine.csv .
   *
   * @return path
   */
  public static String userSelfDataGen() {
    String path =
        System.getProperties().getProperty("user.dir") + "/src/test/resources/userSelfDefine.csv";
    File file = new File(path);
    try {
      if (!file.exists()) {
        file.createNewFile();
      }
      bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
      for (String str : userSelfDefine) {
        bw.write(str + "\n");
      }
      bw.flush();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      try {
        bw.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return path;
  }

  public static void main(String[] args) {
    System.out.println(defaultLongDataGen());
  }

}
