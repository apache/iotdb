/**
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
package org.apache.iotdb.cli.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

@Deprecated
public class CsvTestDataGen {

  private CsvTestDataGen() {

  }

  private static final String PATHS = "Time,root.fit.p.s1,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2."
      + "s1,root.fit.d2.s3";
  private static String[] iso = {
      PATHS,
      "1970-01-01T08:00:00.001+08:00,,1,pass,1,1", "1970-01-01T08:00:00.002+08:00,,2,pass,,",
      "1970-01-01T08:00:00.003+08:00,,3,pass,,", "1970-01-01T08:00:00.004+08:00,4,,,4,4"};
  private static String[] defaultLong = {
      PATHS,
      "1,,1,pass,1,1",
      "2,,2,pass,,", "1970-01-01T08:00:00.003+08:00,,3,pass,,", "3,4,,,4,4"};
  private static String[] userSelfDefine = {
      PATHS,
      "1971,,1,pass,1,1",
      "1972,,2,pass,,", "1973-01-01T08:00:00.003+08:00,,3,pass,,", "1974,4,,,4,4"};
  private static FileOutputStream fos = null;
  private static OutputStreamWriter osw = null;
  private static BufferedWriter bw = null;
  private static final String USER_DIR = "user.dir";

  /**
   * generate iso.csv data.
   *
   * @return path
   */
  public static String isoDataGen() {
    String path = System.getProperties().getProperty(USER_DIR) + "/src/test/resources/iso.csv";
    File file = new File(path);
    writeDataFrom(file, iso);
    return path;
  }

  /**
   * generate default long data file: defaultLong.csv .
   *
   * @return path
   */
  public static String defaultLongDataGen() {
    String path =
        System.getProperties().getProperty(USER_DIR) + "/src/test/resources/defaultLong.csv";
    File file = new File(path);
    writeDataFrom(file, defaultLong);
    return path;
  }

  /**
   * generate user defined data: userSelfDefine.csv .
   *
   * @return path
   */
  public static String userSelfDataGen() {
    String path =
        System.getProperties().getProperty(USER_DIR) + "/src/test/resources/userSelfDefine.csv";
    File file = new File(path);
    writeDataFrom(file, userSelfDefine);
    return path;
  }

  private static void writeDataFrom(File file, String[] info) {
    try {
      if (!file.exists()) {
        file.createNewFile();
      }
      fos = new FileOutputStream(file);
      osw = new OutputStreamWriter(fos);
      bw = new BufferedWriter(osw);
      for (String str : info) {
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
        osw.close();
        fos.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    System.out.println(defaultLongDataGen());
  }

}
