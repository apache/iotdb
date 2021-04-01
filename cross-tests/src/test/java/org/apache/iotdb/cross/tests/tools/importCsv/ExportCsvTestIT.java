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

package org.apache.iotdb.cross.tests.tools.importCsv;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportCsvTestIT extends AbstractScript {

  private final String SQL_FILE = "target" + File.separator + "sql.txt";

  private final String EXPORT_FILE = "target" + File.separator + "dump0.csv";

  private final String[] output = {
    "------------------------------------------",
    "Starting IoTDB Client Export Script",
    "------------------------------------------",
    "Start to export data from sql statement",
    "successfully",
  };

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Override
  protected void testOnWindows(String[] output) throws IOException {

    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "tools" + File.separator + "export-csv.bat",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-td",
            "./target",
            "-s",
            SQL_FILE);
    testOutput(builder, output);
  }

  @Override
  protected void testOnUnix(String[] output) throws IOException {

    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "tools" + File.separator + "export-csv.sh",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-td",
            "./target",
            "-s",
            SQL_FILE);
    testOutput(builder, output);
  }

  private boolean generateSQLFile(String[] sql) {
    BufferedWriter writer;
    try {
      writer = new BufferedWriter(new FileWriter(SQL_FILE));
      writer.write("");
      for (String s : sql) {
        writer.write(s);
        writer.newLine();
      }
      writer.flush();
      writer.close();
      return true;
    } catch (IOException e) {
      System.out.println("failed to create test csv");
    }
    return false;
  }

  @Test
  public void testRawDataQuery()
      throws IOException, StatementExecutionException, IoTDBConnectionException {
    final String[] expectCsv =
        new String[] {
          "Time,root.sg1.d1.s3,root.sg1.d1.s1,root.sg1.d1.s2", "abbe's,1.0,\"\\\"abc\\\",aa\""
        };
    prepareData();
    String os = System.getProperty("os.name").toLowerCase();
    String[] sql = {"select * from root"};
    assertTrue(generateSQLFile(sql));
    if (os.startsWith("windows")) {
      testOnWindows(output);
    } else {
      testOnUnix(output);
    }
    FileReader fileReader = new FileReader(EXPORT_FILE);
    BufferedReader br = new BufferedReader(fileReader);
    String line = br.readLine();
    int i = 0;
    while (line != null) {
      if (i == 0) {
        assertEquals(expectCsv[i], line);
      } else {
        String lineWithoutTime = line.substring(line.indexOf(',') + 1);
        assertEquals(expectCsv[i], lineWithoutTime);
      }
      i++;
      line = br.readLine();
    }
    File file = new File(EXPORT_FILE);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testAggregationQuery()
      throws StatementExecutionException, IoTDBConnectionException, IOException {
    final String[] expectCsv =
        new String[] {
          "Time,count(root.sg1.d1.s3),count(root.sg1.d1.s1),count(root.sg1.d1.s2)", "1,1,1"
        };
    prepareData();
    String os = System.getProperty("os.name").toLowerCase();
    String[] sql = {"select count(*) from root"};
    generateSQLFile(sql);
    if (os.startsWith("windows")) {
      testOnWindows(output);
    } else {
      testOnUnix(output);
    }
    FileReader fileReader = new FileReader(EXPORT_FILE);
    BufferedReader br = new BufferedReader(fileReader);
    String line = br.readLine();
    int i = 0;
    while (line != null) {
      if (i == 0) {
        assertEquals(expectCsv[i], line);
      } else {
        String lineWithoutTime = line.substring(line.indexOf(',') + 1);
        assertEquals(expectCsv[i], lineWithoutTime);
      }
      i++;
      line = br.readLine();
    }
    File file = new File(EXPORT_FILE);
    if (file.exists()) {
      file.delete();
    }
  }

  private void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    List<String> values = new ArrayList<>();
    values.add("1.0");
    values.add("\"abc\",aa");
    values.add("abbe's");
    session.insertRecord(deviceId, 1L, measurements, values);
  }
}
