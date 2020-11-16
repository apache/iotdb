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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExportCsvTestIT extends AbstractScript{

  private final String SQL_FILE = "target" + File.separator + "sql.txt";

  private final String EXPORT_FILE = "target" + File.separator + "dump0.csv";

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
  protected void testOnWindows() throws IOException {
    final String[] output = {
        "------------------------------------------",
        "Starting IoTDB Client Export Script",
        "------------------------------------------",
        "Start to export data from sql statement: select * from root",
        "Statement [select * from root] has dumped to file",
    };
    String dir = getCliPath();
    ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c",
        dir + File.separator + "tools" + File.separator + "export-csv.bat",
        "-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root", "-td", "./target",
        "-s", SQL_FILE);
    testOutput(builder, output);
  }

  @Override
  protected void testOnUnix() throws IOException {
    final String[] output = {
        "------------------------------------------",
        "Starting IoTDB Client Export Script",
        "------------------------------------------",
        "Start to export data from sql statement: select * from root",
        "Statement [select * from root] has dumped to file",
    };
    String dir = getCliPath();
    ProcessBuilder builder = new ProcessBuilder("sh",
        dir + File.separator + "tools" + File.separator + "export-csv.sh",
        "-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root", "-td", "./target",
        "-s", SQL_FILE);
    testOutput(builder, output);
  }

  private boolean generateSQLFile() {
    String[] sql = {
        "select * from root"};
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
  public void test() throws IOException, StatementExecutionException, IoTDBConnectionException {
    final String[] expectCsv = new String[]{"Time,root.sg1.d1.s1",
        "1970-01-01T08:00:00.001+08:00,1.0"};
    prepareData();
    String os = System.getProperty("os.name").toLowerCase();
    assertTrue(generateSQLFile());
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
    FileReader fileReader = new FileReader(EXPORT_FILE);
    BufferedReader br = new BufferedReader(fileReader);
    String line = br.readLine();
    int i = 0;
    while(line != null) {
      assertEquals(expectCsv[i], line);
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

    List<String> values = new ArrayList<>();
    values.add("1.0");
    session.insertRecord(deviceId, 1L, measurements, values);
  }

}
