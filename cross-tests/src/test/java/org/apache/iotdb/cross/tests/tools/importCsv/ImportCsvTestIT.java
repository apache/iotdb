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
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ImportCsvTestIT extends AbstractScript {

  private final String CSV_FILE = "target" + File.separator + "test.csv";

  private static final String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.fit.d1",
        "SET STORAGE GROUP TO root.fit.d2",
        "SET STORAGE GROUP TO root.fit.p",
        "CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE",
        "CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=TEXT,ENCODING=PLAIN",
        "CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE",
        "CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE",
        "CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE",
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

  private static void createSchema() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test() throws IOException, ClassNotFoundException {
    createSchema();
    String os = System.getProperty("os.name").toLowerCase();
    assertTrue(generateTestCSV());
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
    File file = new File(CSV_FILE);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testWithoutCreateSchema() throws IOException, ClassNotFoundException {
    String os = System.getProperty("os.name").toLowerCase();
    assertTrue(generateTestCSV());
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
    File file = new File(CSV_FILE);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      if (statement.execute("select * from root")) {
        ResultSet resultSet = statement.getResultSet();
        testResult(resultSet, 6, 3);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testBigCsvFile() throws IOException, ClassNotFoundException {
    String os = System.getProperty("os.name").toLowerCase();
    assertTrue(generateBigCsvFile());
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
    File file = new File(CSV_FILE);
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      if (statement.execute("select s1 from root.fit.d1")) {
        ResultSet resultSet = statement.getResultSet();
        testResult(resultSet, 2, 25000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
  }

  private static void testResult(
      ResultSet resultSet, int expectedColumnNumber, int expectedRowNumber) throws SQLException {
    if (resultSet != null) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      assertEquals(expectedColumnNumber, columnCount);
      int actualRowNumber = 0;
      while (resultSet.next()) {
        actualRowNumber++;
      }
      assertEquals(expectedRowNumber, actualRowNumber);
    }
  }

  @Test
  public void testImportHeaderCSV() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    assertTrue(generateHeaderTestCSV());
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
    File file = new File(CSV_FILE);
    if (file.exists()) {
      file.delete();
    }
  }

  private boolean generateTestCSV() {
    String[] csvText = {
      "Time,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3,root.fit.p.s1",
      "1,100,'hello',200,300,400",
      "2,500,'',600,700,800",
      "3,900,'Io\"TDB',1000,1100,1200"
    };
    BufferedWriter writer;
    try {
      writer = new BufferedWriter(new FileWriter(CSV_FILE));
      writer.write("");
      for (String s : csvText) {
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

  private boolean generateBigCsvFile() {
    List<String> csvText = new ArrayList<>();
    csvText.add("Time,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1");
    for (int i = 0; i < 25000; i++) {
      csvText.add(i + "," + i + "," + i + "," + i);
    }
    BufferedWriter writer;
    try {
      writer = new BufferedWriter(new FileWriter(CSV_FILE));
      writer.write("");
      for (String s : csvText) {
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

  private boolean generateHeaderTestCSV() {
    String[] csvText = {
      "Time,root.fit.d1.\"s1\",root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3,root.fit.p.s1"
    };
    BufferedWriter writer;
    try {
      writer = new BufferedWriter(new FileWriter(CSV_FILE));
      writer.write("");
      for (String s : csvText) {
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

  @Override
  protected void testOnWindows() throws IOException {
    final String[] output = {
      "````````````````````````````````````````````````",
      "Starting IoTDB Client Import Script",
      "````````````````````````````````````````````````",
      "Start to import data from: test.csv",
      "",
      "Import from: test.csv",
      "Import from: test.csv 100%"
    };
    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            dir + File.separator + "tools" + File.separator + "import-csv.bat",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-f",
            CSV_FILE);
    testOutput(builder, output);
  }

  @Override
  protected void testOnUnix() throws IOException {
    final String[] output = {
      "------------------------------------------",
      "Starting IoTDB Client Import Script",
      "------------------------------------------",
      "Start to import data from: test.csv",
      "",
      "Import from: test.csv",
      "Import from: test.csv 100%"
    };
    String dir = getCliPath();
    ProcessBuilder builder =
        new ProcessBuilder(
            "sh",
            dir + File.separator + "tools" + File.separator + "import-csv.sh",
            "-h",
            "127.0.0.1",
            "-p",
            "6667",
            "-u",
            "root",
            "-pw",
            "root",
            "-f",
            CSV_FILE);
    testOutput(builder, output);
  }
}
