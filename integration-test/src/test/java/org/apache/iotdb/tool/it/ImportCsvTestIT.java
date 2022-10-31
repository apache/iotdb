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

package org.apache.iotdb.tool.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class ImportCsvTestIT extends AbstractScript {

  private final String[] noDataOutput = {
    "````````````````````````````````````````````````",
    "Starting IoTDB Client Import Script",
    "````````````````````````````````````````````````",
    "Start to import data from: test.csv",
    "No records!"
  };

  private final String[] noHeaderOutput = {
    "````````````````````````````````````````````````",
    "Starting IoTDB Client Import Script",
    "````````````````````````````````````````````````",
    "No headers!"
  };

  private final String[] emptyOutput = {
    "````````````````````````````````````````````````",
    "Starting IoTDB Client Import Script",
    "````````````````````````````````````````````````",
    "Empty file!"
  };

  @Before
  public void setUp() throws Exception {
    // start an IotDB server environment
    EnvFactory.getEnv().initBeforeTest();
    // choose an execute command by system.
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      command =
          new String[] {
            "cmd.exe",
            "/c",
            getCliPath() + File.separator + "tools" + File.separator + "import-csv.bat"
          };
    } else {
      command =
          new String[] {
            "sh", getCliPath() + File.separator + "tools" + File.separator + "import-csv.sh"
          };
    }
  }

  @After
  public void tearDown() throws Exception {
    // shutdown IotDB server environment
    EnvFactory.getEnv().cleanAfterTest();
  }
  /**
   * test the situation that the schema has not been created and CSV file has no records
   *
   * @throws java.io.IOException
   */
  @Test
  public void testImportNoRecordsCSV() throws IOException {
    assertTrue(generateTestCSV(false, false, true, false, false));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, noDataOutput);
    File file = new File(CSV_FILE);
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * test the situation that the CSV file has no headers
   *
   * @throws java.io.IOException
   */
  @Test
  public void testNoHeader() throws IOException {
    assertTrue(generateTestCSV(false, true, false, false, false));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, noHeaderOutput);
  }

  /**
   * test the situation that the CSV file is an empty file
   *
   * @throws java.io.IOException
   */
  @Test
  public void testEmptyCSV() throws IOException {
    assertTrue(generateTestCSV(true, false, false, false, false));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, emptyOutput);
  }

  /**
   * test the situation that the schema has been created and CSV file has no problem
   *
   * @throws java.io.IOException
   */
  @Test
  public void test() throws IOException {
    assertTrue(generateTestCSV(false, false, false, false, false));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, null);
    File file = new File(CSV_FILE);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from root.**")) {
      testResult(resultSet, 6, 3);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * test the situation that the schema has been created and CSV file has no problem
   *
   * @throws java.io.IOException
   */
  @Test
  public void testAligned() throws IOException {
    assertTrue(generateTestCSV(false, false, false, false, false));
    String[] params = {"-f", CSV_FILE, "-aligned "};
    testMethod(params, null);
    File file = new File(CSV_FILE);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show devices")) {
      while (resultSet.next()) {
        assertEquals("true", resultSet.getString(2));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * test the situation that the schema has not been created and CSV file has no problem
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWithoutCreateSchema() throws IOException {
    assertTrue(generateTestCSV(false, false, false, false, false));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, null);
    File file = new File(CSV_FILE);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from root.**")) {
      testResult(resultSet, 6, 3);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * test the situation that the schema has not been created and CSV file has no data type
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWithDataType() throws IOException {
    assertTrue(generateTestCSV(false, false, false, false, true));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, null);
    File file = new File(CSV_FILE);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from root.**")) {
      testResult(resultSet, 6, 3);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * test the situation that the schema has not been created and CSV file has no data type
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWithException() throws IOException {
    assertTrue(generateTestCSV(false, false, false, true, true));
    String[] params = {"-f", CSV_FILE};
    testMethod(params, null);
    File file = new File(CSV_FILE);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select ** from root")) {
      testResult(resultSet, 6, 3);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (file.exists()) {
      file.delete();
    }
    // check the failed file
    List<CSVRecord> records = readCsvFile(CSV_FILE + ".failed_0").getRecords();
    String[] realRecords = {
      "Time,root.fit.d1.s1(INT32),root.fit.d1.s2(TEXT),root.fit.d2.s1(INT32),root.fit.d2.s3(INT32),root.fit.p.s1(INT32)",
      "1,100,\"hello\",200,\"300\",400"
    };
    for (int i = 0; i < records.size(); i++) {
      String record = StringUtils.join(records.get(i).toList(), ',');
      assertEquals(realRecords[i], record);
    }
  }

  /**
   * test whether the shape of data is correct
   *
   * @throws java.io.IOException
   */
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

  /**
   * generate the test CSV file by setting parameters
   *
   * @param isEmpty
   * @param isHeaderEmpty
   * @param isRecordsEmpty
   * @param isException
   * @param dataType
   * @return
   */
  private boolean generateTestCSV(
      Boolean isEmpty,
      Boolean isHeaderEmpty,
      Boolean isRecordsEmpty,
      Boolean isException,
      Boolean dataType) {
    String[] csvText;
    if (isEmpty) {
      csvText = new String[] {};
    } else {
      if (isHeaderEmpty) {
        csvText =
            new String[] {
              "1,100,\"hello\",200,300,400",
              "2,500,\"\",600,700,800",
              "3,900,\"Io\"TDB\",1000,1100,1200"
            };
      } else {
        if (isRecordsEmpty) {
          csvText =
              new String[] {
                "Time,root.fit.d1.s1(INT32),root.fit.d1.s2(TEXT),root.fit.d2.s1(INT32),root.fit.d2.s3(INT32),root.fit.p.s1(INT32)"
              };
        } else {
          if (dataType) {
            if (isException) {
              csvText =
                  new String[] {
                    "Time,root.fit.d1.s1(INT32),root.fit.d1.s2(TEXT),root.fit.d2.s1(INT32),root.fit.d2.s3(INT32),root.fit.p.s1(INT32)",
                    "1,100,\"hello\",200,\"300\",400",
                    "2,500,\"\",600,700,800",
                    "3,900,\"Io\"TDB\",1000,1100,1200"
                  };
            } else {
              csvText =
                  new String[] {
                    "Time,root.fit.d1.s1(INT32),root.fit.d1.s2(TEXT),root.fit.d2.s1(INT32),root.fit.d2.s3(INT32),root.fit.p.s1(INT32)",
                    "1,100,\"hello\",200,300,400",
                    "2,500,\"\",600,700,800",
                    "3,900,\"Io\"TDB\",1000,1100,1200"
                  };
            }
          } else {
            if (isException) {
              csvText =
                  new String[] {
                    "Time,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3,root.fit.p.s1",
                    "1,100,\"hello\",200,\"300\",400",
                    "2,500,\"\",600,700,800",
                    "3,900,\"Io\"TDB\",1000,1100,1200"
                  };
            } else {
              csvText =
                  new String[] {
                    "Time,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3,root.fit.p.s1",
                    "1,100,\"hello\",200,300,400",
                    "2,500,\"\",600,700,800",
                    "3,900,\"Io\"TDB\",1000,1100,1200"
                  };
            }
          }
        }
      }
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
}
