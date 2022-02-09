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

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExportCsvTestIT extends AbstractScript {

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      command =
          new String[] {
            "cmd.exe",
            "/c",
            getCliPath() + File.separator + "tools" + File.separator + "export-csv.bat"
          };
    } else {
      command =
          new String[] {
            "sh", getCliPath() + File.separator + "tools" + File.separator + "export-csv.sh"
          };
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testExport()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String[] params = {"-td", "target/", "-q", "select c1,c2,c3 from root.test.t1"};
    prepareData();
    testMethod(params, null);
    CSVParser parser = readCsvFile("target/dump0.csv");
    String[] realRecords = {
      "root.test.t1.c1,root.test.t1.c2,root.test.t1.c3", "1.0,\"\"abc\",aa\",\"abbe's\""
    };
    List<CSVRecord> records = parser.getRecords();
    for (int i = 0; i < records.size(); i++) {
      String record = StringUtils.join(records.get(i).toList(), ',');
      record = record.substring(record.indexOf(',') + 1);
      assertEquals(realRecords[i], record);
    }
  }

  @Test
  public void testWithDataType()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String[] params = {
      "-td", "target/", "-datatype", "true", "-q", "select c1,c2,c3 from root.test.t1"
    };
    prepareData();
    testMethod(params, null);
    CSVParser parser = readCsvFile("target/dump0.csv");
    String[] realRecords = {
      "root.test.t1.c1(FLOAT),root.test.t1.c2(TEXT),root.test.t1.c3(TEXT)",
      "1.0,\"\"abc\",aa\",\"abbe's\""
    };
    List<CSVRecord> records = parser.getRecords();
    for (int i = 0; i < records.size(); i++) {
      String record = StringUtils.join(records.get(i).toList(), ',');
      record = record.substring(record.indexOf(',') + 1);
      assertEquals(realRecords[i], record);
    }
  }

  @Test
  public void testAggregationQuery()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String[] params = {
      "-td", "target/", "-q", "select count(c1),count(c2),count(c3) from root.test.t1"
    };
    prepareData();
    testMethod(params, null);
    CSVParser parser = readCsvFile("target/dump0.csv");
    String[] realRecords = {
      "count(root.test.t1.c1),count(root.test.t1.c2),count(root.test.t1.c3)", "1,1,1"
    };
    List<CSVRecord> records = parser.getRecords();
    for (int i = 0; i < records.size(); i++) {
      String record = StringUtils.join(records.get(i).toList(), ',');
      assertEquals(realRecords[i], record);
    }
  }

  private void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.test.t1";
    List<String> measurements = new ArrayList<>();
    measurements.add("c1");
    measurements.add("c2");
    measurements.add("c3");

    List<String> values = new ArrayList<>();
    values.add("1.0");
    values.add("\"abc\",aa");
    values.add("abbe's");
    session.insertRecord(deviceId, 1L, measurements, values);
  }
}
