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

package org.apache.iotdb.cross.tests.tools.tsfile;

import org.apache.iotdb.cross.tests.tools.importCsv.AbstractScript;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExportTsFileTestIT extends AbstractScript {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      command =
          new String[] {
            "cmd.exe",
            "/c",
            getCliPath() + File.separator + "tools" + File.separator + "export-tsfile.bat"
          };
    } else {
      command =
          new String[] {
            "sh", getCliPath() + File.separator + "tools" + File.separator + "export-tsfile.sh"
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
    String[] params = {"-td", "target", "-q", "select * from root.test.t2"};
    prepareData();
    testMethod(params, null);
    QueryDataSet dataSet = readTsFile("target/dump0.tsfile");
    String[] realRecords = {"1.0,bbbbb,abbes"};
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      String join = StringUtils.join(rowRecord.getFields(), ",");
      assertEquals(realRecords[i++], join);
    }
  }

  private static QueryDataSet readTsFile(String path) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("root.test.t2", "c1"));
      paths.add(new Path("root.test.t2", "c2"));
      paths.add(new Path("root.test.t2", "c3"));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      return readTsFile.query(queryExpression);
    }
  }

  private void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    Session session = null;
    try {
      session = new Session("127.0.0.1", 6667, "root", "root");
      session.open();

      String deviceId = "root.test.t2";
      List<String> measurements = new ArrayList<>();
      measurements.add("c1");
      measurements.add("c2");
      measurements.add("c3");

      List<String> values = new ArrayList<>();
      values.add("1.0");
      values.add("bbbbb");
      values.add("abbes");
      session.insertRecord(deviceId, 1L, measurements, values);

    } finally {
      if (session != null) {
        session.close();
      }
    }
  }
}
