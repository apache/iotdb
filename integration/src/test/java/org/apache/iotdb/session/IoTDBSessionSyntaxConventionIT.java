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
package org.apache.iotdb.session;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBSessionSyntaxConventionIT {

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void createTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String storageGroup = "root.sg";
    session.setStorageGroup(storageGroup);

    try {
      session.createTimeseries(
          "root.sg.d1.\"a.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("is not a legal path"));
    }

    try {
      session.createTimeseries(
          "root.sg.d1.a\".s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("is not a legal path"));
    }

    final SessionDataSet dataSet = session.executeQueryStatement("SHOW TIMESERIES");
    assertFalse(dataSet.hasNext());

    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void testInsert() throws Exception {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("a.b");
    measurements.add("a\".\"b");
    measurements.add("\"a.b");

    List<String> values = new ArrayList<>();
    for (int i = 0; i < measurements.size(); i++) {
      values.add("1");
    }

    try {
      session.insertRecord(deviceId, 1L, measurements, values);
      fail();
    } catch (Exception ignored) {

    }

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    Assert.assertFalse(dataSet.hasNext());

    measurements.clear();
    measurements.add("\"a.b\"");
    measurements.add("\"a“（Φ）”b\"");
    measurements.add("\"a>b\"");
    measurements.add("'a.b'");
    measurements.add("'a“（Φ）”b'");
    measurements.add("'a>b'");
    measurements.add("a“（Φ）”b");
    measurements.add("a>b");
    measurements.add("\\\"a");

    values.clear();
    for (int i = 0; i < measurements.size(); i++) {
      values.add("1");
    }

    session.insertRecord(deviceId, 1L, measurements, values);

    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.\"a.b\""));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.\"a“（Φ）”b\""));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.\"a>b\""));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.'a.b'"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.'a“（Φ）”b'"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.'a>b'"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.`a“（Φ）”b`"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.`a>b`"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.`\\\"a`"));

    session.close();
  }
}
