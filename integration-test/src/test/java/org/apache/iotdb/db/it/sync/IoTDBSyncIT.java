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

package org.apache.iotdb.db.it.sync;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSyncIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSyncIT.class);

  private static final long waitTime = 500L;

  private static final String PIPE_SINK_NAME = "sink";
  private static final String PIPE_NAME = "pipe";
  private static final String IP = "127.0.0.1";
  private static final String PORT = "6667";
  private BaseEnv senderEnv;
  private BaseEnv receiverEnv;

  @Before
  public void setUp() throws Exception {
    senderEnv = EnvFactory.createNewEnv();
    receiverEnv = EnvFactory.createNewEnv();
    senderEnv.initBeforeTest();
    receiverEnv.initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    senderEnv.cleanAfterTest();
    receiverEnv.cleanAfterTest();
  }

  @Test
  public void testSync() {
    try (Connection connection = senderEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipesink %s as IoTDB(ip='%s', port=%s)",
              PIPE_SINK_NAME, IP, receiverEnv.getConnectionPort()));
      statement.execute(String.format("create pipe %s to %s", PIPE_NAME, PIPE_SINK_NAME));
      statement.execute(String.format("start pipe %s", PIPE_NAME));
      SchemaConfig.registerSchema(statement);

      statement.execute(
          String.format(
              "insert into %s(time, %s, %s, %s, %s) values(1, 1, 1, 1.0, \"1\")",
              SchemaConfig.DEVICE_0,
              SchemaConfig.MEASUREMENT_00.getMeasurementId(),
              SchemaConfig.MEASUREMENT_01.getMeasurementId(),
              SchemaConfig.MEASUREMENT_02.getMeasurementId(),
              SchemaConfig.MEASUREMENT_03.getMeasurementId()));
      statement.execute("flush");

      Thread.sleep(waitTime);
      statement.execute(String.format("drop pipe %s", PIPE_NAME));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

    try (Connection connection = receiverEnv.getConnection();
        Statement statement = connection.createStatement()) {
      Thread.sleep(waitTime);

      //      try (ResultSet resultSet =
      //          statement.executeQuery(
      //              String.format(
      //                  "select %s from %s",
      //                  SchemaConfig.MEASUREMENT_00.getMeasurementId(), SchemaConfig.DEVICE_0))) {
      try (ResultSet resultSet = statement.executeQuery("show storage group")) {
        while (resultSet.next()) {
          System.out.println(resultSet.getString("storage group"));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public static class SchemaConfig {
    private static final String STORAGE_GROUP_0 = "root.sg.test_0";
    private static final String STORAGE_GROUP_1 = "root.sg.test_1";

    // device 0, nonaligned, sg 0
    private static final String DEVICE_0 = "root.sg.test_0.d_0";
    private static final MeasurementSchema MEASUREMENT_00 =
        new MeasurementSchema("sensor_00", TSDataType.INT32, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_01 =
        new MeasurementSchema("sensor_01", TSDataType.INT64, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_02 =
        new MeasurementSchema("sensor_02", TSDataType.DOUBLE, TSEncoding.GORILLA);
    private static final MeasurementSchema MEASUREMENT_03 =
        new MeasurementSchema("sensor_03", TSDataType.TEXT, TSEncoding.PLAIN);

    // device 1, aligned, sg 0
    private static final String DEVICE_1 = "root.sg.test_0.a_1";
    private static final MeasurementSchema MEASUREMENT_10 =
        new MeasurementSchema("sensor_10", TSDataType.INT32, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_11 =
        new MeasurementSchema("sensor_11", TSDataType.INT64, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_12 =
        new MeasurementSchema("sensor_12", TSDataType.DOUBLE, TSEncoding.GORILLA);
    private static final MeasurementSchema MEASUREMENT_13 =
        new MeasurementSchema("sensor_13", TSDataType.TEXT, TSEncoding.PLAIN);

    // device 2, non aligned, sg 1
    private static final String DEVICE_2 = "root.sg.test_1.d_2";
    private static final MeasurementSchema MEASUREMENT_20 =
        new MeasurementSchema("sensor_20", TSDataType.INT32, TSEncoding.RLE);

    // device 3, non aligned, sg 1
    private static final String DEVICE_3 = "root.sg.test_1.d_3";
    private static final MeasurementSchema MEASUREMENT_30 =
        new MeasurementSchema("sensor_30", TSDataType.INT32, TSEncoding.RLE);

    // device 4, aligned, sg 1
    private static final String DEVICE_4 = "root.sg.test_1.a_4";
    private static final MeasurementSchema MEASUREMENT_40 =
        new MeasurementSchema("sensor_40", TSDataType.INT32, TSEncoding.RLE);

    private static void registerSchema(Statement statement) throws SQLException {
      statement.execute("set storage group to " + STORAGE_GROUP_0);
      statement.execute("set storage group to " + STORAGE_GROUP_1);

      statement.execute(convert2SQL(DEVICE_0, MEASUREMENT_00));
      statement.execute(convert2SQL(DEVICE_0, MEASUREMENT_01));
      statement.execute(convert2SQL(DEVICE_0, MEASUREMENT_02));
      statement.execute(convert2SQL(DEVICE_0, MEASUREMENT_03));

      statement.execute(
          convert2AlignedSQL(
              DEVICE_1,
              Arrays.asList(MEASUREMENT_10, MEASUREMENT_11, MEASUREMENT_12, MEASUREMENT_13)));

      statement.execute(convert2SQL(DEVICE_2, MEASUREMENT_20));

      statement.execute(convert2SQL(DEVICE_3, MEASUREMENT_30));

      statement.execute(convert2AlignedSQL(DEVICE_4, Collections.singletonList(MEASUREMENT_40)));
    }

    private static String convert2SQL(String device, MeasurementSchema schema) {
      String sql =
          String.format(
              "create timeseries %s %s",
              new Path(device, schema.getMeasurementId()).getFullPath(), schema.getType().name());
      LOGGER.info(String.format("schema execute: %s.", sql));
      return sql;
    }

    private static String convert2AlignedSQL(String device, List<MeasurementSchema> schemas) {
      String sql = String.format("create aligned timeseries %s(", device);
      for (int i = 0; i < schemas.size(); i++) {
        MeasurementSchema schema = schemas.get(i);
        sql += (String.format("%s %s", schema.getMeasurementId(), schema.getType().name()));
        sql += (i == schemas.size() - 1 ? ")" : ",");
      }
      LOGGER.info(String.format("schema execute: %s.", sql));
      return sql;
    }

    private static void deleteSG(Statement statement) throws SQLException {
      statement.execute(String.format("delete storage group %s", STORAGE_GROUP_0));
      statement.execute(String.format("delete storage group %s", STORAGE_GROUP_1));
    }
  }
}
