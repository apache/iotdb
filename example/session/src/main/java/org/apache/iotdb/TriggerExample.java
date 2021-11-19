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

package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SuppressWarnings("squid:S106")
public class TriggerExample {

  private static Session session;

  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";

  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder().host(LOCAL_HOST).port(6667).username("root").password("root").build();
    session.open(false);

    createTimeseries();

    createTrigger();

    insertTablet();

    query();

    dropTrigger();

    session.close();
  }

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
      session.createTimeseries(
          ROOT_SG1_D1_S1, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
    }
  }

  private static void createTrigger() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "CREATE TRIGGER moving_extreme "
            + "AFTER INSERT "
            + "ON root.sg1.d1.s1 "
            + "AS 'org.apache.iotdb.db.engine.trigger.builtin.MovingExtremeTrigger'"
            + "WITH ("
            + "  'device' = 'root.extreme.sg1.d1', "
            + "  'measurement' = 's1'"
            + ")");
  }

  private static void dropTrigger() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("drop trigger moving_extreme");
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.DOUBLE));

    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 100000);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 10000000; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, new Random().nextDouble());

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet dataSet = session.executeQueryStatement("select ** from root")) {
      System.out.println(dataSet.getColumnNames());
      while (dataSet.hasNext()) {
        System.out.println(dataSet.next());
      }
    }
  }
}
