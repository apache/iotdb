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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SessionConcurrentExample {

  private static final int sgNum = 20;
  private static final int deviceNum = 100;
  private static final int parallelDegreeForOneSG = 3;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);
    createTemplate(session);
    session.close();

    CountDownLatch latch = new CountDownLatch(sgNum * parallelDegreeForOneSG);
    ExecutorService es = Executors.newFixedThreadPool(sgNum * parallelDegreeForOneSG);

    for (int i = 0; i < sgNum * parallelDegreeForOneSG; i++) {
      int currentIndex = i;
      es.execute(() -> concurrentOperation(latch, currentIndex));
    }

    es.shutdown();

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void concurrentOperation(CountDownLatch latch, int currentIndex) {

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open(false);
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }

    for (int j = 0; j < deviceNum; j++) {
      try {
        insertTablet(
            session, String.format("root.sg_%d.d_%d", currentIndex / parallelDegreeForOneSG, j));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        e.printStackTrace();
      }
    }

    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }

    latch.countDown();
  }

  private static void createTemplate(Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Collections.singletonList("s3"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }
    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s1");
    schemaNames.add("s2");
    schemaNames.add("s3");

    session.createSchemaTemplate(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
    for (int i = 0; i < sgNum; i++) {
      session.setSchemaTemplate("template1", "root.sg_" + i);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet(Session session, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
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
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(deviceId, schemaList, 100);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
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

    // Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }
}
