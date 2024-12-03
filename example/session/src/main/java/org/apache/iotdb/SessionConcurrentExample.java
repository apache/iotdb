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

import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SessionConcurrentExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionConcurrentExample.class);

  private static final int SG_NUM = 20;
  private static final int DEVICE_NUM = 100;
  private static final int PARALLEL_DEGREE_FOR_ONE_SG = 3;
  private static Random random = new Random();

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, IOException {

    try (Session session = new Session("127.0.0.1", 6667, "root", "root")) {
      session.open(false);
      createTemplate(session);
    } catch (Exception e) {
      LOGGER.error("create template with Session error", e);
    }

    CountDownLatch latch = new CountDownLatch(SG_NUM * PARALLEL_DEGREE_FOR_ONE_SG);
    ExecutorService es = Executors.newFixedThreadPool(SG_NUM * PARALLEL_DEGREE_FOR_ONE_SG);

    for (int i = 0; i < SG_NUM * PARALLEL_DEGREE_FOR_ONE_SG; i++) {
      int currentIndex = i;
      es.execute(() -> concurrentOperation(latch, currentIndex));
    }

    es.shutdown();

    try {
      latch.await();
    } catch (InterruptedException e) {
      LOGGER.warn("CountDownLatch interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private static void concurrentOperation(CountDownLatch latch, int currentIndex) {

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open(false);
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Open Session error", e);
    }

    for (int j = 0; j < DEVICE_NUM; j++) {
      try {
        insertTablet(
            session,
            String.format("root.sg_%d.d_%d", currentIndex / PARALLEL_DEGREE_FOR_ONE_SG, j));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("Insert tablet error", e);
      }
    }

    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Close session error", e);
    }

    latch.countDown();
  }

  private static void createTemplate(Session session)
      throws IoTDBConnectionException, StatementExecutionException, IOException {

    Template template = new Template("template1", false);
    MeasurementNode mNodeS1 =
        new MeasurementNode("s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS2 =
        new MeasurementNode("s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    MeasurementNode mNodeS3 =
        new MeasurementNode("s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    template.addToTemplate(mNodeS1);
    template.addToTemplate(mNodeS2);
    template.addToTemplate(mNodeS3);

    session.createSchemaTemplate(template);
    for (int i = 0; i < SG_NUM; i++) {
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
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(deviceId, schemaList, 100);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = random.nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
      }
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.getRowSize() != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, time);
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.getRowSize() != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }
}
