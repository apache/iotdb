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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.commons.utils.function.CheckedTriConsumer;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeReqAutoSliceIT extends AbstractPipeDualManualIT {
  private static final int generateDataSize = 10;

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(4);
    receiverEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(4);
  }

  @Test
  public void insertTablet() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          senderSession.insertTablet(tablet);
        },
        false);
  }

  @Ignore
  @Test
  public void insertTabletReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          senderSession.insertTablet(tablet);
        },
        true);
  }

  @Ignore
  @Test
  public void insertAlignedTablet() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          senderSession.insertAlignedTablet(tablet);
        },
        false);
  }

  @Ignore
  @Test
  public void insertAlignedTabletReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          senderSession.insertAlignedTablet(tablet);
        },
        true);
  }

  @Ignore
  @Test
  public void insertRecordsReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          senderSession.insertRecords(
              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
        },
        true);
  }

  @Ignore
  @Test
  public void insertRecord() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          for (int i = 0; i < values.size(); i++) {
            senderSession.insertRecord(
                tablet.getDeviceId(),
                timestamps.get(i),
                pair.left.get(i),
                pair.right.get(i),
                values.get(i).toArray());
          }
        },
        false);
  }

  @Ignore
  @Test
  public void insertRecordReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          for (int i = 0; i < values.size(); i++) {
            senderSession.insertRecord(
                tablet.getDeviceId(),
                timestamps.get(i),
                pair.left.get(i),
                pair.right.get(i),
                values.get(i).toArray());
          }
        },
        true);
  }

  @Ignore
  @Test
  public void insertAlignedRecord() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          for (int i = 0; i < values.size(); i++) {
            senderSession.insertAlignedRecord(
                tablet.getDeviceId(),
                timestamps.get(i),
                pair.left.get(i),
                pair.right.get(i),
                values.get(i));
          }
        },
        false);
  }

  @Ignore
  @Test
  public void insertAlignedRecordReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          for (int i = 0; i < values.size(); i++) {
            senderSession.insertAlignedRecord(
                tablet.getDeviceId(),
                timestamps.get(i),
                pair.left.get(i),
                pair.right.get(i),
                values.get(i));
          }
        },
        true);
  }

  @Ignore
  @Test
  public void insertRecords() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          senderSession.insertRecords(
              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
        },
        false);
  }

  @Ignore
  @Test
  public void insertAlignedRecords() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          senderSession.insertAlignedRecords(
              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
        },
        false);
  }

  @Ignore
  @Test
  public void insertAlignedRecordsReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
          senderSession.insertAlignedRecords(
              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
        },
        true);
  }

  @Ignore
  @Test
  public void insertStringRecordsOfOneDevice() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
          senderSession.insertStringRecordsOfOneDevice(
              tablet.getDeviceId(), timestamps, pair.left, values);
        },
        false);
  }

  @Ignore
  @Test
  public void insertStringRecordsOfOneDeviceReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
          senderSession.insertStringRecordsOfOneDevice(
              tablet.getDeviceId(), timestamps, pair.left, values);
        },
        true);
  }

  @Ignore
  @Test
  public void insertAlignedStringRecordsOfOneDevice() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
          senderSession.insertAlignedStringRecordsOfOneDevice(
              tablet.getDeviceId(), timestamps, pair.left, values);
        },
        false);
  }

  @Ignore
  @Test
  public void insertAlignedStringRecordsOfOneDeviceReceiveByTsFile() {
    prepareReqAutoSliceTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          List<Long> timestamps = getTimestampList(tablet);
          Pair<List<List<String>>, List<List<TSDataType>>> pair =
              getMeasurementSchemasAndType(tablet);
          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
          senderSession.insertAlignedStringRecordsOfOneDevice(
              tablet.getDeviceId(), timestamps, pair.left, values);
        },
        true);
  }

  private void prepareReqAutoSliceTest(
      CheckedTriConsumer<ISession, ISession, Tablet, Exception> consumer, boolean isTsFile) {
    Tablet tablet = createTablet();
    createTimeSeries();
    try (ISession senderSession = senderEnv.getSessionConnection();
        ISession receiverSession = receiverEnv.getSessionConnection()) {
      if (isTsFile) {
        consumer.accept(senderSession, receiverSession, tablet);
        senderSession.executeNonQueryStatement("flush");
        Thread.sleep(2000);
        createPipe(senderSession, true);
      } else {
        createPipe(senderSession, false);
        Thread.sleep(2000);
        consumer.accept(senderSession, receiverSession, tablet);
        senderSession.executeNonQueryStatement("flush");
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    verify(tablet);
  }

  private void createPipe(ISession session, boolean isTsFile)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        String.format(
            "create pipe test"
                + " with source ('source'='iotdb-source','source.path'='root.test.**')"
                + " with sink ('sink'='iotdb-thrift-sync-sink','node-urls'='%s:%s','batch.enable'='false','sink.format'='%s')",
            receiverEnv.getIP(), receiverEnv.getPort(), isTsFile ? "tsfile" : "tablet"));
  }

  private int[] createTestDataForInt32() {
    int[] data = new int[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < generateDataSize; i++) {
      data[i] = random.nextInt();
    }
    return data;
  }

  private long[] createTestDataForInt64() {
    long[] data = new long[generateDataSize];
    long time = System.currentTimeMillis();
    for (int i = 0; i < generateDataSize; i++) {
      data[i] = time + i;
    }
    return data;
  }

  private void verify(Tablet tablet) {
    HashSet<String> set = new HashSet<>();
    for (int i = 0; i < generateDataSize; i++) {
      set.add(
          String.format(
              "%d,%d,%d,",
              tablet.timestamps[i], ((int[]) tablet.values[0])[i], ((int[]) tablet.values[1])[i]));
    }
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.test.** ORDER BY time ASC",
        "Time,root.test.db.temperature,root.test.db.status,",
        set,
        20);
  }

  private void createTimeSeries() {
    List<String> timeSeriesCreation =
        Arrays.asList(
            "create timeseries root.test.db.status with datatype=INT32,encoding=PLAIN",
            "create timeseries root.test.db.temperature with datatype=INT32,encoding=PLAIN");
    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, timeSeriesCreation);
    TestUtils.tryExecuteNonQueriesWithRetry(receiverEnv, timeSeriesCreation);
  }

  private Tablet createTablet() {
    long[] timestamp = createTestDataForInt64();
    int[] temperature = createTestDataForInt32();
    int[] status = createTestDataForInt32();

    Object[] objects = new Object[2];
    objects[0] = temperature;
    objects[1] = status;

    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(2);
    measurementSchemas.add(new MeasurementSchema("temperature", TSDataType.INT32));
    measurementSchemas.add(new MeasurementSchema("status", TSDataType.INT32));

    BitMap[] bitMaps = new BitMap[2];
    for (int i = 0; i < bitMaps.length; i++) {
      bitMaps[i] = new BitMap(generateDataSize);
    }

    return new Tablet(
        "root.test.db", measurementSchemas, timestamp, objects, bitMaps, generateDataSize);
  }

  private List<Long> getTimestampList(Tablet tablet) {
    long[] timestamps = tablet.timestamps;
    List<Long> data = new ArrayList<>(timestamps.length);
    for (long timestamp : timestamps) {
      data.add(timestamp);
    }
    return data;
  }

  private Pair<List<List<String>>, List<List<TSDataType>>> getMeasurementSchemasAndType(
      Tablet tablet) {
    List<List<String>> schemaData = new ArrayList<>(tablet.getRowSize());
    List<List<TSDataType>> typeData = new ArrayList<>(tablet.getRowSize());
    List<String> measurementSchemas = new ArrayList<>(tablet.getSchemas().size());
    List<TSDataType> types = new ArrayList<>(tablet.getRowSize());
    for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
      measurementSchemas.add(measurementSchema.getMeasurementName());
      types.add(measurementSchema.getType());
    }

    for (int i = 0; i < tablet.getRowSize(); i++) {
      schemaData.add(measurementSchemas);
      typeData.add(types);
    }

    return new Pair<>(schemaData, typeData);
  }

  private List<String> getDeviceID(Tablet tablet) {
    List<String> data = new ArrayList<>(tablet.getRowSize());
    for (int i = 0; i < tablet.getRowSize(); i++) {
      data.add(tablet.getDeviceId());
    }
    return data;
  }

  private List<List<Object>> generateTabletInsertRecordForTable(final Tablet tablet) {
    List<List<Object>> insertRecords = new ArrayList<>(tablet.getRowSize());
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.getRowSize(); i++) {
      List<Object> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        switch (schemas.get(j).getType()) {
          case INT64:
          case TIMESTAMP:
            insertRecord.add(((long[]) values[j])[i]);
            break;
          case INT32:
            insertRecord.add(((int[]) values[j])[i]);
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private List<List<String>> generateTabletInsertStrRecordForTable(Tablet tablet) {
    List<List<String>> insertRecords = new ArrayList<>(tablet.getRowSize());
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.getRowSize(); i++) {
      List<String> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        switch (schemas.get(j).getType()) {
          case INT64:
            insertRecord.add(String.valueOf(((long[]) values[j])[i]));
            break;
          case INT32:
            insertRecord.add(String.valueOf(((int[]) values[j])[i]));
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }
}
