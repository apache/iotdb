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

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeTypeConversionISession extends AbstractPipeDualManualIT {
  private static final int generateDataSize = 100;

  // Test for converting BOOLEAN to OtherType
  @Test
  public void insertTablet() {
    prepareTypeConversionTest(
        (ISession session, Tablet tablet) -> {
          try {
            session.insertTablet(tablet);

            validateResultSet(
                query(session, tablet.getSchemas()),
                generateTabletInsertRecordForTable(tablet),
                tablet.timestamps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        false);
  }

  @Test
  public void insertAlignedTablet() {
    prepareTypeConversionTest(
        (ISession session, Tablet tablet) -> {
          try {
            session.insertAlignedTablet(tablet);

            validateResultSet(
                query(session, tablet.getSchemas()),
                generateTabletInsertRecordForTable(tablet),
                tablet.timestamps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        false);
  }

  @Test
  public void insertRecords() {
    prepareTypeConversionTest(
        (ISession session, Tablet tablet) -> {
          try {
            List<Long> timestamps = getTimestampList(tablet);
            Pair<List<List<String>>, List<List<TSDataType>>> pair =
                getMeasurementSchemasAndType(tablet);
            List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
            session.insertRecords(getDeviceID(tablet), timestamps, pair.left, pair.right, values);

            validateResultSet(
                query(session, tablet.getSchemas()),
                generateTabletInsertRecordForTable(tablet),
                tablet.timestamps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        false);
  }

  @Test
  public void insertAlignedRecords() {
    prepareTypeConversionTest(
        (ISession session, Tablet tablet) -> {
          try {
            List<Long> timestamps = getTimestampList(tablet);
            Pair<List<List<String>>, List<List<TSDataType>>> pair =
                getMeasurementSchemasAndType(tablet);
            List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
            session.insertAlignedRecords(
                getDeviceID(tablet), timestamps, pair.left, pair.right, values);

            validateResultSet(
                query(session, tablet.getSchemas()),
                generateTabletInsertRecordForTable(tablet),
                tablet.timestamps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        false);
  }

  @Test
  public void insertStringRecordsOfOneDevice() {
    prepareTypeConversionTest(
        (ISession session, Tablet tablet) -> {
          try {
            List<Long> timestamps = getTimestampList(tablet);
            Pair<List<List<String>>, List<List<TSDataType>>> pair =
                getMeasurementSchemasAndType(tablet);
            List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
            session.insertStringRecordsOfOneDevice(
                tablet.getDeviceId(), timestamps, pair.left, values);

            validateResultSet(
                query(session, tablet.getSchemas()),
                generateTabletInsertRecordForTable(tablet),
                tablet.timestamps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        false);
  }

  @Test
  public void insertAlignedStringRecordsOfOneDevice() {
    prepareTypeConversionTest(
        (ISession session, Tablet tablet) -> {
          try {
            List<Long> timestamps = getTimestampList(tablet);
            Pair<List<List<String>>, List<List<TSDataType>>> pair =
                getMeasurementSchemasAndType(tablet);
            List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
            session.insertAlignedStringRecordsOfOneDevice(
                tablet.getDeviceId(), timestamps, pair.left, values);

            validateResultSet(
                query(session, tablet.getSchemas()),
                generateTabletInsertRecordForTable(tablet),
                tablet.timestamps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        false);
  }

  private SessionDataSet query(ISession session, List<IMeasurementSchema> measurementSchemas)
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = "select ";
    StringBuffer param = new StringBuffer();
    for (IMeasurementSchema schema : measurementSchemas) {
      param.append(schema.getMeasurementId());
      param.append(',');
    }
    sql = sql + param.substring(0, param.length() - 1);
    sql = sql + " from root.test.** ORDER BY time ASC";
    return session.executeQueryStatement(sql);
  }

  private void prepareTypeConversionTest(
      BiConsumer<ISession, Tablet> biConsumer, boolean isTsFile) {
    List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    for (Pair<MeasurementSchema, MeasurementSchema> pair : measurementSchemas) {
      createTimeSeries(pair.left.getMeasurementId(), pair.left.getType().name(), senderEnv);
      createTimeSeries(pair.right.getMeasurementId(), pair.right.getType().name(), receiverEnv);
    }

    if (isTsFile) {
      Tablet tablet = generateTabletAndMeasurementSchema(measurementSchemas, "root.test");
      try {
        biConsumer.accept(senderEnv.getSessionConnection(), tablet);
        Thread.sleep(2000);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      createDataPipe(true);
    } else {
      createDataPipe(false);
      try {
        Thread.sleep(2000);
        Tablet tablet = generateTabletAndMeasurementSchema(measurementSchemas, "root.test");
        biConsumer.accept(senderEnv.getSessionConnection(), tablet);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void createTimeSeries(String measurementID, String dataType, BaseEnv env) {
    String timeSeriesCreationQuery =
        String.format(
            "create timeseries root.test.%s with datatype=%s,encoding=PLAIN",
            measurementID, dataType);
    TestUtils.tryExecuteNonQueriesWithRetry(
        env, Collections.singletonList(timeSeriesCreationQuery));
  }

  private void createDataPipe(boolean isTSFile) {
    String sql =
        String.format(
            "create pipe test"
                + " with source ('source'='iotdb-source','source.path'='root.test.**','realtime.mode'='forced-log','realtime.enable'='true','history.enable'='false')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s:%s','batch.enable'='false','sink.format'='%s')",
            receiverEnv.getIP(), receiverEnv.getPort(), isTSFile ? "tsfile" : "tablet");
    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList(sql));
  }

  private void insertTablet(ISession session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    session.insertTablet(tablet);
  }

  private void insertAlignedTablet(ISession session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    session.insertAlignedTablet(tablet);
  }

  private void insertRecords(ISession session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timestamps = getTimestampList(tablet);
    Pair<List<List<String>>, List<List<TSDataType>>> pair = getMeasurementSchemasAndType(tablet);
    List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
    session.insertRecords(getDeviceID(tablet), timestamps, pair.left, pair.right, values);
  }

  private void insertAlignedRecords(ISession session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timestamps = getTimestampList(tablet);
    Pair<List<List<String>>, List<List<TSDataType>>> pair = getMeasurementSchemasAndType(tablet);
    List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
    session.insertAlignedRecords(getDeviceID(tablet), timestamps, pair.left, pair.right, values);
  }

  private void insertStrRecords(ISession session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timestamps = getTimestampList(tablet);
    Pair<List<List<String>>, List<List<TSDataType>>> pair = getMeasurementSchemasAndType(tablet);
    List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
    session.insertStringRecordsOfOneDevice(tablet.getDeviceId(), timestamps, pair.left, values);
  }

  private void insertAlignedStrRecords(ISession session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    List<Long> timestamps = getTimestampList(tablet);
    Pair<List<List<String>>, List<List<TSDataType>>> pair = getMeasurementSchemasAndType(tablet);
    List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
    session.insertAlignedStringRecordsOfOneDevice(
        tablet.getDeviceId(), timestamps, pair.left, values);
  }

  private void validateResultSet(
      SessionDataSet dataSet, List<List<Object>> values, long[] timestamps)
      throws IoTDBConnectionException, StatementExecutionException {
    int index = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();

      Assert.assertEquals(record.getTimestamp(), timestamps[index]);
      List<Object> rowValues = values.get(index++);

      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        switch (field.getDataType()) {
          case INT64:
          case TIMESTAMP:
            Assert.assertEquals(field.getLongV(), (long) rowValues.get(i));
            break;
          case DATE:
            Assert.assertEquals(field.getDateV(), rowValues.get(i));
            break;
          case BLOB:
            Assert.assertEquals(field.getBinaryV(), rowValues.get(i));
            break;
          case TEXT:
          case STRING:
            Assert.assertEquals(field.getStringValue(), rowValues.get(i));
            break;
          case INT32:
            Assert.assertEquals(field.getIntV(), (int) rowValues.get(i));
            break;
          case DOUBLE:
            Assert.assertEquals(0, Double.compare(field.getDoubleV(), (double) rowValues.get(i)));
            break;
          case FLOAT:
            Assert.assertEquals(0, Float.compare(field.getFloatV(), (float) rowValues.get(i)));
            break;
        }
      }
    }
  }

  private boolean[] createTestDataForBoolean() {
    boolean[] data = new boolean[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextBoolean();
    }
    return data;
  }

  private int[] createTestDataForInt32() {
    int[] data = new int[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt();
    }
    return data;
  }

  private long[] createTestDataForInt64() {
    long[] data = new long[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextLong();
    }
    return data;
  }

  private float[] createTestDataForFloat() {
    float[] data = new float[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextFloat();
    }
    return data;
  }

  private double[] createTestDataForDouble() {
    double[] data = new double[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextDouble();
    }
    return data;
  }

  private long[] createTestDataForTimestamp() {
    long[] data = new long[generateDataSize];
    long time = new Date().getTime();
    for (int i = 0; i < data.length; i++) {
      data[i] = time + i;
    }
    return data;
  }

  private int[] createTestDataForDate() {
    int[] data = new int[generateDataSize];
    int year = 2023;
    int month = 1;
    int day = 1;
    for (int i = 0; i < data.length; i++) {
      data[i] = year * 10000 + (month * 100) + day;
      // update
      day++;
      if (day > 28) {
        day = 1;
        month++;
        if (month > 12) {
          month = 1;
          year++;
        }
      }
    }
    return data;
  }

  private Binary[] createTestDataForString() {
    String[] stringData = {
      "Hello",
      "Hello World!",
      "This is a test.",
      "IoTDB Hello World!!!!",
      "IoTDB is an excellent time series database!!!!!!!!!"
    };
    Binary[] data = new Binary[generateDataSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = new Binary(stringData[(i % data.length)].getBytes(TSFileConfig.STRING_CHARSET));
    }
    return data;
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
    List<List<String>> schemaData = new ArrayList<>(tablet.rowSize);
    List<List<TSDataType>> typeData = new ArrayList<>(tablet.rowSize);
    List<String> measurementSchemas = new ArrayList<>(tablet.getSchemas().size());
    List<TSDataType> types = new ArrayList<>(tablet.rowSize);
    for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
      measurementSchemas.add(measurementSchema.getMeasurementId());
      types.add(measurementSchema.getType());
    }

    for (int i = 0; i < tablet.rowSize; i++) {
      schemaData.add(measurementSchemas);
      typeData.add(types);
    }

    return new Pair<>(schemaData, typeData);
  }

  private List<String> getDeviceID(Tablet tablet) {
    List<String> data = new ArrayList<>(tablet.rowSize);
    for (int i = 0; i < tablet.rowSize; i++) {
      data.add(tablet.getDeviceId());
    }
    return data;
  }

  private List<List<Object>> generateTabletInsertRecordForTable(final Tablet tablet) {
    List<List<Object>> insertRecords = new ArrayList<>(tablet.rowSize);
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.rowSize; i++) {
      List<Object> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        switch (schemas.get(j).getType()) {
          case INT64:
          case TIMESTAMP:
            insertRecord.add(((long[]) values[i])[j]);
            break;
          case INT32:
            insertRecord.add(((int[]) values[i])[j]);
            break;
          case DOUBLE:
            insertRecord.add(((double[]) values[i])[j]);
            break;
          case FLOAT:
            insertRecord.add(((float[]) values[i])[j]);
            break;
          case DATE:
            insertRecord.add(DateUtils.parseIntToDate(((int[]) values[i])[j]));
            break;
          case TEXT:
          case STRING:
            insertRecord.add(
                new String(((Binary[]) values[i])[j].getValues(), TSFileConfig.STRING_CHARSET));
            break;
          case BLOB:
            insertRecord.add(((Binary[]) values[i])[j]);
            break;
          case BOOLEAN:
            insertRecord.add(((boolean[]) values[i])[j]);
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private List<List<String>> generateTabletInsertStrRecordForTable(Tablet tablet) {
    List<List<String>> insertRecords = new ArrayList<>(tablet.rowSize);
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.rowSize; i++) {
      List<String> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        switch (schemas.get(j).getType()) {
          case INT64:
            insertRecord.add(String.valueOf(((long[]) values[i])[j]));
            break;
          case TIMESTAMP:
            insertRecord.add(
                RpcUtils.formatDatetime(
                    "default", "ms", ((long[]) values[i])[j], ZoneId.systemDefault()));
            break;
          case INT32:
            insertRecord.add(String.valueOf(((int[]) values[i])[j]));
            break;
          case DOUBLE:
            insertRecord.add(String.valueOf(((double[]) values[i])[j]));
            break;
          case FLOAT:
            insertRecord.add(String.valueOf(((float[]) values[i])[j]));
            break;
          case DATE:
            insertRecord.add(DateUtils.formatDate(((int[]) values[i])[j]));
            break;
          case TEXT:
          case STRING:
            insertRecord.add(
                new String(((Binary[]) values[i])[j].getValues(), TSFileConfig.STRING_CHARSET));
            break;
          case BLOB:
            insertRecord.add(
                BytesUtils.parseBlobByteArrayToString(((Binary[]) values[i])[j].getValues()));
            break;
          case BOOLEAN:
            insertRecord.add(String.valueOf(((boolean[]) values[i])[j]));
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private Tablet generateTabletAndMeasurementSchema(
      List<Pair<MeasurementSchema, MeasurementSchema>> pairs, String deviceId) {
    long[] timestamp = createTestDataForTimestamp();
    Object[] objects = new Object[pairs.size()];
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(pairs.size());
    List<MeasurementSchema> measurementSchemaList = new ArrayList<>(pairs.size());
    BitMap[] bitMaps = new BitMap[generateDataSize];
    List<List<MeasurementSchema>> result = new ArrayList<>(generateDataSize);
    for (int i = 0; i < bitMaps.length; i++) {
      result.add(measurementSchemaList);
      bitMaps[i] = new BitMap(pairs.size());
    }
    List<Tablet.ColumnType> columnTypes = new ArrayList<>(pairs.size());
    for (int i = 0; i < objects.length; i++) {
      MeasurementSchema schema = pairs.get(i).left;
      measurementSchemas.add(schema);
      measurementSchemaList.add(schema);
      columnTypes.add(Tablet.ColumnType.MEASUREMENT);
      switch (pairs.get(i).right.getType()) {
        case INT64:
          objects[i] = createTestDataForInt64();
          break;
        case INT32:
          objects[i] = createTestDataForInt32();
          break;
        case TIMESTAMP:
          objects[i] = createTestDataForTimestamp();
          break;
        case DOUBLE:
          objects[i] = createTestDataForDouble();
          break;
        case FLOAT:
          objects[i] = createTestDataForFloat();
          break;
        case DATE:
          objects[i] = createTestDataForDate();
          break;
        case STRING:
        case BLOB:
        case TEXT:
          objects[i] = createTestDataForString();
          break;
        case BOOLEAN:
          objects[i] = createTestDataForBoolean();
          break;
      }
    }
    return new Tablet(
        deviceId, measurementSchemas, columnTypes, timestamp, objects, bitMaps, generateDataSize);
  }

  private List<Pair<MeasurementSchema, MeasurementSchema>> generateMeasurementSchemas() {
    TSDataType[] dataTypes = {
      TSDataType.STRING,
      TSDataType.TEXT,
      TSDataType.BLOB,
      TSDataType.TIMESTAMP,
      TSDataType.BOOLEAN,
      TSDataType.DATE,
      TSDataType.DOUBLE,
      TSDataType.FLOAT,
      TSDataType.INT32,
      TSDataType.INT64
    };
    List<Pair<MeasurementSchema, MeasurementSchema>> pairs = new ArrayList<>();

    for (TSDataType type : dataTypes) {
      for (TSDataType dataType : dataTypes) {
        String id = String.format("%s2%s", type.name(), dataType.name());
        pairs.add(new Pair<>(new MeasurementSchema(id, type), new MeasurementSchema(id, dataType)));
      }
    }
    return pairs;
  }
}
