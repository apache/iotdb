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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.commons.utils.function.CheckedTriConsumer;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.receiver.transform.converter.ValueConverter;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeTypeConversionISessionIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  private static final int generateDataSize = 1000;

  @Test
  public void insertTablet() {
    prepareTypeConversionTest(
        (ITableSession senderSession, ITableSession receiverSession, Tablet tablet) -> {
          senderSession.insert(tablet);
        },
        false);
  }

  @Test
  @Ignore("The receiver conversion is currently banned, will ignore conflict")
  public void insertTabletReceiveByTsFile() {
    prepareTypeConversionTest(
        (ITableSession senderSession, ITableSession receiverSession, Tablet tablet) -> {
          senderSession.insert(tablet);
        },
        true);
  }

  private SessionDataSet query(
      ITableSession session, List<IMeasurementSchema> measurementSchemas, String tableName)
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = "select ";
    StringBuilder param = new StringBuilder();
    for (IMeasurementSchema schema : measurementSchemas) {
      param.append(schema.getMeasurementName());
      param.append(',');
    }

    sql = sql + param + "time from " + tableName + " ORDER BY time ASC";
    session.executeNonQueryStatement("use test");
    return session.executeQueryStatement(sql);
  }

  private void prepareTypeConversionTest(
      CheckedTriConsumer<ITableSession, ITableSession, Tablet, Exception> executeDataWriteOperation,
      boolean isTsFile) {
    List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    Tablet tablet = generateTabletAndMeasurementSchema(measurementSchemas, "test");
    createDatabaseAndTable(measurementSchemas, true, tablet.getColumnTypes(), senderEnv);
    createDatabaseAndTable(measurementSchemas, false, tablet.getColumnTypes(), receiverEnv);
    try (ITableSession senderSession = senderEnv.getTableSessionConnection();
        ITableSession receiverSession = receiverEnv.getTableSessionConnection()) {
      senderSession.executeNonQueryStatement("use test");
      receiverSession.executeNonQueryStatement("use test");
      if (isTsFile) {
        // Send TsFile data to receiver
        executeDataWriteOperation.accept(senderSession, receiverSession, tablet);
        senderSession.executeNonQueryStatement("flush");
        createDataPipe(true);
      } else {
        // Send Tablet data to receiver
        // Write once to create data regions, guarantee that no any tsFiles will be sent
        executeDataWriteOperation.accept(senderSession, receiverSession, tablet);
        createDataPipe(false);
        // The actual implementation logic of inserting data
        executeDataWriteOperation.accept(senderSession, receiverSession, tablet);
        senderSession.executeNonQueryStatement("flush");
      }

      final Consumer<String> handleFailure =
          o -> {
            TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          };
      // Verify receiver data
      long timeoutSeconds = 600;
      List<List<Object>> expectedValues =
          generateTabletResultSetForTable(tablet, measurementSchemas);
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try {
                  validateResultSet(
                      query(receiverSession, tablet.getSchemas(), tablet.getTableName()),
                      expectedValues,
                      tablet.getTimestamps());
                } catch (Exception | Error e) {
                  handleFailure.accept(e.getMessage());
                  fail(e.getMessage());
                }
              });
    } catch (Exception e) {
      fail(e.getMessage());
    }
    tablet.reset();
  }

  private void createDatabaseAndTable(
      List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas,
      boolean isLeft,
      List<ColumnCategory> categories,
      BaseEnv env) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      final MeasurementSchema measurement =
          isLeft ? measurementSchemas.get(i).getLeft() : measurementSchemas.get(i).getRight();
      builder.append(
          String.format(
              "%s %s %s,",
              measurement.getMeasurementName(), measurement.getType(), categories.get(i).name()));
    }
    builder.deleteCharAt(builder.length() - 1);
    String tableCreation =
        String.format("create table if not exists test (%s)", builder.toString());
    TestUtils.executeNonQueries(
        null,
        "table",
        env,
        Arrays.asList("create database if not exists test", "use test", tableCreation),
        null);
  }

  private void createDataPipe(boolean isTSFile) {
    String sql =
        String.format(
            "create pipe test"
                + " with source ('source'='iotdb-source','realtime.mode'='%s','history.enable'='%s')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s:%s','batch.enable'='false','sink.format'='%s')",
            isTSFile ? "file" : "forced-log",
            isTSFile,
            receiverEnv.getIP(),
            receiverEnv.getPort(),
            isTSFile ? "tsfile" : "tablet");
    TestUtils.executeNonQueries(
        null, BaseEnv.TABLE_SQL_DIALECT, senderEnv, Collections.singletonList(sql), null);
  }

  private void validateResultSet(
      SessionDataSet dataSet, List<List<Object>> values, long[] timestamps)
      throws IoTDBConnectionException, StatementExecutionException {
    int index = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      List<Object> rowValues = values.get(index++);
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        if (field.getDataType() == null) {
          assertNull(rowValues.get(i));
          continue;
        }
        switch (field.getDataType()) {
          case INT64:
          case TIMESTAMP:
            assertEquals(field.getLongV(), (long) rowValues.get(i));
            break;
          case DATE:
            assertEquals(field.getDateV(), rowValues.get(i));
            break;
          case BLOB:
          case TEXT:
          case STRING:
            assertEquals(field.getBinaryV(), rowValues.get(i));
            break;
          case INT32:
            assertEquals(field.getIntV(), (int) rowValues.get(i));
            break;
          case DOUBLE:
            assertEquals(0, Double.compare(field.getDoubleV(), (double) rowValues.get(i)));
            break;
          case FLOAT:
            assertEquals(0, Float.compare(field.getFloatV(), (float) rowValues.get(i)));
            break;
        }
      }
    }
    assertEquals(values.size(), index);
  }

  private void createTestDataForBoolean(Tablet tablet, int j) {
    Random random = new Random();
    for (int i = 0; i < generateDataSize; i++) {
      if (random.nextBoolean()) {
        tablet.addValue(i, j, random.nextBoolean());
      }
    }
  }

  private void createTestDataForInt32(Tablet tablet, int j) {
    Random random = new Random();
    for (int i = 0; i < generateDataSize; i++) {
      if (random.nextBoolean()) {
        tablet.addValue(i, j, random.nextInt());
      }
    }
  }

  private void createTestDataForInt64(Tablet tablet, int j) {
    Random random = new Random();
    for (int i = 0; i < generateDataSize; i++) {
      if (random.nextBoolean()) {
        tablet.addValue(i, j, random.nextLong());
      }
    }
  }

  private void createTestDataForFloat(Tablet tablet, int j) {
    Random random = new Random();
    for (int i = 0; i < generateDataSize; i++) {
      if (random.nextBoolean()) {
        tablet.addValue(i, j, random.nextFloat());
      }
    }
  }

  private void createTestDataForDouble(Tablet tablet, int j) {
    Random random = new Random();
    for (int i = 0; i < generateDataSize; i++) {
      if (random.nextBoolean()) {
        tablet.addValue(i, j, random.nextDouble());
      }
    }
  }

  private void createTestDataForTimestamp(Tablet tablet, int j) {
    Random random = new Random();
    long time = new Date().getTime();
    for (int i = 0; i < generateDataSize; i++) {
      if (random.nextBoolean()) {
        tablet.addValue(i, j, time);
      }
    }
  }

  private void createTestDataForTimeColumn(Tablet tablet) {
    long time = new Date().getTime();
    for (int i = 0; i < generateDataSize; i++) {
      tablet.addTimestamp(i, time++);
    }
  }

  private void createTestDataForDate(Tablet tablet, int j) {
    int year = 2025;
    int month = 1;
    int day = 1;
    for (int i = 0; i < generateDataSize; i++) {
      tablet.addValue(i, j, DateUtils.parseIntToLocalDate(year * 10000 + (month * 100) + day));
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
  }

  private void createTestDataForString(Tablet tablet, int j) {
    String[] stringData = {
      "Hello",
      "Hello World!",
      "This is a test.",
      "IoTDB Hello World!!!!",
      "IoTDB is an excellent time series database!!!!!!!!!",
      "12345678910!!!!!!!!",
      "123456",
      "1234567.123213",
      "21232131.21",
      "enable =  true",
      "true",
      "false",
    };
    for (int i = 0; i < generateDataSize; i++) {
      tablet.addValue(
          i, j, stringData[(i % stringData.length)].getBytes(TSFileConfig.STRING_CHARSET));
    }
  }

  private List<List<Object>> generateTabletResultSetForTable(
      final Tablet tablet, List<Pair<MeasurementSchema, MeasurementSchema>> pairs) {
    List<List<Object>> insertRecords = new ArrayList<>(tablet.getRowSize());
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.getValues();
    for (int i = 0; i < tablet.getRowSize(); i++) {
      List<Object> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        TSDataType sourceType = pairs.get(j).left.getType();
        TSDataType targetType = pairs.get(j).right.getType();
        Object value = null;
        switch (sourceType) {
          case INT64:
          case TIMESTAMP:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((long[]) values[j])[i];
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
          case INT32:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((int[]) values[j])[i];
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
          case DOUBLE:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((double[]) values[j])[i];
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
          case FLOAT:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((float[]) values[j])[i];
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
          case DATE:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((LocalDate[]) values[j])[i];
            if (targetType == TSDataType.DATE) {
              insertRecord.add(value);
              break;
            }
            if (value != null) {
              value = DateUtils.parseDateExpressionToInt((LocalDate) value);
            }
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((Binary[]) values[j])[i];
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
          case BOOLEAN:
            value = tablet.getBitMaps()[j].isMarked(i) ? null : ((boolean[]) values[j])[i];
            if (targetType.isCompatible(sourceType)) {
              value = targetType.castFromSingleValue(sourceType, value);
            } else {
              value = ValueConverter.convert(sourceType, targetType, value);
              value = convert(value, targetType);
            }
            insertRecord.add(value);
            break;
        }
      }
      insertRecord.add(tablet.getTimestamp(i));
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private Object convert(Object value, TSDataType targetType) {
    if (value == null) {
      return null;
    }
    switch (targetType) {
      case DATE:
        return DateUtils.parseIntToLocalDate((Integer) value);
      case TEXT:
      case STRING:
        return value;
    }
    return value;
  }

  private Tablet generateTabletAndMeasurementSchema(
      List<Pair<MeasurementSchema, MeasurementSchema>> pairs, String tableName) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (Pair<MeasurementSchema, MeasurementSchema> pair : pairs) {
      schemaList.add(pair.left);
    }

    final List<ColumnCategory> columnTypes = generateTabletColumnCategory(pairs.size());
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            generateDataSize);
    tablet.initBitMaps();
    createTestDataForTimeColumn(tablet);
    for (int i = 0; i < pairs.size(); i++) {
      MeasurementSchema schema = pairs.get(i).left;
      switch (schema.getType()) {
        case INT64:
          createTestDataForInt64(tablet, i);
          break;
        case INT32:
          createTestDataForInt32(tablet, i);
          break;
        case TIMESTAMP:
          createTestDataForTimestamp(tablet, i);
          break;
        case DOUBLE:
          createTestDataForDouble(tablet, i);
          break;
        case FLOAT:
          createTestDataForFloat(tablet, i);
          break;
        case DATE:
          createTestDataForDate(tablet, i);
          break;
        case STRING:
        case BLOB:
        case TEXT:
          createTestDataForString(tablet, i);
          break;
        case BOOLEAN:
          createTestDataForBoolean(tablet, i);
          break;
      }
    }

    return tablet;
  }

  private List<ColumnCategory> generateTabletColumnCategory(int size) {
    List<ColumnCategory> columnTypes = new ArrayList<>(size);
    columnTypes.add(ColumnCategory.TAG);
    columnTypes.add(ColumnCategory.TAG);
    columnTypes.add(ColumnCategory.TAG);
    columnTypes.add(ColumnCategory.TAG);
    for (int i = 0; i < size - 4; i++) {
      columnTypes.add(ColumnCategory.FIELD);
    }
    return columnTypes;
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

    for (int t = 0; t < 4; t++) {
      pairs.add(
          new Pair<>(
              new MeasurementSchema("s" + t, TSDataType.STRING),
              new MeasurementSchema("s" + t, TSDataType.STRING)));
    }

    for (TSDataType type : dataTypes) {
      for (TSDataType dataType : dataTypes) {
        String id = String.format("%s2%s", type.name(), dataType.name());
        pairs.add(new Pair<>(new MeasurementSchema(id, type), new MeasurementSchema(id, dataType)));
      }
    }
    return pairs;
  }
}
