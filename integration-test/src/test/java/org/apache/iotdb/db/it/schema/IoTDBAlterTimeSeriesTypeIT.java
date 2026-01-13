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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.commons.utils.MetadataUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.v4.TsFileTreeWriterBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotSupportedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.relational.it.session.IoTDBSessionRelationalIT.genValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlterTimeSeriesTypeIT {

  private static final Logger log = LoggerFactory.getLogger(IoTDBAlterTimeSeriesTypeIT.class);
  private static long timeout = -1;
  private static final String database = "root.alter";
  public static final List<TSDataType> DATA_TYPE_LIST =
      Arrays.asList(TSDataType.STRING, TSDataType.TEXT, TSDataType.BOOLEAN);
  public static final List<TSDataType> UNSUPPORT_ACCUMULATOR_QUERY_DATA_TYPE_LIST =
      Collections.singletonList(TSDataType.BLOB);

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setCompactionScheduleInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testWriteAndAlter() throws Exception {
    Set<TSDataType> typesToTest = new HashSet<>();
    Collections.addAll(typesToTest, TSDataType.values());
    typesToTest.remove(TSDataType.VECTOR);
    typesToTest.remove(TSDataType.UNKNOWN);

    for (TSDataType from : typesToTest) {
      for (TSDataType to : typesToTest) {
        if (from != to && to.isCompatible(from)) {
          System.out.printf("testing %s to %s%n", from, to);
          doWriteAndAlter(from, to);

          testNonAlignDeviceSequenceDataQuery(from, to);
          testNonAlignDeviceUnSequenceDataQuery(from, to);
          testNonAlignDeviceUnSequenceOverlappedDataQuery(from, to);

          testAlignDeviceSequenceDataQuery(from, to);
          testAlignDeviceUnSequenceDataQuery(from, to);
          testAlignDeviceUnSequenceOverlappedDataQuery(from, to);
        }
      }
    }
  }

  private void doWriteAndAlter(TSDataType from, TSDataType to)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "SET CONFIGURATION \"enable_unseq_space_compaction\"='false'");
      if (from == TSDataType.DATE && !to.isCompatible(from)) {
        throw new NotSupportedException("Not supported DATE type.");
      }

      // create a time series with type of "from"
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".write_and_alter_column_type.s1 " + from);

      // write a sequence tsfile point of "from"
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", from));
      Tablet tablet = new Tablet(database + ".write_and_alter_column_type", schemaList);
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(from, 1));
      session.insertTablet(tablet);
      tablet.reset();
      session.executeNonQueryStatement("FLUSH");

      // write an unsequence tsfile point of "from"
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(from, 1));
      session.insertTablet(tablet);
      tablet.reset();
      session.executeNonQueryStatement("FLUSH");

      // write a sequence memtable point of "from"
      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(from, 2));
      session.insertTablet(tablet);
      tablet.reset();

      // write an unsequence memtable point of "from"
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(from, 1));
      session.insertTablet(tablet);
      tablet.reset();

      try (SessionDataSet dataSet1 =
          session.executeQueryStatement(
              "select s1 from " + database + ".write_and_alter_column_type order by time")) {
        for (int i = 1; i <= 2; i++) {
          RowRecord rec1 = dataSet1.next();
          long time = rec1.getTimestamp();
          Object v = rec1.getFields().get(0).getObjectValue(from);
          assertEquals(time, i);
          assertEquals(v, genValue(from, i));
        }
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES " + database + ".write_and_alter_column_type.s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".write_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select s1 from " + database + ".write_and_alter_column_type order by time");
      RowRecord rec;
      TSDataType newType = isCompatible ? to : from;
      for (int i = 1; i <= 2; i++) {
        rec = dataSet.next();
        if (newType == TSDataType.BLOB) {
          Binary v = rec.getFields().get(0).getBinaryV();
          assertEquals(genValue(newType, i), v);
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec.getFields().get(0).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          if (from == TSDataType.DATE) {
            Binary v = rec.getFields().get(0).getBinaryV();
            assertEquals(new Binary(genValue(from, i).toString(), StandardCharsets.UTF_8), v);
          } else {
            log.info("from is {}", from);
            assertEquals(
                newType.castFromSingleValue(from, genValue(from, i)),
                rec.getFields().get(0).getBinaryV());
          }
        } else {
          assertEquals(genValue(newType, i).toString(), rec.getFields().get(0).toString());
        }
      }

      assertNull(dataSet.next());
      dataSet.close();

      // write an altered point in sequence and unsequnce tsfile
      //      List<IMeasurementSchema> newSchemaList = new ArrayList<>();
      //      newSchemaList.add(new MeasurementSchema("s1", newType));
      //              Arrays.asList(TSDataType.STRING, TSDataType.INT32)
      //      tablet = new Tablet(database + ".write_and_alter_column_type", newSchemaList);

      tablet =
          new Tablet(
              database + ".write_and_alter_column_type",
              Collections.singletonList("s1"),
              Collections.singletonList(to),
              Collections.singletonList(ColumnCategory.FIELD));

      tablet.addTimestamp(0, 3);
      tablet.addValue("s1", 0, genValue(newType, 3));
      session.insertTablet(tablet);
      tablet.reset();

      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(newType, 1));
      session.insertTablet(tablet);
      tablet.reset();
      session.executeNonQueryStatement("FLUSH");

      // write an altered point in sequence and unsequnce memtable
      tablet.addTimestamp(0, 4);
      tablet.addValue("s1", 0, genValue(newType, 4));
      session.insertTablet(tablet);
      tablet.reset();

      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(newType, 2));
      session.insertTablet(tablet);
      tablet.reset();

      log.info("Write completely");

      dataSet =
          session.executeQueryStatement(
              "select s1 from " + database + ".write_and_alter_column_type order by time");
      for (int i = 1; i <= 4; i++) {
        rec = dataSet.next();
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec.getFields().get(0).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec.getFields().get(0).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          assertEquals(genValue(to, i), rec.getFields().get(0).getBinaryV());
        } else {
          assertEquals(genValue(newType, i).toString(), rec.getFields().get(0).toString());
        }
      }
      assertFalse(dataSet.hasNext());

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".write_and_alter_column_type");
        rec = dataSet.next();
        int[] expectedValue = {1, 4};
        for (int i = 0; i < 2; i++) {
          if (newType == TSDataType.BLOB) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getBinaryV());
          } else if (newType == TSDataType.DATE) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getDateV());
          } else {
            assertEquals(
                genValue(newType, expectedValue[i]).toString(), rec.getFields().get(i).toString());
          }
        }
        assertFalse(dataSet.hasNext());
      } else {
        dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".write_and_alter_column_type");
        rec = dataSet.next();
        int[] expectedValue = {1, 4, 1, 4};
        for (int i = 0; i < 4; i++) {
          if (newType == TSDataType.BLOB) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getBinaryV());
          } else if (newType == TSDataType.DATE) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getDateV());
          } else {
            assertEquals(
                genValue(newType, expectedValue[i]).toString(), rec.getFields().get(i).toString());
          }
        }
        assertFalse(dataSet.hasNext());
      }

      if (newType.isNumeric()) {
        dataSet =
            session.executeQueryStatement(
                "select avg(s1),sum(s1) from " + database + ".write_and_alter_column_type");
        rec = dataSet.next();
        assertEquals(2.5, rec.getFields().get(0).getDoubleV(), 0.001);
        assertEquals(10.0, rec.getFields().get(1).getDoubleV(), 0.001);
        assertFalse(dataSet.hasNext());
      }
    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement(
            "DELETE TIMESERIES " + database + ".write_and_alter_column_type.s1");
      }
    }
  }

  @Test
  public void testAlterWithoutWrite() throws IoTDBConnectionException, StatementExecutionException {
    Set<TSDataType> typesToTest = new HashSet<>();
    Collections.addAll(typesToTest, TSDataType.values());
    typesToTest.remove(TSDataType.VECTOR);
    typesToTest.remove(TSDataType.UNKNOWN);

    for (TSDataType from : typesToTest) {
      for (TSDataType to : typesToTest) {
        if (from != to && to.isCompatible(from)) {
          System.out.printf("testing %s to %s%n", from, to);
          doAlterWithoutWrite(from, to, false);
          doAlterWithoutWrite(from, to, true);
        }
      }
    }
  }

  private void doAlterWithoutWrite(TSDataType from, TSDataType to, boolean flush)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (from == TSDataType.DATE && !to.isCompatible(from)) {
        throw new NotSupportedException("Not supported DATE type.");
      }

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".just_alter_column_type.s1 " + from);

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES " + database + ".just_alter_column_type.s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES " + database + ".just_alter_column_type.s1 SET DATA TYPE " + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      TSDataType newType = isCompatible ? to : from;

      // write a point
      Tablet tablet =
          new Tablet(
              database + ".just_alter_column_type",
              Collections.singletonList("s1"),
              Collections.singletonList(newType),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(newType, 1));
      session.insertTablet(tablet);
      tablet.reset();

      if (flush) {
        session.executeNonQueryStatement("FLUSH");
      }

      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select * from " + database + ".just_alter_column_type order by time");
      RowRecord rec = dataSet.next();
      assertEquals(1, rec.getTimestamp());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, 1), rec.getFields().get(0).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, 1), rec.getFields().get(0).getDateV());
      } else {
        assertEquals(genValue(newType, 1).toString(), rec.getFields().get(0).toString());
      }

      assertFalse(dataSet.hasNext());

      session.executeNonQueryStatement(
          "DELETE TIMESERIES " + database + ".just_alter_column_type.**");
    }
  }

  @Test
  public void testAlterNonExist() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES " + database + ".non_exist.s1 SET DATA TYPE INT64");
        fail("Should throw exception");
      } catch (StatementExecutionException e) {
        assertEquals("508: Path [" + database + ".non_exist.s1] does not exist", e.getMessage());
      }
    }
  }

  @Test
  public void testAlterWrongType() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("CREATE TIMESERIES " + database + ".wrong_type.s1 int32");

      try {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES " + database + ".wrong_type.s1 SET DATA TYPE VECTOR");
        fail("Should throw exception");
      } catch (StatementExecutionException e) {
        assertEquals("701: Unsupported datatype: VECTOR", e.getMessage());
      }
    }
  }

  @Test
  public void testContinuousAlter() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".alter_and_alter.s1 int32");

      // time=1 and time=2 are INT32
      Tablet tablet =
          new Tablet(
              database + ".alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(TSDataType.INT32, 1));
      session.insertTablet(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              database + ".alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(TSDataType.INT32, 2));
      session.insertTablet(tablet);
      tablet.reset();

      // time=3 and time=4 are FLOAT
      session.executeNonQueryStatement(
          "ALTER TIMESERIES " + database + ".alter_and_alter.s1 SET DATA TYPE FLOAT");
      tablet =
          new Tablet(
              database + ".alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.FLOAT),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 3);
      tablet.addValue("s1", 0, genValue(TSDataType.FLOAT, 3));
      session.insertTablet(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              database + ".alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.FLOAT),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 4);
      tablet.addValue("s1", 0, genValue(TSDataType.FLOAT, 4));
      session.insertTablet(tablet);
      tablet.reset();

      // time=5 and time=6 are DOUBLE
      session.executeNonQueryStatement(
          "ALTER TIMESERIES " + database + ".alter_and_alter.s1 SET DATA TYPE DOUBLE");
      tablet =
          new Tablet(
              database + ".alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.DOUBLE),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 5);
      tablet.addValue("s1", 0, genValue(TSDataType.DOUBLE, 5));
      session.insertTablet(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              database + ".alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.DOUBLE),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 6);
      tablet.addValue("s1", 0, genValue(TSDataType.DOUBLE, 6));
      session.insertTablet(tablet);
      tablet.reset();

      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select * from " + database + ".alter_and_alter order by time");
      RowRecord rec;
      for (int i = 1; i < 7; i++) {
        rec = dataSet.next();
        assertEquals(i, rec.getTimestamp());
        assertEquals(genValue(TSDataType.DOUBLE, i).toString(), rec.getFields().get(0).toString());
      }
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void testConcurrentWriteAndAlter()
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".concurrent_write_and_alter.s1 int32");
    }

    ExecutorService threadPool = Executors.newCachedThreadPool();
    AtomicInteger writeCounter = new AtomicInteger(0);
    int maxWrite = 10000;
    int flushInterval = 100;
    int alterStart = 5000;
    threadPool.submit(
        () -> {
          try {
            write(writeCounter, maxWrite, flushInterval);
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
          }
        });
    threadPool.submit(
        () -> {
          try {
            alter(writeCounter, alterStart);
          } catch (InterruptedException
              | IoTDBConnectionException
              | StatementExecutionException e) {
            throw new RuntimeException(e);
          }
        });
    threadPool.shutdown();
    assertTrue(threadPool.awaitTermination(1, TimeUnit.MINUTES));

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select count(s1) from " + database + ".concurrent_write_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      assertEquals(maxWrite, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }
  }

  private void write(AtomicInteger writeCounter, int maxWrite, int flushInterval)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      int writtenCnt = 0;
      do {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO "
                    + database
                    + ".concurrent_write_and_alter (time, s1) VALUES (%d, %d)",
                writtenCnt,
                writtenCnt));
        if (((writtenCnt + 1) % flushInterval) == 0) {
          session.executeNonQueryStatement("FLUSH");
        }
      } while ((writtenCnt = writeCounter.incrementAndGet()) < maxWrite);
    }
  }

  private void alter(AtomicInteger writeCounter, int alterStart)
      throws InterruptedException, IoTDBConnectionException, StatementExecutionException {
    while (writeCounter.get() < alterStart) {
      Thread.sleep(10);
    }
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "ALTER TABLE " + database + ".concurrent_write_and_alter.s1 SET DATA TYPE DOUBLE");
    }
  }

  @Test
  public void testLoadAndAlter()
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");
    }

    // file1-file4 s1=INT32

    // file1-file3 single device small range ([1, 1]), may load without split
    List<File> filesToLoad = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      File file = new File("target", "f" + i + ".tsfile");
      try (TsFileTreeWriter tsFileWriter = new TsFileTreeWriterBuilder().file(file).build()) {
        // Register time series schemas
        IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.INT32);

        // Schema for device dId
        tsFileWriter.registerTimeseries(database + ".load_and_alter", schema);

        Tablet tablet =
            new Tablet(
                database + ".load_and_alter",
                Collections.singletonList("s1"),
                Collections.singletonList(TSDataType.INT32),
                Collections.singletonList(ColumnCategory.FIELD));
        tablet.addTimestamp(0, i);
        // tablet.addValue("dId", 0, "d" + i);
        tablet.addValue("s1", 0, i);
        tsFileWriter.write(tablet);
      }
      filesToLoad.add(file);
    }
    // file4 multi device large range ([2, 100_000_000]), load with split
    File file = new File("target", "f" + 4 + ".tsfile");
    try (TsFileTreeWriter tsFileWriter = new TsFileTreeWriterBuilder().file(file).build()) {
      // Register time series schemas
      IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.INT32);

      // Schema for device dId
      tsFileWriter.registerTimeseries(database + ".load_and_alter", schema);

      Tablet tablet =
          new Tablet(
              database + ".load_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      int rowIndex = 0;
      for (int i = 4; i <= 9; i++) {
        tablet.addTimestamp(rowIndex, i);
        tablet.addValue("s1", rowIndex, i);
        rowIndex++;
      }
      tsFileWriter.write(tablet);
    }
    filesToLoad.add(file);

    // load file1-file4
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }
    }
    // check load result
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from " + database + ".load_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      assertEquals(9, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }

    filesToLoad.forEach(
        tsfile -> {
          tsfile.delete();
          File resourceFile = new File(tsfile.getAbsolutePath() + ".resource");
          resourceFile.delete();
        });
    filesToLoad.clear();

    // file5-file8 s1=DOUBLE

    // file5-file7 single device small range ([3, 3]), may load without split
    for (int i = 5; i <= 7; i++) {
      file = new File("target", "f" + i + ".tsfile");
      try (TsFileTreeWriter tsFileWriter = new TsFileTreeWriterBuilder().file(file).build()) {
        // Register time series schemas
        IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.DOUBLE);

        // Schema for device dId
        tsFileWriter.registerTimeseries(database + ".load_and_alter", schema);

        Tablet tablet =
            new Tablet(
                database + ".load_and_alter",
                Collections.singletonList("s1"),
                Collections.singletonList(TSDataType.DOUBLE),
                Collections.singletonList(ColumnCategory.FIELD));
        tablet.addTimestamp(0, i + 5);
        tablet.addValue("s1", 0, 3.0);
        tsFileWriter.write(tablet);
      }
      filesToLoad.add(file);
    }
    // file8 multi device large range ([4, 100_000_001]), load with split
    file = new File("target", "f" + 8 + ".tsfile");
    try (TsFileTreeWriter tsFileWriter = new TsFileTreeWriterBuilder().file(file).build()) {
      // Register time series schemas
      IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.DOUBLE);

      // Schema for device dId
      tsFileWriter.registerTimeseries(database + ".load_and_alter", schema);

      Tablet tablet =
          new Tablet(
              database + ".load_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.DOUBLE),
              Collections.singletonList(ColumnCategory.FIELD));
      int rowIndex = 0;
      for (int i = 8; i <= 10; i++) {
        tablet.addTimestamp(rowIndex, i + 5);
        tablet.addValue("s1", rowIndex, 4.0);
        rowIndex++;
      }
      tsFileWriter.write(tablet);
    }
    filesToLoad.add(file);

    // load file5-file8
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }
    }
    // check load result
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from " + database + ".load_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      // Due to the operation of load tsfile execute directly, don't access memtable or generate
      // InsertNode object, so don't need to check the data type.
      // When query this measurement point, will only find the data of TSDataType.INT32. So this is
      // reason what cause we can't find the data of TSDataType.DOUBLE. So result is 9, is not 15.
      //      assertEquals(15, rec.getFields().get(0).getLongV());
      assertEquals(9, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }

    // alter s1 to double
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "ALTER TIMESERIES " + database + ".load_and_alter.s1 SET DATA TYPE DOUBLE");
    }

    filesToLoad.forEach(
        tsfile -> {
          tsfile.delete();
          File resourceFile = new File(tsfile.getAbsolutePath() + ".resource");
          resourceFile.delete();
        });
    filesToLoad.clear();

    // file9-file12 s1=INT32
    // file9-file11 single device small range ([5, 5]), may load without split
    for (int i = 9; i <= 11; i++) {
      file = new File("target", "f" + i + ".tsfile");
      try (TsFileTreeWriter tsFileWriter = new TsFileTreeWriterBuilder().file(file).build()) {
        // Register time series schemas
        IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.INT32);

        // Schema for device dId
        tsFileWriter.registerTimeseries(database + ".load_and_alter", schema);

        Tablet tablet =
            new Tablet(
                database + ".load_and_alter",
                Collections.singletonList("s1"),
                Collections.singletonList(TSDataType.INT32),
                Collections.singletonList(ColumnCategory.FIELD));
        tablet.addTimestamp(0, i + 7);
        tablet.addValue("s1", 0, 5);
        tsFileWriter.write(tablet);
      }
      filesToLoad.add(file);
    }
    // file12 multi device large range ([6, 100_000_002]), load with split
    file = new File("target", "f" + 12 + ".tsfile");
    try (TsFileTreeWriter tsFileWriter = new TsFileTreeWriterBuilder().file(file).build()) {
      // Register time series schemas
      IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.INT32);

      // Schema for device dId
      tsFileWriter.registerTimeseries(database + ".load_and_alter", schema);

      Tablet tablet =
          new Tablet(
              database + ".load_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      int rowIndex = 0;
      for (int i = 12; i <= 14; i++) {
        tablet.addTimestamp(rowIndex, i + 7);
        tablet.addValue("s1", rowIndex, 6);
        rowIndex++;
      }
      tsFileWriter.write(tablet);
    }
    filesToLoad.add(file);

    // load file9-file12, should succeed
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }
    }
    // check load result
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from " + database + ".load_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      assertEquals(21, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }

    filesToLoad.forEach(
        tsfile -> {
          tsfile.delete();
          File resourceFile = new File(tsfile.getAbsolutePath() + ".resource");
          resourceFile.delete();
        });
    filesToLoad.clear();
  }

  @Test
  public void testLoadAndAccumulator()
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_seq_space_compaction\"=\"false\"");
      statement.execute("set configuration \"enable_unseq_space_compaction\"=\"false\"");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      if (!session.checkTimeseriesExists("root.sg1.d1.s1")) {
        session.createTimeseries(
            "root.sg1.d1.s1", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
      }
      if (!session.checkTimeseriesExists("root.sg1.d1.s2")) {
        session.createTimeseries(
            "root.sg1.d1.s2", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
      }

      Double firstValue = null;
      Double lastValue = null;

      // file1-file3 s1=DOUBLE

      // file1-file3
      List<File> filesToLoad = new ArrayList<>();
      for (int i = 1; i <= 3; i++) {
        File file = new File("target", "f" + i + ".tsfile");
        List<String> columnNames = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
        List<String> columnTypes = Arrays.asList("DOUBLE", "DOUBLE");

        if (file.exists()) {
          Files.delete(file.toPath());
        }

        try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
          // device -> column indices in columnNames
          Map<String, List<Integer>> deviceColumnIndices = new HashMap<>();
          Set<String> alignedDevices = new HashSet<>();
          Map<String, List<IMeasurementSchema>> deviceSchemaMap = new LinkedHashMap<>();
          //          deviceSchemaMap.put("root.sg1.d1", new ArrayList<>());

          collectSchemas(
              session,
              columnNames,
              columnTypes,
              deviceSchemaMap,
              alignedDevices,
              deviceColumnIndices);

          List<Tablet> tabletList = constructTablets(deviceSchemaMap, alignedDevices, tsFileWriter);

          if (!tabletList.isEmpty()) {
            long timestamp = i;
            double dataValue = new Random(10).nextDouble();
            if (firstValue == null) {
              firstValue = dataValue;
            }
            writeWithTablets(
                timestamp,
                dataValue,
                TSDataType.DOUBLE,
                tabletList,
                alignedDevices,
                tsFileWriter,
                deviceColumnIndices);
            tsFileWriter.flush();
          } else {
            fail("No tablets to write");
          }
          tsFileWriter.close();
        }

        filesToLoad.add(file);
      }

      // load file1-file3 into IoTDB
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }

      // clear data
      filesToLoad.forEach(
          tsfile -> {
            tsfile.delete();
            File resourceFile = new File(tsfile.getAbsolutePath() + ".resource");
            resourceFile.delete();
          });
      filesToLoad.clear();

      // check load result
      SessionDataSet dataSet = session.executeQueryStatement("select count(s1) from root.sg1.d1");
      RowRecord rec;
      rec = dataSet.next();
      assertEquals(3, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());

      // file4-file6 s1=INT32

      // file4-file6
      for (int i = 4; i <= 6; i++) {
        File file = new File("target", "f" + i + ".tsfile");
        List<String> columnNames = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2");
        List<String> columnTypes = Arrays.asList("INT32", "INT32");

        if (file.exists()) {
          Files.delete(file.toPath());
        }

        try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
          // device -> column indices in columnNames
          Map<String, List<Integer>> deviceColumnIndices = new HashMap<>();
          Set<String> alignedDevices = new HashSet<>();
          Map<String, List<IMeasurementSchema>> deviceSchemaMap = new LinkedHashMap<>();

          collectSchemas(
              session,
              columnNames,
              columnTypes,
              deviceSchemaMap,
              alignedDevices,
              deviceColumnIndices);

          List<Tablet> tabletList = constructTablets(deviceSchemaMap, alignedDevices, tsFileWriter);

          if (!tabletList.isEmpty()) {
            long timestamp = i;
            int dataValue = new Random(10).nextInt();
            lastValue = Double.valueOf(dataValue);
            writeWithTablets(
                timestamp,
                dataValue,
                TSDataType.INT32,
                tabletList,
                alignedDevices,
                tsFileWriter,
                deviceColumnIndices);
            tsFileWriter.flush();
          } else {
            fail("No tablets to write");
          }
          tsFileWriter.close();
        }

        filesToLoad.add(file);
      }

      // load file4-file6 into IoTDB
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }

      // check load result
      dataSet = session.executeQueryStatement("select count(s1) from root.sg1.d1");
      rec = dataSet.next();
      assertEquals(6, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());

      dataSet = session.executeQueryStatement("select first_value(s1) from root.sg1.d1");
      rec = dataSet.next();
      assertEquals(firstValue.doubleValue(), rec.getFields().get(0).getDoubleV(), 0.001);
      assertFalse(dataSet.hasNext());

      dataSet = session.executeQueryStatement("select last_value(s1) from root.sg1.d1");
      rec = dataSet.next();
      assertEquals(lastValue.doubleValue(), rec.getFields().get(0).getDoubleV(), 0.001);
      assertFalse(dataSet.hasNext());

      // clear data
      filesToLoad.forEach(
          tsfile -> {
            tsfile.delete();
            File resourceFile = new File(tsfile.getAbsolutePath() + ".resource");
            resourceFile.delete();
          });
      filesToLoad.clear();
      session.executeNonQueryStatement("DELETE TIMESERIES root.sg1.d1.s1");
    }
  }

  public void testNonAlignDeviceSequenceDataQuery(TSDataType from, TSDataType to) throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".construct_and_alter_column_type.s1 " + from);
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".construct_and_alter_column_type.s2 " + from);

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              database + ".construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      //      for (int i = 1; i <= 1024; i++) {
      //        int rowIndex = tablet.getRowSize();
      //        tablet.addTimestamp(0, i);
      //        tablet.addValue("s1", rowIndex, genValue(from, i));
      //        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
      //        session.insert(tablet);
      //        tablet.reset();
      //      }
      //
      //      session.executeNonQueryStatement("FLUSH");
      for (int i = 1; i <= 512; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      } else {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      }

      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (Exception e) {
        log.info(e.getMessage());
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES "
                + database
                + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      System.out.println(
          "[testNonAlignDeviceSequenceDataQuery] AFTER ALTER TIMESERIES s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;

      try {
        standardSelectTestAfterAlterColumnType(from, session, newType);
        // Accumulator query test
        standardAccumulatorQueryTest(session, from, newType);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      if (from == TSDataType.DATE) {
        accumulatorQueryTestForDateType(session, to);
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        boolean isExist =
            session
                .executeQueryStatement(
                    "show timeseries " + database + ".construct_and_alter_column_type.**", timeout)
                .hasNext();
        if (isExist) {
          session.executeNonQueryStatement(
              "DELETE TIMESERIES " + database + ".construct_and_alter_column_type.**");
        }
      }
    }
  }

  public void testAlignDeviceSequenceDataQuery(TSDataType from, TSDataType to) throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE ALIGNED TIMESERIES "
              + database
              + ".construct_and_alter_column_type(s1 "
              + from
              + ", s2 "
              + from
              + ")");

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              database + ".construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      //      for (int i = 1; i <= 1024; i++) {
      //        int rowIndex = tablet.getRowSize();
      //        tablet.addTimestamp(0, i);
      //        tablet.addValue("s1", rowIndex, genValue(from, i));
      //        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
      //        session.insert(tablet);
      //        tablet.reset();
      //      }
      //
      //      session.executeNonQueryStatement("FLUSH");
      for (int i = 1; i <= 512; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      } else {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      }

      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES "
                + database
                + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      System.out.println(
          "[testAlignDeviceSequenceDataQuery] AFTER ALTER TIMESERIES s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;

      try {
        standardSelectTestAfterAlterColumnType(from, session, newType);
        // Accumulator query test
        standardAccumulatorQueryTest(session, from, newType);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      if (from == TSDataType.DATE) {
        accumulatorQueryTestForDateType(session, to);
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        boolean isExist =
            session
                .executeQueryStatement(
                    "show timeseries " + database + ".construct_and_alter_column_type.**", timeout)
                .hasNext();
        if (isExist) {
          session.executeNonQueryStatement(
              "DELETE TIMESERIES " + database + ".construct_and_alter_column_type.**");
        }
      }
    }
  }

  public void testNonAlignDeviceUnSequenceDataQuery(TSDataType from, TSDataType to)
      throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".construct_and_alter_column_type.s1 " + from);
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".construct_and_alter_column_type.s2 " + from);

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              database + ".construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      System.out.println(tablet.getSchemas().toString());

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 1; i <= 512; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }
      //        session.executeNonQueryStatement("FLUSH");

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      } else {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      }

      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES "
                + database
                + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      System.out.println(
          "[testNonAlignDeviceUnSequenceDataQuery] AFTER ALTER TIMESERIES s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;

      try {
        standardSelectTestAfterAlterColumnType(from, session, newType);
        // Accumulator query test
        standardAccumulatorQueryTest(session, from, newType);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      if (from == TSDataType.DATE) {
        accumulatorQueryTestForDateType(session, to);
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        boolean isExist =
            session
                .executeQueryStatement(
                    "show timeseries " + database + ".construct_and_alter_column_type.**", timeout)
                .hasNext();
        if (isExist) {
          session.executeNonQueryStatement(
              "DELETE TIMESERIES " + database + ".construct_and_alter_column_type.**");
        }
      }
    }
  }

  public void testAlignDeviceUnSequenceDataQuery(TSDataType from, TSDataType to) throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE ALIGNED TIMESERIES "
              + database
              + ".construct_and_alter_column_type(s1 "
              + from
              + ", s2 "
              + from
              + ")");

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              database + ".construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      System.out.println(tablet.getSchemas().toString());

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 1; i <= 512; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }
      //        session.executeNonQueryStatement("FLUSH");

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      } else {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      }

      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES "
                + database
                + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      System.out.println(
          "[testAlignDeviceUnSequenceDataQuery] AFTER ALTER TIMESERIES s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;

      try {
        standardSelectTestAfterAlterColumnType(from, session, newType);
        // Accumulator query test
        standardAccumulatorQueryTest(session, from, newType);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      if (from == TSDataType.DATE) {
        accumulatorQueryTestForDateType(session, to);
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        boolean isExist =
            session
                .executeQueryStatement(
                    "show timeseries " + database + ".construct_and_alter_column_type.**", timeout)
                .hasNext();
        if (isExist) {
          session.executeNonQueryStatement(
              "DELETE TIMESERIES " + database + ".construct_and_alter_column_type.**");
        }
      }
    }
  }

  public void testNonAlignDeviceUnSequenceOverlappedDataQuery(TSDataType from, TSDataType to)
      throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".construct_and_alter_column_type.s1 " + from);
      session.executeNonQueryStatement(
          "CREATE TIMESERIES " + database + ".construct_and_alter_column_type.s2 " + from);

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              database + ".construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      System.out.println(tablet.getSchemas().toString());

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 1; i <= 520; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }
      //        session.executeNonQueryStatement("FLUSH");

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      } else {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      }

      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES "
                + database
                + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      System.out.println(
          "[testNonAlignDeviceUnSequenceOverlappedDataQuery] AFTER ALTER TIMESERIES s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;

      try {
        standardSelectTestAfterAlterColumnType(from, session, newType);
        // Accumulator query test
        standardAccumulatorQueryTest(session, from, newType);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      if (from == TSDataType.DATE) {
        accumulatorQueryTestForDateType(session, to);
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        boolean isExist =
            session
                .executeQueryStatement(
                    "show timeseries " + database + ".construct_and_alter_column_type.**", timeout)
                .hasNext();
        if (isExist) {
          session.executeNonQueryStatement(
              "DELETE TIMESERIES " + database + ".construct_and_alter_column_type.**");
        }
      }
    }
  }

  public void testAlignDeviceUnSequenceOverlappedDataQuery(TSDataType from, TSDataType to)
      throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION 'enable_unseq_space_compaction'='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE ALIGNED TIMESERIES "
              + database
              + ".construct_and_alter_column_type(s1 "
              + from
              + ", s2 "
              + from
              + ")");

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              database + ".construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      System.out.println(tablet.getSchemas().toString());

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 1; i <= 520; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insertTablet(tablet);
        tablet.reset();
      }
      //        session.executeNonQueryStatement("FLUSH");

      if (DATA_TYPE_LIST.contains(from) || DATA_TYPE_LIST.contains(to)) {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      } else {
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        RowRecord rec = dataSet.next();
        while (rec != null) {
          System.out.println(rec.getFields().toString());
          rec = dataSet.next();
        }
        dataSet.close();
      }

      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (NotSupportedException e) {
        log.info(e.getMessage());
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new Exception(e);
      }

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TIMESERIES "
                + database
                + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TIMESERIES "
                  + database
                  + ".construct_and_alter_column_type.s1 SET DATA TYPE "
                  + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      System.out.println(
          "[testAlignDeviceUnSequenceOverlappedDataQuery] AFTER ALTER TIMESERIES s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;

      try {
        standardSelectTestAfterAlterColumnType(from, session, newType);
        // Accumulator query test
        standardAccumulatorQueryTest(session, from, newType);
      } catch (Exception e) {
        log.info(e.getMessage());
      }

      if (from == TSDataType.DATE) {
        accumulatorQueryTestForDateType(session, to);
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        boolean isExist =
            session
                .executeQueryStatement(
                    "show timeseries " + database + ".construct_and_alter_column_type.**", timeout)
                .hasNext();
        if (isExist) {
          session.executeNonQueryStatement(
              "DELETE TIMESERIES " + database + ".construct_and_alter_column_type.**");
        }
      }
    }
  }

  // Don't support for non-align device unsequence data query, because non-align timeseries is not
  // exist in the table model, only exist align timeseries.
  // Though support for non-align timeseries in the tree model, can let tree transfer to table, but
  // table is a view table, it don't allow alter column type.
  // So can't support functions as below:
  // testNonAlignDeviceSequenceDataQuery();
  // testNonAlignDeviceUnSequenceDataQuery();

  private static void standardSelectTestAfterAlterColumnType(
      TSDataType from, ISession session, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    if (from == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }

    // Value Filter Test
    // 1.satisfy the condition of value filter completely
    SessionDataSet dataSet1;
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= '1' and s2 > '2' order by time");
      } else if (from == TSDataType.BLOB) {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2' order by time");
      } else if (from == TSDataType.BOOLEAN) {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) = 'true' and cast(s2 as TEXT) = 'true' order by time");
      } else {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= '1' and s2 > 2 order by time");
      }
    } else if (newType == TSDataType.BLOB) {
      //      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT || from == TSDataType.BLOB) {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2' order by time");
        //                "select s1 from construct_and_alter_column_type where cast(s1 as
        // TEXT) >= '1' and s2 > '2' order by time");
      } else {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as INT64) >= 1 and cast(s2 as INT64) > 2 order by time");
      }
    } else if (newType == TSDataType.BOOLEAN) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1 from "
                  + database
                  + ".construct_and_alter_column_type where s1 = true and s2 = true order by time");
    } else {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= '1' and s2 > '2' order by time");
      } else {
        dataSet1 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= 1 and s2 > 2 order by time");
      }
    }
    RowRecord rec1;
    for (int i = 2; i <= 3; i++) {
      rec1 = dataSet1.next();
      if (from != TSDataType.BOOLEAN) {
        assertEquals(i, rec1.getTimestamp());
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec1.getFields().get(0).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec1.getFields().get(0).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          if (from == TSDataType.DATE) {
            assertEquals(
                new Binary(genValue(from, i).toString(), StandardCharsets.UTF_8),
                rec1.getFields().get(0).getBinaryV());
          } else {
            assertEquals(
                newType.castFromSingleValue(from, genValue(from, i)),
                rec1.getFields().get(0).getBinaryV());
          }
        } else {
          assertEquals(genValue(newType, i).toString(), rec1.getFields().get(0).toString());
        }
      }
    }
    dataSet1.close();

    // 2.satisfy the condition of value filter partially
    SessionDataSet dataSet2;
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= '1' or s2 < '2' order by time");
      } else if (from == TSDataType.BLOB) {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) >= '1' or cast(s2 as TEXT) < '2' order by time");
      } else if (from == TSDataType.BOOLEAN) {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) = 'true' or cast(s2 as TEXT) = 'false' order by time");
      } else {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= '1' or s2 < 2 order by time");
      }
    } else if (newType == TSDataType.BLOB) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT || from == TSDataType.BLOB) {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) >= '1' or cast(s2 as TEXT) < '2' order by time");
      } else {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as INT64) >= 1 or cast(s2 as INT64) < 2 order by time");
      }
    } else if (newType == TSDataType.BOOLEAN) {
      dataSet2 =
          session.executeQueryStatement(
              "select s1 from "
                  + database
                  + ".construct_and_alter_column_type where s1 = true or s2 = false order by time");
    } else {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= '1' or s2 < '2' order by time");
      } else {
        dataSet2 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 >= 1 or s2 < 2 order by time");
      }
    }

    RowRecord rec2;
    for (int i = 1; i <= 2; i++) {
      rec2 = dataSet2.next();
      if (from != TSDataType.BOOLEAN) {
        assertEquals(i, rec2.getTimestamp());
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec2.getFields().get(0).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec2.getFields().get(0).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          if (from == TSDataType.DATE) {
            assertEquals(
                new Binary(genValue(from, i).toString(), StandardCharsets.UTF_8),
                rec2.getFields().get(0).getBinaryV());
          } else {
            assertEquals(
                newType.castFromSingleValue(from, genValue(from, i)),
                rec2.getFields().get(0).getBinaryV());
          }
        } else {
          assertEquals(genValue(newType, i).toString(), rec2.getFields().get(0).toString());
        }
      }
    }
    dataSet2.close();

    // 3.can't satisfy the condition of value filter at all
    SessionDataSet dataSet3;
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
      if (from == TSDataType.BLOB) {
        dataSet3 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) < '1' order by time");
      } else if (from == TSDataType.BOOLEAN) {
        dataSet3 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where cast(s1 as TEXT) = 'false' order by time");
      } else {
        dataSet3 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 < '1' order by time");
      }
    } else if (newType == TSDataType.BLOB || from == TSDataType.BLOB) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1 from "
                  + database
                  + ".construct_and_alter_column_type where cast(s1 as TEXT) < '1' order by time");
    } else if (newType == TSDataType.BOOLEAN) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1 from "
                  + database
                  + ".construct_and_alter_column_type where s1 = false order by time");
    } else {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet3 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 < '1' order by time");
      } else {
        dataSet3 =
            session.executeQueryStatement(
                "select s1 from "
                    + database
                    + ".construct_and_alter_column_type where s1 < 1 order by time");
      }
    }
    if (from != TSDataType.BOOLEAN) {
      assertFalse(dataSet3.hasNext());
    }
    dataSet3.close();

    // Time filter
    // 1.satisfy the condition of time filter
    SessionDataSet dataSet4 =
        session.executeQueryStatement(
            "select s1 from "
                + database
                + ".construct_and_alter_column_type where time > 1 order by time");
    RowRecord rec4;
    for (int i = 2; i <= 3; i++) {
      rec4 = dataSet4.next();
      assertEquals(i, rec4.getTimestamp());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, i), rec4.getFields().get(0).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, i), rec4.getFields().get(0).getDateV());
      } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
        if (from == TSDataType.DATE) {
          assertEquals(
              new Binary(genValue(from, i).toString(), StandardCharsets.UTF_8),
              rec4.getFields().get(0).getBinaryV());
        } else {
          assertEquals(
              newType.castFromSingleValue(from, genValue(from, i)),
              rec4.getFields().get(0).getBinaryV());
        }
      } else {
        assertEquals(genValue(newType, i).toString(), rec4.getFields().get(0).toString());
      }
    }
    dataSet4.close();
  }

  private static void standardSelectTest(ISession session, TSDataType from, TSDataType to)
      throws StatementExecutionException, IoTDBConnectionException {
    if (from == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }

    // Value Filter Test
    // 1.satisfy the condition of value filter completely
    SessionDataSet dataSet1;
    if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 >= '1' and s2 > '2' order by time");
    } else if (from == TSDataType.BLOB) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2' order by time");
    } else if (from == TSDataType.BOOLEAN) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 = true and s2 = true order by time");
    } else {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 >= 1 and s2 > 2 order by time");
    }
    RowRecord rec1;
    for (int i = 2; i <= 3; i++) {
      rec1 = dataSet1.next();
      System.out.println("rec1: " + rec1.toString());
      if (from != TSDataType.BOOLEAN) {
        assertEquals(genValue(from, i), rec1.getFields().get(0).getObjectValue(from));
        assertEquals(genValue(from, i * 2), rec1.getFields().get(1).getObjectValue(from));
      }
    }
    dataSet1.close();

    // 2.satisfy the condition of value filter partially
    SessionDataSet dataSet2;
    if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 >= '1' or s2 < '2' order by time");
    } else if (from == TSDataType.BLOB) {
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where cast(s1 as TEXT) >= '1' or cast(s2 as TEXT) < '2' order by time");
    } else if (from == TSDataType.BOOLEAN) {
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 = true or s2 = false order by time");
    } else {
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 >= 1 or s2 < 2 order by time");
    }
    RowRecord rec2;
    for (int i = 1; i <= 2; i++) {
      rec2 = dataSet2.next();
      if (from != TSDataType.BOOLEAN) {
        assertEquals(genValue(from, i), rec2.getFields().get(0).getObjectValue(from));
        assertEquals(genValue(from, i * 2), rec2.getFields().get(1).getObjectValue(from));
      }
    }
    dataSet2.close();

    // 3.can't satisfy the condition of value filter at all
    SessionDataSet dataSet3;
    if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 < '1' order by time");
    } else if (from == TSDataType.BLOB) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where cast(s1 as TEXT) < '1' order by time");
    } else if (from == TSDataType.BOOLEAN) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 = false order by time");
    } else {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from "
                  + database
                  + ".construct_and_alter_column_type where s1 < 1 order by time");
    }
    if (from != TSDataType.BOOLEAN) {
      assertFalse(dataSet3.hasNext());
    }
    dataSet3.close();

    // Time filter
    // 1.satisfy the condition of time filter
    SessionDataSet dataSet4 =
        session.executeQueryStatement(
            "select s1, s2 from "
                + database
                + ".construct_and_alter_column_type where time > 1 order by time");
    RowRecord rec4;
    for (int i = 2; i <= 3; i++) {
      rec4 = dataSet4.next();
      if (from != TSDataType.BOOLEAN) {
        assertEquals(genValue(from, i), rec4.getFields().get(0).getObjectValue(from));
        assertEquals(genValue(from, i * 2), rec4.getFields().get(1).getObjectValue(from));
      }
    }
    dataSet4.close();
  }

  private static void standardAccumulatorQueryTest(ISession session, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    if (newType == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }

    SessionDataSet dataSet;
    RowRecord rec;
    if (!UNSUPPORT_ACCUMULATOR_QUERY_DATA_TYPE_LIST.contains(newType)) {
      int[] expectedValue;
      int max = 4;
      if (DATA_TYPE_LIST.contains(newType)) {
        dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        rec = dataSet.next();
        expectedValue = new int[] {1, 1024};
        if (newType == TSDataType.STRING
            || newType == TSDataType.TEXT
            || newType == TSDataType.BLOB) {
          //        expectedValue[1] = 999;
        } else if (newType == TSDataType.BOOLEAN) {
          expectedValue = new int[] {19700102, 19721021};
        }
        max = 2;
      } else {
        dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        rec = dataSet.next();
        expectedValue = new int[] {1, 1024, 1, 1024};
        if (newType == TSDataType.STRING
            || newType == TSDataType.TEXT
            || newType == TSDataType.BLOB) {
          expectedValue[1] = 999;
        } else if (newType == TSDataType.BOOLEAN) {
          expectedValue = new int[] {19700102, 19721021, 19700102, 19721021};
        }
      }

      if (newType != TSDataType.BOOLEAN) {
        for (int i = 0; i < max; i++) {
          if (newType == TSDataType.BLOB) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getBinaryV());
          } else if (newType == TSDataType.DATE) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getDateV());
          } else {
            log.info(
                "i is {}, expected value: {}, actual value: {}",
                i,
                genValue(newType, expectedValue[i]).toString(),
                rec.getFields().get(i).toString());
            assertEquals(
                genValue(newType, expectedValue[i]).toString(), rec.getFields().get(i).toString());
          }
        }

        assertFalse(dataSet.hasNext());

        if (newType.isNumeric()) {
          dataSet =
              session.executeQueryStatement(
                  "select avg(s1),sum(s1) from " + database + ".construct_and_alter_column_type");
          rec = dataSet.next();
          assertEquals(512.5, rec.getFields().get(0).getDoubleV(), 0.001);
          assertEquals(524800.0, rec.getFields().get(1).getDoubleV(), 0.001);
          assertFalse(dataSet.hasNext());
        }
      }
    }

    // can use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from " + database + ".construct_and_alter_column_type where time > 0");
    rec = dataSet.next();
    assertEquals(1024, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());

    // can't use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from "
                + database
                + ".construct_and_alter_column_type where time > 10000");
    rec = dataSet.next();
    assertEquals(0, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());
  }

  private static void standardAccumulatorQueryTest(
      ISession session, TSDataType from, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    if (from == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }

    if (from == TSDataType.BLOB || newType == TSDataType.BLOB) {
      throw new NotSupportedException("Not supported BLOB type.");
    }
    //    if (from == TSDataType.BOOLEAN
    //        && (newType == TSDataType.STRING || newType == TSDataType.TEXT)) {
    if (from == TSDataType.BOOLEAN && DATA_TYPE_LIST.contains(newType)) {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select first_value(s1),last_value(s1) from "
                  + database
                  + ".construct_and_alter_column_type");
      RowRecord rec = dataSet.next();
      boolean[] expectedValue = {false, true};
      for (int i = 0; i < 2; i++) {
        if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          assertEquals(String.valueOf(expectedValue[i]), rec.getFields().get(i).toString());
        }
      }
    } else {
      SessionDataSet dataSet;
      int[] expectedValue;
      int max = 4;
      if (DATA_TYPE_LIST.contains(newType)) {
        dataSet =
            session.executeQueryStatement(
                "select first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        expectedValue = new int[] {1, 1024};
        max = 2;
      } else {
        dataSet =
            session.executeQueryStatement(
                "select min_value(s1),max_value(s1),first_value(s1),last_value(s1) from "
                    + database
                    + ".construct_and_alter_column_type");
        expectedValue = new int[] {1, 1024, 1, 1024};
        if (newType == TSDataType.BOOLEAN) {
          expectedValue = new int[] {19700102, 19721021, 19700102, 19721021};
        }
      }
      RowRecord rec = dataSet.next();
      if (newType != TSDataType.BOOLEAN) {
        for (int i = 0; i < max; i++) {
          if (newType == TSDataType.BLOB) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getBinaryV());
          } else if (newType == TSDataType.DATE) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getDateV());
          } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
            if (from == TSDataType.DATE) {
              log.info(
                  "i is {}, expected value: {}, actual value: {}",
                  i,
                  new Binary(genValue(from, expectedValue[i]).toString(), StandardCharsets.UTF_8),
                  rec.getFields().get(i).getBinaryV());
              assertEquals(
                  new Binary(genValue(from, expectedValue[i]).toString(), StandardCharsets.UTF_8),
                  rec.getFields().get(i).getBinaryV());
            } else {
              log.info(
                  "i is {}, expected value: {}, actual value: {}",
                  i,
                  newType.castFromSingleValue(from, genValue(from, expectedValue[i])),
                  rec.getFields().get(i).getBinaryV());
              assertEquals(
                  newType.castFromSingleValue(from, genValue(from, expectedValue[i])),
                  rec.getFields().get(i).getBinaryV());
            }
          } else {
            log.info(
                "i is {}, expected value: {}, actual value: {}",
                i,
                genValue(newType, expectedValue[i]).toString(),
                rec.getFields().get(i).toString());
            assertEquals(
                genValue(newType, expectedValue[i]).toString(), rec.getFields().get(i).toString());
          }
        }

        assertFalse(dataSet.hasNext());

        if (newType.isNumeric()) {
          dataSet =
              session.executeQueryStatement(
                  "select avg(s1),sum(s1) from " + database + ".construct_and_alter_column_type");
          rec = dataSet.next();
          assertEquals(512.5, rec.getFields().get(0).getDoubleV(), 0.001);
          assertEquals(524800.0, rec.getFields().get(1).getDoubleV(), 0.001);
          assertFalse(dataSet.hasNext());
        }
      }

      // can use statistics information
      dataSet =
          session.executeQueryStatement(
              "select count(*) from "
                  + database
                  + ".construct_and_alter_column_type where time > 0");
      rec = dataSet.next();
      assertEquals(1024, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());

      // can't use statistics information
      dataSet =
          session.executeQueryStatement(
              "select count(*) from "
                  + database
                  + ".construct_and_alter_column_type where time > 10000");
      rec = dataSet.next();
      assertEquals(0, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }
  }

  private static void accumulatorQueryTestForDateType(ISession session, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    if (newType != TSDataType.STRING && newType != TSDataType.TEXT) {
      return;
    }

    log.info("Test the result that after transfered newType:");

    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select first_value(s1),last_value(s1) from "
                + database
                + ".construct_and_alter_column_type");
    RowRecord rec = dataSet.next();
    int[] expectedValue = {19700102, 19721021};
    if (newType != TSDataType.BOOLEAN) {
      for (int i = 0; i < 2; i++) {
        if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          log.info(
              "i is {}, expected value: {}, actual value: {}",
              i,
              TSDataType.getDateStringValue(expectedValue[i]),
              //              rec.getFields().get(i).getBinaryV().toString());
              rec.getFields().get(i).getStringValue());
          assertEquals(
              TSDataType.getDateStringValue(expectedValue[i]),
              rec.getFields().get(i).getBinaryV().toString());
        }
      }
    }

    // can use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from " + database + ".construct_and_alter_column_type where time > 0");
    rec = dataSet.next();
    assertEquals(1024, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());

    // can't use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from "
                + database
                + ".construct_and_alter_column_type where time > 10000");
    rec = dataSet.next();
    assertEquals(0, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());
  }

  private static void writeWithTablets(
      long timestamp,
      Object dataValue,
      TSDataType tsDataType,
      List<Tablet> tabletList,
      Set<String> alignedDevices,
      TsFileWriter tsFileWriter,
      Map<String, List<Integer>> deviceColumnIndices)
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    RowRecord rowRecord = new RowRecord(timestamp);
    rowRecord.addField(dataValue, tsDataType);
    rowRecord.addField(dataValue, tsDataType);
    List<Field> fields = rowRecord.getFields();

    for (Tablet tablet : tabletList) {
      String deviceId = tablet.getDeviceId();
      List<Integer> columnIndices = deviceColumnIndices.get(deviceId);
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());
      List<IMeasurementSchema> schemas = tablet.getSchemas();

      for (int i = 0, columnIndicesSize = columnIndices.size(); i < columnIndicesSize; i++) {
        Integer columnIndex = columnIndices.get(i);
        IMeasurementSchema measurementSchema = schemas.get(i);
        //        Object value = fields.get(columnIndex -
        // 1).getObjectValue(measurementSchema.getType());
        Object value = fields.get(columnIndex).getObjectValue(measurementSchema.getType());
        tablet.addValue(measurementSchema.getMeasurementName(), rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writeToTsFile(alignedDevices, tsFileWriter, tablet);
        tablet.reset();
      }
    }

    for (Tablet tablet : tabletList) {
      if (tablet.getRowSize() != 0) {
        writeToTsFile(alignedDevices, tsFileWriter, tablet);
      }
    }
  }

  private static void writeToTsFile(
      Set<String> deviceFilterSet, TsFileWriter tsFileWriter, Tablet tablet)
      throws IOException, WriteProcessException {
    if (deviceFilterSet.contains(tablet.getDeviceId())) {
      tsFileWriter.writeAligned(tablet);
    } else {
      tsFileWriter.writeTree(tablet);
    }
  }

  private static List<Tablet> constructTablets(
      Map<String, List<IMeasurementSchema>> deviceSchemaMap,
      Set<String> alignedDevices,
      TsFileWriter tsFileWriter)
      throws WriteProcessException {
    List<Tablet> tabletList = new ArrayList<>(deviceSchemaMap.size());
    for (Map.Entry<String, List<IMeasurementSchema>> stringListEntry : deviceSchemaMap.entrySet()) {
      String deviceId = stringListEntry.getKey();
      List<IMeasurementSchema> schemaList = stringListEntry.getValue();
      Tablet tablet = new Tablet(deviceId, schemaList);
      tablet.initBitMaps();
      Path path = new Path(tablet.getDeviceId());
      if (alignedDevices.contains(tablet.getDeviceId())) {
        tsFileWriter.registerAlignedTimeseries(path, schemaList);
      } else {
        tsFileWriter.registerTimeseries(path, schemaList);
      }
      tabletList.add(tablet);
    }
    return tabletList;
  }

  protected static TSDataType getType(String typeStr) {
    try {
      return TSDataType.valueOf(typeStr);
    } catch (Exception e) {
      return null;
    }
  }

  private static void collectSchemas(
      ISession session,
      List<String> columnNames,
      List<String> columnTypes,
      Map<String, List<IMeasurementSchema>> deviceSchemaMap,
      Set<String> alignedDevices,
      Map<String, List<Integer>> deviceColumnIndices)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < columnNames.size(); i++) {
      String column = columnNames.get(i);
      if (!column.startsWith("root.")) {
        continue;
      }
      TSDataType tsDataType = getType(columnTypes.get(i));
      Path path = new Path(column, true);
      String deviceId = path.getDeviceString();
      // query whether the device is aligned or not
      try (SessionDataSet deviceDataSet =
          session.executeQueryStatement("show devices " + deviceId, timeout)) {
        List<Field> deviceList = deviceDataSet.next().getFields();
        if (deviceList.size() > 1 && "true".equals(deviceList.get(1).getStringValue())) {
          alignedDevices.add(deviceId);
        }
      }

      // query timeseries metadata
      MeasurementSchema measurementSchema =
          new MeasurementSchema(path.getMeasurement(), tsDataType);
      List<Field> seriesList =
          session.executeQueryStatement("show timeseries " + column, timeout).next().getFields();
      measurementSchema.setEncoding(TSEncoding.valueOf(seriesList.get(4).getStringValue()));
      measurementSchema.setCompressionType(
          CompressionType.valueOf(seriesList.get(5).getStringValue()));

      deviceSchemaMap.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(measurementSchema);
      deviceColumnIndices.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(i);
    }
  }

  @Test
  public void testUsingSameColumn() {
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.TIMESTAMP, TSDataType.INT64));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.INT64, TSDataType.TIMESTAMP));

    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.DATE, TSDataType.INT32));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.INT32, TSDataType.DATE));

    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.TEXT, TSDataType.BLOB));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.TEXT, TSDataType.STRING));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.BLOB, TSDataType.TEXT));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.BLOB, TSDataType.STRING));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.STRING, TSDataType.BLOB));
    assertEquals(true, SchemaUtils.isUsingSameColumn(TSDataType.STRING, TSDataType.TEXT));
  }
}
