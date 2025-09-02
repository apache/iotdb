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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.commons.utils.MetadataUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
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
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
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

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.relational.it.session.IoTDBSessionRelationalIT.genValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlterColumnTypeIT {

  private static final Logger log = LoggerFactory.getLogger(IoTDBAlterColumnTypeIT.class);
  private static long timeout = -1;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setCompactionScheduleInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS test");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testWriteAndAlter()
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    Set<TSDataType> typesToTest = new HashSet<>();
    Collections.addAll(typesToTest, TSDataType.values());
    typesToTest.remove(TSDataType.VECTOR);
    typesToTest.remove(TSDataType.UNKNOWN);
    //    typesToTest.remove(TSDataType.STRING);
    //    typesToTest.remove(TSDataType.TEXT);
    //    typesToTest.remove(TSDataType.DATE);
    //    typesToTest.remove(TSDataType.TIMESTAMP); // start
    //    typesToTest.remove(TSDataType.BLOB);
    //    typesToTest.remove(TSDataType.BOOLEAN);

    //    doWriteAndAlter(TSDataType.INT32, TSDataType.FLOAT, false);
    //    doWriteAndAlter(TSDataType.INT32, TSDataType.FLOAT, true);

    for (TSDataType from : typesToTest) {
      for (TSDataType to : typesToTest) {
        //    TSDataType from = TSDataType.INT32;
        //    TSDataType to = TSDataType.STRING;
        //      if (from == TSDataType.DATE && to == TSDataType.DATE) {
        //        continue;
        //      }

        //            TSDataType from = TSDataType.DATE;
        //            TSDataType to = TSDataType.DATE;

        /***@todo tmp test***/
        //        System.out.printf("testing %s to %s%n", from, to);
        //        doWriteAndAlter(from, to);
        //        if (from == TSDataType.DATE && to == TSDataType.DATE) {
        //          continue;
        //        }
        //        if (to.isCompatible(from)) {
        //          testAlignDeviceSequenceDataQuery(from, to);
        //          testAlignDeviceUnSequenceDataQuery(from, to);
        //        }
        /***@todo tmp test***/

        if (from != to && to.isCompatible(from)) {
          System.out.printf("testing %s to %s%n", from, to);
          doWriteAndAlter(from, to);
          testAlignDeviceSequenceDataQuery(from, to);
          testAlignDeviceUnSequenceDataQuery(from, to);
        }
      }
    }
  }

  private void doWriteAndAlter(TSDataType from, TSDataType to)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      /***@todo add new test***/
      session.executeNonQueryStatement("SET CONFIGURATION enable_unseq_space_compaction='false'");
      if (from == TSDataType.DATE && !to.isCompatible(from)) {
        throw new NotSupportedException("Not supported DATE type.");
      }
      /***@todo end***/

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS write_and_alter_column_type (s1 " + from + ")");

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              "write_and_alter_column_type",
              Collections.singletonList("s1"),
              Collections.singletonList(from),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(from, 1));
      session.insert(tablet);
      tablet.reset();
      session.executeNonQueryStatement("FLUSH");

      // write an unsequence tsfile point of "from"
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(from, 1));
      session.insert(tablet);
      tablet.reset();
      session.executeNonQueryStatement("FLUSH");

      // write a sequence memtable point of "from"
      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(from, 2));
      session.insert(tablet);
      tablet.reset();

      // write an unsequence memtable point of "from"
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(from, 1));
      session.insert(tablet);
      tablet.reset();

      /**
       * @todo tmp test if delete ?*
       */
      SessionDataSet dataSet1 =
          session.executeQueryStatement("select * from write_and_alter_column_type order by time");
      RowRecord rec1;
      for (int i = 1; i <= 2; i++) {
        rec1 = dataSet1.next();
        assertEquals(i, rec1.getFields().get(0).getLongV());
        System.out.println(i + " is " + rec1.getFields().get(1).toString());
      }
      /**
       * @todo tmp test end *
       */

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TABLE write_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE write_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
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
          // @todo tmp test
          //          session.executeQueryStatement("select * from write_and_alter_column_type order
          // by time");
          // @todo tmp test
          session.executeQueryStatement(
              "select time, s1 from write_and_alter_column_type order by time");
      RowRecord rec;
      TSDataType newType = isCompatible ? to : from;
      for (int i = 1; i <= 2; i++) {
        rec = dataSet.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec.getFields().get(1).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec.getFields().get(1).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          if (from == TSDataType.DATE) {
            assertEquals(genValue(from, i).toString(), rec.getFields().get(1).toString());
            //
            // getDateStringValue(Integer.parseInt(rec.getFields().get(1).toString())));
          } else {
            assertEquals(String.valueOf(genValue(from, i)), rec.getFields().get(1).toString());
          }
        } else {
          assertEquals(genValue(newType, i).toString(), rec.getFields().get(1).toString());
        }
      }
      assertNull(dataSet.next());
      dataSet.close();

      // write an altered point in sequence and unsequnce tsfile
      tablet =
          new Tablet(
              "write_and_alter_column_type",
              Collections.singletonList("s1"),
              Collections.singletonList(newType),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 3);
      tablet.addValue("s1", 0, genValue(newType, 3));
      session.insert(tablet);
      tablet.reset();

      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(newType, 1));
      session.insert(tablet);
      tablet.reset();
      session.executeNonQueryStatement("FLUSH");

      // write an altered point in sequence and unsequnce memtable
      tablet.addTimestamp(0, 4);
      tablet.addValue("s1", 0, genValue(newType, 4));
      session.insert(tablet);
      tablet.reset();

      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(newType, 2));
      session.insert(tablet);
      tablet.reset();

      dataSet =
          session.executeQueryStatement("select * from write_and_alter_column_type order by time");
      for (int i = 1; i <= 4; i++) {
        rec = dataSet.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec.getFields().get(1).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec.getFields().get(1).getDateV());
        } else {
          assertEquals(genValue(newType, i).toString(), rec.getFields().get(1).toString());
        }
      }
      assertFalse(dataSet.hasNext());

      dataSet =
          session.executeQueryStatement(
              "select min(s1),max(s1),first(s1),last(s1) from write_and_alter_column_type");
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

      if (newType.isNumeric()) {
        dataSet =
            session.executeQueryStatement(
                "select avg(s1),sum(s1) from write_and_alter_column_type");
        rec = dataSet.next();
        assertEquals(2.5, rec.getFields().get(0).getDoubleV(), 0.001);
        assertEquals(10.0, rec.getFields().get(1).getDoubleV(), 0.001);
        assertFalse(dataSet.hasNext());
      }

      session.executeNonQueryStatement("DROP TABLE write_and_alter_column_type");
    }
  }

  @Test
  public void testAlterWithoutWrite() throws IoTDBConnectionException, StatementExecutionException {
    Set<TSDataType> typesToTest = new HashSet<>();
    Collections.addAll(typesToTest, TSDataType.values());
    typesToTest.remove(TSDataType.VECTOR);
    typesToTest.remove(TSDataType.UNKNOWN);
    //    typesToTest.remove(TSDataType.STRING);
    //    typesToTest.remove(TSDataType.TEXT);
    //    typesToTest.remove(TSDataType.DATE);

    for (TSDataType from : typesToTest) {
      for (TSDataType to : typesToTest) {
        /**
         * @todo tmp test/ System.out.printf("testing %s to %s%n", from, to);
         *     doAlterWithoutWrite(from, to, false); doAlterWithoutWrite(from, to, true); /***@todo
         *     end**
         */
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
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      /***@todo tmp**/
      if (from == TSDataType.DATE && !to.isCompatible(from)) {
        throw new NotSupportedException("Not supported DATE type.");
      }
      /***@todo end**/

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS just_alter_column_type (s1 " + from + ")");

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TABLE just_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE just_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
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
              "just_alter_column_type",
              Collections.singletonList("s1"),
              Collections.singletonList(newType),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(newType, 1));
      session.insert(tablet);
      tablet.reset();

      if (flush) {
        session.executeNonQueryStatement("FLUSH");
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from just_alter_column_type order by time");
      RowRecord rec = dataSet.next();
      assertEquals(1, rec.getFields().get(0).getLongV());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, 1), rec.getFields().get(1).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, 1), rec.getFields().get(1).getDateV());
      } else {
        assertEquals(genValue(newType, 1).toString(), rec.getFields().get(1).toString());
      }

      assertFalse(dataSet.hasNext());

      session.executeNonQueryStatement("DROP TABLE just_alter_column_type");
    }
  }

  @Test
  public void testAlterNonExist() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      try {
        session.executeNonQueryStatement(
            "ALTER TABLE non_exist ALTER COLUMN s1 SET DATA TYPE INT64");
        fail("Should throw exception");
      } catch (StatementExecutionException e) {
        assertEquals("550: Table 'test.non_exist' does not exist", e.getMessage());
      }
      session.executeNonQueryStatement(
          "ALTER TABLE IF EXISTS non_exist ALTER COLUMN s1 SET DATA TYPE INT64");

      session.executeNonQueryStatement("CREATE TABLE IF NOT EXISTS non_exist (s1 int32)");

      try {
        session.executeNonQueryStatement(
            "ALTER TABLE non_exist ALTER COLUMN s2 SET DATA TYPE INT64");
        fail("Should throw exception");
      } catch (StatementExecutionException e) {
        assertEquals("616: Column s2 in table 'test.non_exist' does not exist.", e.getMessage());
      }
      session.executeNonQueryStatement(
          "ALTER TABLE non_exist ALTER COLUMN IF EXISTS s2 SET DATA TYPE INT64");
    }
  }

  @Test
  public void testAlterWrongType() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement("CREATE TABLE IF NOT EXISTS wrong_type (s1 int32)");

      try {
        session.executeNonQueryStatement(
            "ALTER TABLE non_exist ALTER COLUMN s1 SET DATA TYPE VECTOR");
        fail("Should throw exception");
      } catch (StatementExecutionException e) {
        assertEquals("701: Unknown type: VECTOR", e.getMessage());
      }
    }
  }

  @Test
  public void testDropAndAlter() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement("CREATE TABLE IF NOT EXISTS drop_and_alter (s1 int32)");

      // time=1 and time=2 are INT32 and deleted by drop column
      Tablet tablet =
          new Tablet(
              "drop_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(TSDataType.INT32, 1));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              "drop_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(TSDataType.INT32, 2));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("ALTER TABLE drop_and_alter DROP COLUMN s1");

      // time=3 and time=4 are STRING
      tablet =
          new Tablet(
              "drop_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.STRING),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 3);
      tablet.addValue("s1", 0, genValue(TSDataType.STRING, 3));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              "drop_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.STRING),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 4);
      tablet.addValue("s1", 0, genValue(TSDataType.STRING, 4));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement(
          "ALTER TABLE drop_and_alter ALTER COLUMN s1 SET DATA TYPE TEXT");

      // time=5 and time=6 are TEXT
      tablet =
          new Tablet(
              "drop_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.TEXT),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 5);
      tablet.addValue("s1", 0, genValue(TSDataType.STRING, 5));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              "drop_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.TEXT),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 6);
      tablet.addValue("s1", 0, genValue(TSDataType.STRING, 6));
      session.insert(tablet);
      tablet.reset();

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from drop_and_alter order by time");
      // s1 is dropped but the time should remain
      RowRecord rec;
      for (int i = 1; i < 3; i++) {
        rec = dataSet.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        assertNull(rec.getFields().get(1).getDataType());
      }
      for (int i = 3; i < 7; i++) {
        rec = dataSet.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        assertEquals(genValue(TSDataType.STRING, i).toString(), rec.getFields().get(1).toString());
      }
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void testContinuousAlter() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement("CREATE TABLE IF NOT EXISTS alter_and_alter (s1 int32)");

      // time=1 and time=2 are INT32
      Tablet tablet =
          new Tablet(
              "alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, genValue(TSDataType.INT32, 1));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              "alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(TSDataType.INT32, 2));
      session.insert(tablet);
      tablet.reset();

      // time=3 and time=4 are FLOAT
      session.executeNonQueryStatement(
          "ALTER TABLE alter_and_alter ALTER COLUMN s1 SET DATA TYPE FLOAT");
      tablet =
          new Tablet(
              "alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.FLOAT),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 3);
      tablet.addValue("s1", 0, genValue(TSDataType.FLOAT, 3));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              "alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.FLOAT),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 4);
      tablet.addValue("s1", 0, genValue(TSDataType.FLOAT, 4));
      session.insert(tablet);
      tablet.reset();

      // time=5 and time=6 are DOUBLE
      session.executeNonQueryStatement(
          "ALTER TABLE alter_and_alter ALTER COLUMN s1 SET DATA TYPE DOUBLE");
      tablet =
          new Tablet(
              "alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.DOUBLE),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 5);
      tablet.addValue("s1", 0, genValue(TSDataType.DOUBLE, 5));
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLUSH");

      tablet =
          new Tablet(
              "alter_and_alter",
              Collections.singletonList("s1"),
              Collections.singletonList(TSDataType.DOUBLE),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 6);
      tablet.addValue("s1", 0, genValue(TSDataType.DOUBLE, 6));
      session.insert(tablet);
      tablet.reset();

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from alter_and_alter order by time");
      RowRecord rec;
      for (int i = 1; i < 7; i++) {
        rec = dataSet.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        assertEquals(genValue(TSDataType.DOUBLE, i).toString(), rec.getFields().get(1).toString());
      }
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void testConcurrentWriteAndAlter()
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS concurrent_write_and_alter (s1 int32)");
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

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from concurrent_write_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      assertEquals(maxWrite, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }
  }

  private void write(AtomicInteger writeCounter, int maxWrite, int flushInterval)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      int writtenCnt = 0;
      do {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO concurrent_write_and_alter (time, s1) VALUES (%d, %d)",
                writtenCnt, writtenCnt));
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
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement(
          "ALTER TABLE concurrent_write_and_alter ALTER COLUMN s1 SET DATA TYPE DOUBLE");
    }
  }

  @Test
  public void testLoadAndAlter()
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement("SET CONFIGURATION enable_unseq_space_compaction='false'");
    }

    // file1-file4 s1=INT32
    TableSchema schema1 =
        new TableSchema(
            "load_and_alter",
            Arrays.asList(
                new ColumnSchema("dId", TSDataType.STRING, ColumnCategory.TAG),
                new ColumnSchema("s1", TSDataType.INT32, ColumnCategory.FIELD)));
    // file1-file3 single device small range ([1, 1]), may load without split
    List<File> filesToLoad = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      File file = new File("target", "f" + i + ".tsfile");
      try (ITsFileWriter tsFileWriter =
          new TsFileWriterBuilder().file(file).tableSchema(schema1).build()) {
        Tablet tablet =
            new Tablet(
                schema1.getTableName(),
                Arrays.asList("dId", "s1"),
                Arrays.asList(TSDataType.STRING, TSDataType.INT32),
                Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
        tablet.addTimestamp(0, 1);
        tablet.addValue("dId", 0, "d" + i);
        tablet.addValue("s1", 0, 1);
        tsFileWriter.write(tablet);
      }
      filesToLoad.add(file);
    }
    // file4 multi device large range ([2, 100_000_000]), load with split
    File file = new File("target", "f" + 4 + ".tsfile");
    try (ITsFileWriter tsFileWriter =
        new TsFileWriterBuilder().file(file).tableSchema(schema1).build()) {
      Tablet tablet =
          new Tablet(
              schema1.getTableName(),
              Arrays.asList("dId", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
      int rowIndex = 0;
      for (int i = 1; i <= 3; i++) {
        tablet.addTimestamp(rowIndex, 2);
        tablet.addValue("dId", rowIndex, "d" + i);
        tablet.addValue("s1", rowIndex, 2);
        rowIndex++;
        tablet.addTimestamp(rowIndex, 100_000_000);
        tablet.addValue("dId", rowIndex, "d" + i);
        tablet.addValue("s1", rowIndex, 100_000_000);
        rowIndex++;
      }
      tsFileWriter.write(tablet);
    }
    filesToLoad.add(file);

    // load file1-file4
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }
    }
    // check load result
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from load_and_alter");
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
    TableSchema schema2 =
        new TableSchema(
            "load_and_alter",
            Arrays.asList(
                new ColumnSchema("dId", TSDataType.STRING, ColumnCategory.TAG),
                new ColumnSchema("s1", TSDataType.DOUBLE, ColumnCategory.FIELD)));
    // file5-file7 single device small range ([3, 3]), may load without split
    for (int i = 5; i <= 7; i++) {
      file = new File("target", "f" + i + ".tsfile");
      try (ITsFileWriter tsFileWriter =
          new TsFileWriterBuilder().file(file).tableSchema(schema2).build()) {
        Tablet tablet =
            new Tablet(
                schema2.getTableName(),
                Arrays.asList("dId", "s1"),
                Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
                Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
        tablet.addTimestamp(0, 3);
        tablet.addValue("dId", 0, "d" + i);
        tablet.addValue("s1", 0, 3.0);
        tsFileWriter.write(tablet);
      }
      filesToLoad.add(file);
    }
    // file8 multi device large range ([4, 100_000_001]), load with split
    file = new File("target", "f" + 8 + ".tsfile");
    try (ITsFileWriter tsFileWriter =
        new TsFileWriterBuilder().file(file).tableSchema(schema2).build()) {
      Tablet tablet =
          new Tablet(
              schema2.getTableName(),
              Arrays.asList("dId", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
      int rowIndex = 0;
      for (int i = 1; i <= 3; i++) {
        tablet.addTimestamp(rowIndex, 4);
        tablet.addValue("dId", rowIndex, "d" + i);
        tablet.addValue("s1", rowIndex, 4.0);
        rowIndex++;
        tablet.addTimestamp(rowIndex, 100_000_001);
        tablet.addValue("dId", rowIndex, "d" + i);
        tablet.addValue("s1", rowIndex, 100_000_001.0);
        rowIndex++;
      }
      tsFileWriter.write(tablet);
    }
    filesToLoad.add(file);

    // load file5-file8
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }
    }
    // check load result
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from load_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      // Due to the operation of load tsfile execute directly, don't access memtable or generate
      // InsertNode object, so don't need to check the data type.
      // When query this measurement point, will only find the data of TSDataType.INT32. So this is
      // reason what cause we can't find the data of TSDataType.DOUBLE. So result is 9, is not 18.
      //      assertEquals(18, rec.getFields().get(0).getLongV());
      assertEquals(9, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }

    // alter s1 to double
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement(
          "ALTER TABLE load_and_alter ALTER COLUMN s1 SET DATA TYPE DOUBLE");
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
      try (ITsFileWriter tsFileWriter =
          new TsFileWriterBuilder().file(file).tableSchema(schema1).build()) {
        Tablet tablet =
            new Tablet(
                schema1.getTableName(),
                Arrays.asList("dId", "s1"),
                Arrays.asList(TSDataType.STRING, TSDataType.INT32),
                Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
        tablet.addTimestamp(0, 5);
        tablet.addValue("dId", 0, "d" + i);
        tablet.addValue("s1", 0, 5);
        tsFileWriter.write(tablet);
      }
      filesToLoad.add(file);
    }
    // file12 multi device large range ([6, 100_000_002]), load with split
    file = new File("target", "f" + 12 + ".tsfile");
    try (ITsFileWriter tsFileWriter =
        new TsFileWriterBuilder().file(file).tableSchema(schema1).build()) {
      Tablet tablet =
          new Tablet(
              schema1.getTableName(),
              Arrays.asList("dId", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
      int rowIndex = 0;
      for (int i = 1; i <= 3; i++) {
        tablet.addTimestamp(rowIndex, 6);
        tablet.addValue("dId", rowIndex, "d" + i);
        tablet.addValue("s1", rowIndex, 6);
        rowIndex++;
        tablet.addTimestamp(rowIndex, 100_000_002);
        tablet.addValue("dId", rowIndex, "d" + i);
        tablet.addValue("s1", rowIndex, 100_000_002);
        rowIndex++;
      }
      tsFileWriter.write(tablet);
    }
    filesToLoad.add(file);

    // load file9-file12, should succeed
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      for (File f : filesToLoad) {
        session.executeNonQueryStatement("LOAD '" + f.getAbsolutePath() + "'");
      }
    }
    // check load result
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      SessionDataSet dataSet =
          session.executeQueryStatement("select count(s1) from load_and_alter");
      RowRecord rec;
      rec = dataSet.next();
      assertEquals(27, rec.getFields().get(0).getLongV());
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
  public void testAlterViewType() throws IoTDBConnectionException, StatementExecutionException {
    String[] createTreeDataSqls = {
      "CREATE ALIGNED TIMESERIES root.db.battery.b0(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b0(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b1(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (1, 1, 1)",
      "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (2, 1, 1)",
      "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (3, 1, 1)",
      "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (4, 1, 1)",
      "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values ("
          + System.currentTimeMillis()
          + ", 1, 1)",
      "CREATE TIMESERIES root.db.battery.b2.voltage INT32",
      "CREATE TIMESERIES root.db.battery.b2.current FLOAT",
      "INSERT INTO root.db.battery.b2(time, voltage, current) values (1, 1, 1)",
      "INSERT INTO root.db.battery.b2(time, voltage, current) values (2, 1, 1)",
      "INSERT INTO root.db.battery.b2(time, voltage, current) values (3, 1, 1)",
      "INSERT INTO root.db.battery.b2(time, voltage, current) values (4, 1, 1)",
      "INSERT INTO root.db.battery.b2(time, voltage, current) values ("
          + System.currentTimeMillis()
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b3(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b4(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b4(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b5(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b5(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b6(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b6(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b7(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b7(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b8(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b8(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "CREATE ALIGNED TIMESERIES root.db.battery.b9(voltage INT32, current FLOAT)",
      "INSERT INTO root.db.battery.b9(time, voltage, current) aligned values ("
          + (System.currentTimeMillis() - 100000)
          + ", 1, 1)",
      "flush",
      "set ttl to root.db.battery.** 200000",
      "set ttl to root.db.battery.b0 50000",
      "set ttl to root.db.battery.b6 50000",
    };

    String[] createTableSqls = {
      "CREATE DATABASE test",
      "USE test",
      "CREATE VIEW view1 (battery TAG, voltage INT32 FIELD, current FLOAT FIELD) as root.db.battery.**",
    };

    prepareData(createTreeDataSqls);
    prepareTableData(createTableSqls);

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select count(*) from view1 where battery = 'b1'");
      assertTrue(sessionDataSet.hasNext());
      RowRecord record = sessionDataSet.next();
      assertEquals(1, record.getField(0).getLongV());
      assertFalse(sessionDataSet.hasNext());
      sessionDataSet.close();

      sessionDataSet = session.executeQueryStatement("select * from view1");
      int count = 0;
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        count++;
      }
      sessionDataSet.close();
      assertEquals(8, count);

      // alter the type to "to"
      TSDataType from = TSDataType.FLOAT;
      TSDataType to = TSDataType.DOUBLE;
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE view1 ALTER COLUMN current SET DATA TYPE " + to);
          //          SessionDataSet dataSet = session.executeQueryStatement(
          //                  "DESC view1");
          //          while (dataSet.hasNext()) {
          //            RowRecord rowRecord = dataSet.next();
          //            System.out.println("rowRecord is " + rowRecord.toString());
          //          }
        } catch (Exception e) {
          assertEquals(
              "701: Table 'test.view1' is a tree view table, does not support alter table",
              e.getMessage());
        }
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE view1 ALTER COLUMN current SET DATA TYPE " + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      session.executeNonQueryStatement("DROP VIEW view1");
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("DELETE TIMESERIES root.db.battery.**");
    }
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

  public void testAlignDeviceSequenceDataQuery(TSDataType from, TSDataType to)
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement("SET CONFIGURATION enable_unseq_space_compaction='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS construct_and_alter_column_type (s1 "
              + from
              + ", s2 "
              + from
              + ")");

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              "construct_and_alter_column_type",
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
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insert(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select min(s1),max(s1),first(s1),last(s1) from construct_and_alter_column_type");
      RowRecord rec = dataSet.next();
      while (rec != null) {
        System.out.println(rec.getFields().toString());
        rec = dataSet.next();
      }
      dataSet.close();

      //      SessionDataSet dataSet1 =
      //              session.executeQueryStatement(
      ////                      "select cast(s1 as TEXT), cast(s2 as TEXT), s1, s2 from
      // construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2'
      // order by time");
      //                      "select cast(s1 as INT64), cast(s2 as INT64), s1, s2 from
      // construct_and_alter_column_type where cast(s1 as INT64) > 1 and cast(s2 as INT64) > 2 order
      // by time limit 10");
      //      RowRecord rec1 = dataSet1.next();
      //      while (rec1 != null) {
      //        System.out.println(rec1.getFields().toString());
      //        rec1 = dataSet1.next();
      //      }
      //      dataSet1.close();
      //      System.exit(0);

      /**
       * @todo tmp test*
       */
      //      standardSelectTest(from, session);
      //      standardAccumulatorQueryTest(session, from);
      /** end* */
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
            "ALTER TABLE construct_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE construct_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      // @todo tmp test
      System.out.println("[testAlignDeviceSequenceDataQuery] AFTER ALTER COLUMN s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;
      /**
       * @todo tmp test*
       */
      //      standardSelectTestAfterAlterColumnType(from, session, newType);
      //
      //      // Accumulator query test
      //      standardAccumulatorQueryTest(session, newType);
      /**
       * @todo end*
       */
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

      session.executeNonQueryStatement("DROP TABLE construct_and_alter_column_type");
    }
  }

  public void testAlignDeviceUnSequenceDataQuery(TSDataType from, TSDataType to)
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement("SET CONFIGURATION enable_unseq_space_compaction='false'");

      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS construct_and_alter_column_type (s1 "
              + from
              + ", s2 "
              + from
              + ")");

      // write a sequence tsfile point of "from"
      Tablet tablet =
          new Tablet(
              "construct_and_alter_column_type",
              Arrays.asList("s1", "s2"),
              Arrays.asList(from, from),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));

      // @todo tmp test
      System.out.println(tablet.getSchemas().toString());

      for (int i = 513; i <= 1024; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLUSH");

      for (int i = 1; i <= 512; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(0, i);
        tablet.addValue("s1", rowIndex, genValue(from, i));
        tablet.addValue("s2", rowIndex, genValue(from, i * 2));
        session.insert(tablet);
        tablet.reset();
      }
      // @todo tmp test if delete?
      //        session.executeNonQueryStatement("FLUSH");

      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select min(s1),max(s1),first(s1),last(s1) from construct_and_alter_column_type");
      RowRecord rec = dataSet.next();
      while (rec != null) {
        System.out.println(rec.getFields().toString());
        rec = dataSet.next();
      }
      dataSet.close();

      //      SessionDataSet dataSet1 =
      //              session.executeQueryStatement(
      ////                      "select cast(s1 as TEXT), cast(s2 as TEXT), s1, s2 from
      // construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2'
      // order by time");
      //                      "select cast(s1 as INT64), cast(s2 as INT64), s1, s2 from
      // construct_and_alter_column_type where cast(s1 as INT64) > 1 and cast(s2 as INT64) > 2 order
      // by time limit 10");
      //      RowRecord rec1 = dataSet1.next();
      //      while (rec1 != null) {
      //        System.out.println(rec1.getFields().toString());
      //        rec1 = dataSet1.next();
      //      }
      //      dataSet1.close();
      //      System.exit(0);

      /**
       * @todo tmp test*
       */
      //      standardSelectTest(from, session);
      //      standardAccumulatorQueryTest(session, from);
      /**
       * @todo end*
       */

      /**
       * @todo tmp test*
       */
      try {
        standardSelectTest(session, from, to);
        standardAccumulatorQueryTest(session, from);
      } catch (Exception e) {
        log.info(e.getMessage());
      }
      /**
       * @todo end*
       */

      // alter the type to "to"
      boolean isCompatible = MetadataUtils.canAlter(from, to);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TABLE construct_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE construct_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
        } catch (StatementExecutionException e) {
          assertEquals(
              "701: New type " + to + " is not compatible with the existing one " + from,
              e.getMessage());
        }
      }

      // @todo tmp test
      System.out.println(
          "[testAlignDeviceUnSequenceDataQuery] AFTER ALTER COLUMN s1 SET DATA TYPE ");

      // If don't execute the flush" operation, verify if result can get valid value, not be null
      // when query memtable.
      //      session.executeNonQueryStatement("FLUSH");

      TSDataType newType = isCompatible ? to : from;
      /**
       * @todo tmp test*
       */
      //      standardSelectTestAfterAlterColumnType(from, session, newType);

      // Accumulator query test
      //      standardAccumulatorQueryTest(session, newType);
      /**
       * @todo end*
       */
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

      session.executeNonQueryStatement("DROP TABLE construct_and_alter_column_type");
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
      TSDataType from, ITableSession session, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    /**
     * @todo tmp test*
     */
    //    if (from == TSDataType.DATE && newType.isCompatible(from)) {
    if (from == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }
    /**
     * @todo test*
     */

    // Value Filter Test
    // 1.satisfy the condition of value filter completely
    SessionDataSet dataSet1;
    // @todo tmp
    //    if (newType == TSDataType.STRING) {
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= '1' and s2 > '2' order by time");
      } else if (from == TSDataType.BLOB) {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2' order by time");
      } else if (from == TSDataType.BOOLEAN) {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) = 'true' and cast(s2 as TEXT) = 'true' order by time");
      } else {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= '1' and s2 > 2 order by time");
      }
      // @todo tmp test
      //    } else if (newType == TSDataType.BLOB || from == TSDataType.DATE) {
    } else if (newType == TSDataType.BLOB) {
      //      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT || from == TSDataType.BLOB) {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2' order by time");
        //                "select time, s1 from construct_and_alter_column_type where cast(s1 as
        // TEXT) >= '1' and s2 > '2' order by time");
      } else {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as INT64) >= 1 and cast(s2 as INT64) > 2 order by time");
      }
    } else if (newType == TSDataType.BOOLEAN) {
      dataSet1 =
          session.executeQueryStatement(
              "select time, s1 from construct_and_alter_column_type where s1 = true and s2 = true order by time");
    } else {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= '1' and s2 > '2' order by time");
      } else {
        dataSet1 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= 1 and s2 > 2 order by time");
      }
    }
    //            "select time, s1 from construct_and_alter_column_type order by time limit 2 offset
    // 1");
    RowRecord rec1;
    // @todo tmp test
    //    for (int i = (from == TSDataType.DATE) ? 1 : 2; i <= ((from == TSDataType.DATE) ? 2 : 3);
    // i++) {
    for (int i = 2; i <= 3; i++) {
      rec1 = dataSet1.next();
      if (from != TSDataType.BOOLEAN) {
        assertEquals(i, rec1.getFields().get(0).getLongV());
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec1.getFields().get(1).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec1.getFields().get(1).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          if (from == TSDataType.DATE) {
            assertEquals(
                genValue(from, i).toString(),
                TSDataType.getDateStringValue(
                    Integer.parseInt(rec1.getFields().get(1).toString())));
          } else {
            assertEquals(String.valueOf(genValue(from, i)), rec1.getFields().get(1).toString());
          }
        } else {
          assertEquals(genValue(newType, i).toString(), rec1.getFields().get(1).toString());
        }
      }
    }
    dataSet1.close();

    // 2.satisfy the condition of value filter partially
    SessionDataSet dataSet2;
    //    if (newType == TSDataType.STRING || from == TSDataType.TEXT) {
    // @todo tmp test
    //    if (newType == TSDataType.STRING) {
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet2 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= '1' or s2 < '2' order by time");
      } else if (from == TSDataType.BLOB) {
        dataSet2 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) >= '1' or cast(s2 as TEXT) < '2' order by time");
      } else if (from == TSDataType.BOOLEAN) {
        dataSet2 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) = 'true' or cast(s2 as TEXT) = 'false' order by time");
      } else {
        dataSet2 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= '1' or s2 < 2 order by time");
      }
      // @todo tmp test
      //    } else if (newType == TSDataType.BLOB || from == TSDataType.DATE) {
    } else if (newType == TSDataType.BLOB) {
      //      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      if (from == TSDataType.STRING || from == TSDataType.TEXT || from == TSDataType.BLOB) {
        dataSet2 =
            session.executeQueryStatement(
                //                "select time, s1 from construct_and_alter_column_type where
                // cast(s1 as TEXT) >= '1' or s2 < '2' order by time");
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) >= '1' or cast(s2 as TEXT) < '2' order by time");
      } else {
        dataSet2 =
            session.executeQueryStatement(
                //                    "select time, s1 from construct_and_alter_column_type where
                // cast(s1 as TEXT) >= '1' or s2 < 2 order by time");
                "select time, s1 from construct_and_alter_column_type where cast(s1 as INT64) >= 1 or cast(s2 as INT64) < 2 order by time");
      }
    } else if (newType == TSDataType.BOOLEAN) {
      dataSet2 =
          session.executeQueryStatement(
              "select time, s1 from construct_and_alter_column_type where s1 = true or s2 = false order by time");
    } else {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet2 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= '1' or s2 < '2' order by time");
      } else {
        dataSet2 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 >= 1 or s2 < 2 order by time");
      }
    }

    RowRecord rec2;
    for (int i = 1; i <= 2; i++) {
      rec2 = dataSet2.next();
      if (from != TSDataType.BOOLEAN) {
        assertEquals(i, rec2.getFields().get(0).getLongV());
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i), rec2.getFields().get(1).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i), rec2.getFields().get(1).getDateV());
        } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          if (from == TSDataType.DATE) {
            assertEquals(
                genValue(from, i).toString(),
                TSDataType.getDateStringValue(
                    Integer.parseInt(rec2.getFields().get(1).toString())));
          } else {
            assertEquals(String.valueOf(genValue(from, i)), rec2.getFields().get(1).toString());
          }
        } else {
          assertEquals(genValue(newType, i).toString(), rec2.getFields().get(1).toString());
        }
      }
    }
    dataSet2.close();

    // 3.can't satisfy the condition of value filter at all
    SessionDataSet dataSet3;
    //    if (newType == TSDataType.STRING || from == TSDataType.TEXT) {
    /***@todo tmp test**/
    //    if (newType == TSDataType.STRING) {
    //      dataSet3 =
    //          session.executeQueryStatement(
    //              "select time, s1 from construct_and_alter_column_type where s1 < '1' order by
    // time");
    //      //    } else if (newType == TSDataType.BLOB || from == TSDataType.DATE) {
    //    } else if (newType == TSDataType.BLOB || from == TSDataType.DATE || from ==
    // TSDataType.BLOB) {
    /***@todo tmp test end**/
    /**
     * @todo tmp test start*
     */
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
      if (from == TSDataType.BLOB) {
        dataSet3 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) < '1' order by time");
      } else if (from == TSDataType.BOOLEAN) {
        dataSet3 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) = 'false' order by time");
      } else {
        dataSet3 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 < '1' order by time");
      }
    } else if (newType == TSDataType.BLOB || from == TSDataType.BLOB) {
      /**
       * @todo tmp test end*
       */
      dataSet3 =
          session.executeQueryStatement(
              "select time, s1 from construct_and_alter_column_type where cast(s1 as TEXT) < '1' order by time");
    } else if (newType == TSDataType.BOOLEAN) {
      dataSet3 =
          session.executeQueryStatement(
              "select time, s1 from construct_and_alter_column_type where s1 = false order by time");
    } else {
      if (from == TSDataType.STRING || from == TSDataType.TEXT) {
        dataSet3 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 < '1' order by time");
      } else {
        dataSet3 =
            session.executeQueryStatement(
                "select time, s1 from construct_and_alter_column_type where s1 < 1 order by time");
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
            "select time, s1 from construct_and_alter_column_type where time > 1 order by time");
    RowRecord rec4;
    for (int i = 2; i <= 3; i++) {
      rec4 = dataSet4.next();
      assertEquals(i, rec4.getFields().get(0).getLongV());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, i), rec4.getFields().get(1).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, i), rec4.getFields().get(1).getDateV());
      } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
        if (from == TSDataType.DATE) {
          assertEquals(
              genValue(from, i).toString(),
              TSDataType.getDateStringValue(Integer.parseInt(rec4.getFields().get(1).toString())));
        } else {
          assertEquals(String.valueOf(genValue(from, i)), rec4.getFields().get(1).toString());
        }
      } else {
        assertEquals(genValue(newType, i).toString(), rec4.getFields().get(1).toString());
      }
    }
    dataSet4.close();
  }

  //  private static void standardSelectTest(TSDataType from, ITableSession session) @todo tmp test
  private static void standardSelectTest(ITableSession session, TSDataType from, TSDataType to)
      throws StatementExecutionException, IoTDBConnectionException {
    /**
     * @todo tmp test*
     */
    //    if (from == TSDataType.DATE && !to.isCompatible(from)) {
    if (from == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }
    /**
     * @todo tmp test end*
     */

    // Value Filter Test
    // 1.satisfy the condition of value filter completely
    SessionDataSet dataSet1;
    if (from == TSDataType.STRING || from == TSDataType.TEXT) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 >= '1' and s2 > '2' order by time");
    } else if (from == TSDataType.BLOB) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where cast(s1 as TEXT) >= '1' and cast(s2 as TEXT) > '2' order by time");
      /**
       * @todo test**
       */
      //    } else if (from == TSDataType.DATE) {
      //      dataSet1 =
      //          session.executeQueryStatement(
      //              //              "select s1, s2 from construct_and_alter_column_type where
      // cast(s1 as
      //              // INT64) >= 1 and cast(s2 as INT64) > 2 order by time");
      //              "select s1, s2 from construct_and_alter_column_type where s1 >=
      // TO_TIMESTAMP('1970-01-02', 'yyyy-MM-dd') and s2 > TO_TIMESTAMP('1970-01-02', 'yyyy-MM-dd')
      // order by time");
      /**
       * @todo test end**
       */
    } else if (from == TSDataType.BOOLEAN) {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 = true and s2 = true order by time");
    } else {
      dataSet1 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 >= 1 and s2 > 2 order by time");
    }
    RowRecord rec1;
    // @todo tmp test
    //    for (int i = (from == TSDataType.DATE) ? 1 : 2; i <= ((from == TSDataType.DATE) ? 2 : 3);
    // i++) {
    for (int i = 2; i <= 3; i++) {
      rec1 = dataSet1.next();
      System.out.println("rec1: " + rec1.toString());
      if (from != TSDataType.BOOLEAN) {
        //        assertEquals(genValue(TSDataType.INT64, i),
        // rec1.getFields().get(0).getObjectValue(TSDataType.INT64));
        //        assertEquals(genValue(TSDataType.INT64, i * 2),
        // rec1.getFields().get(1).getObjectValue(TSDataType.INT64));
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
              "select s1, s2 from construct_and_alter_column_type where s1 >= '1' or s2 < '2' order by time");
    } else if (from == TSDataType.BLOB) {
      /**
       * @todo tmp test *
       */
      //      dataSet2 =
      //          session.executeQueryStatement(
      //              "select s1, s2 from construct_and_alter_column_type where cast(s1 as TEXT) >=
      // '1' or cast(s2 as TEXT) < '2' order by time");
      //    } else if (from == TSDataType.DATE) {
      /**
       * @todo tmp test end *
       */
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where cast(s1 as TEXT) >= '1' or cast(s2 as TEXT) < '2' order by time");
    } else if (from == TSDataType.BOOLEAN) {
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 = true or s2 = false order by time");
    } else {
      dataSet2 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 >= 1 or s2 < 2 order by time");
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
              "select s1, s2 from construct_and_alter_column_type where s1 < '1' order by time");
    } else if (from == TSDataType.BLOB) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where cast(s1 as TEXT) < '1' order by time");
      /**
       * @todo tmp test*
       */
      //    } else if (from == TSDataType.DATE) {
      //      dataSet3 =
      //          session.executeQueryStatement(
      //              "select s1, s2 from construct_and_alter_column_type where cast(s1 as TEXT) <
      // '1' order by time");
      /**
       * @todo tmp test end*
       */
    } else if (from == TSDataType.BOOLEAN) {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 = false order by time");
    } else {
      dataSet3 =
          session.executeQueryStatement(
              "select s1, s2 from construct_and_alter_column_type where s1 < 1 order by time");
    }
    if (from != TSDataType.BOOLEAN) {
      assertFalse(dataSet3.hasNext());
    }
    dataSet3.close();

    // Time filter
    // 1.satisfy the condition of time filter
    SessionDataSet dataSet4 =
        session.executeQueryStatement(
            "select s1, s2 from construct_and_alter_column_type where time > 1 order by time");
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

  private static void standardAccumulatorQueryTest(ITableSession session, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    /**
     * @todo tmp test*
     */
    if (newType == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }
    /**
     * @todo tmp test end*
     */
    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select min(s1),max(s1),first(s1),last(s1) from construct_and_alter_column_type");
    RowRecord rec = dataSet.next();
    int[] expectedValue = {1, 1024, 1, 1024};
    if (newType == TSDataType.STRING || newType == TSDataType.TEXT || newType == TSDataType.BLOB) {
      expectedValue[1] = 999;
    } else if (newType == TSDataType.BOOLEAN) {
      expectedValue = new int[] {19700102, 19721021, 19700102, 19721021};
    }
    if (newType != TSDataType.BOOLEAN) {
      for (int i = 0; i < 4; i++) {
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getDateV());
        } else {
          // @todo tmp test
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
                "select avg(s1),sum(s1) from construct_and_alter_column_type");
        rec = dataSet.next();
        assertEquals(512.5, rec.getFields().get(0).getDoubleV(), 0.001);
        assertEquals(524800.0, rec.getFields().get(1).getDoubleV(), 0.001);
        assertFalse(dataSet.hasNext());
      }
    }

    // can use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from construct_and_alter_column_type where time > 0");
    rec = dataSet.next();
    assertEquals(1024, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());

    // can't use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from construct_and_alter_column_type where time > 10000");
    rec = dataSet.next();
    assertEquals(0, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());
  }

  private static void standardAccumulatorQueryTest(
      ITableSession session, TSDataType from, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    if (from == TSDataType.DATE) {
      throw new NotSupportedException("Not supported DATE type.");
    }
    if (from == TSDataType.BOOLEAN
        && (newType == TSDataType.STRING || newType == TSDataType.TEXT)) {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select first(s1),last(s1) from construct_and_alter_column_type");
      RowRecord rec = dataSet.next();
      boolean[] expectedValue = {false, true};
      for (int i = 0; i < 2; i++) {
        if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          assertEquals(String.valueOf(expectedValue[i]), rec.getFields().get(i).toString());
        }
      }
    } else {
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "select min(s1),max(s1),first(s1),last(s1) from construct_and_alter_column_type");
      RowRecord rec = dataSet.next();
      int[] expectedValue = {1, 1024, 1, 1024};
      if (newType == TSDataType.STRING
          || newType == TSDataType.TEXT
          || newType == TSDataType.BLOB) {
        expectedValue[1] = 999;
      } else if (newType == TSDataType.BOOLEAN) {
        expectedValue = new int[] {19700102, 19721021, 19700102, 19721021};
      }
      if (newType != TSDataType.BOOLEAN) {
        for (int i = 0; i < 4; i++) {
          if (newType == TSDataType.BLOB) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getBinaryV());
          } else if (newType == TSDataType.DATE) {
            assertEquals(genValue(newType, expectedValue[i]), rec.getFields().get(i).getDateV());
          } else if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
            if (from == TSDataType.DATE) {
              log.info(
                  "i is {}, expected value: {}, actual value: {}",
                  i,
                  TSDataType.getDateStringValue(expectedValue[i]),
                  rec.getFields().get(i).toString());
              assertEquals(
                  TSDataType.getDateStringValue(expectedValue[i]),
                  rec.getFields().get(i).toString());
            } else {
              log.info(
                  "i is {}, expected value: {}, actual value: {}",
                  i,
                  genValue(from, expectedValue[i]).toString(),
                  rec.getFields().get(i).toString());
              assertEquals(
                  genValue(from, expectedValue[i]).toString(), rec.getFields().get(i).toString());
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
                  "select avg(s1),sum(s1) from construct_and_alter_column_type");
          rec = dataSet.next();
          assertEquals(512.5, rec.getFields().get(0).getDoubleV(), 0.001);
          assertEquals(524800.0, rec.getFields().get(1).getDoubleV(), 0.001);
          assertFalse(dataSet.hasNext());
        }
      }

      // can use statistics information
      dataSet =
          session.executeQueryStatement(
              "select count(*) from construct_and_alter_column_type where time > 0");
      rec = dataSet.next();
      assertEquals(1024, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());

      // can't use statistics information
      dataSet =
          session.executeQueryStatement(
              "select count(*) from construct_and_alter_column_type where time > 10000");
      rec = dataSet.next();
      assertEquals(0, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }
  }

  private static void accumulatorQueryTestForDateType(ITableSession session, TSDataType newType)
      throws StatementExecutionException, IoTDBConnectionException {
    if (newType != TSDataType.STRING && newType != TSDataType.TEXT) {
      return;
    }

    log.info("Test the result that after transfered newType:");

    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select first(s1),last(s1) from construct_and_alter_column_type");
    RowRecord rec = dataSet.next();
    int[] expectedValue = {19700102, 19721021};
    if (newType != TSDataType.BOOLEAN) {
      for (int i = 0; i < 2; i++) {
        if (newType == TSDataType.STRING || newType == TSDataType.TEXT) {
          log.info(
              "i is {}, expected value: {}, actual value: {}",
              i,
              TSDataType.getDateStringValue(expectedValue[i]),
              rec.getFields().get(i).toString());
          assertEquals(
              TSDataType.getDateStringValue(expectedValue[i]), rec.getFields().get(i).toString());
        }
      }
    }

    // can use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from construct_and_alter_column_type where time > 0");
    rec = dataSet.next();
    assertEquals(1024, rec.getFields().get(0).getLongV());
    assertFalse(dataSet.hasNext());

    // can't use statistics information
    dataSet =
        session.executeQueryStatement(
            "select count(*) from construct_and_alter_column_type where time > 10000");
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

  public static String getDateStringValue(int value) {
    //    if (value < 19700101) {
    //      return String.valueOf(value);
    //    }
    return String.format("%04d-%02d-%02d", value / 10000, (value % 10000) / 100, value % 100);
  }
}
