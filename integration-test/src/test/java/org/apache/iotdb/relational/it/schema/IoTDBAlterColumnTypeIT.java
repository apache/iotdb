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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
public class IoTDBAlterColumnTypeIT {

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
  public void testWriteAndAlter() throws IoTDBConnectionException, StatementExecutionException {
    Set<TSDataType> typesToTest = new HashSet<>();
    Collections.addAll(typesToTest, TSDataType.values());
    typesToTest.remove(TSDataType.VECTOR);
    typesToTest.remove(TSDataType.UNKNOWN);

    //    doWriteAndAlter(TSDataType.INT32, TSDataType.FLOAT, false);
    //    doWriteAndAlter(TSDataType.INT32, TSDataType.FLOAT, true);

    for (TSDataType from : typesToTest) {
      for (TSDataType to : typesToTest) {
        System.out.printf("testing %s to %s%n", from, to);
        doWriteAndAlter(from, to);
      }
    }
  }

  private void doWriteAndAlter(TSDataType from, TSDataType to)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
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

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from write_and_alter_column_type order by time");
      RowRecord rec;
      TSDataType newType = isCompatible ? to : from;
      for (int i = 1; i <= 2; i++) {
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

    for (TSDataType from : typesToTest) {
      for (TSDataType to : typesToTest) {
        System.out.printf("testing %s to %s%n", from, to);
        doAlterWithoutWrite(from, to, false);
        doAlterWithoutWrite(from, to, true);
      }
    }
  }

  private void doAlterWithoutWrite(TSDataType from, TSDataType to, boolean flush)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
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
      session.executeNonQueryStatement("SET CONFIGURATION enable_seq_space_compaction='false'");
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

    filesToLoad.forEach(File::delete);
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
              schema1.getTableName(),
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
      assertEquals(18, rec.getFields().get(0).getLongV());
      assertFalse(dataSet.hasNext());
    }

    // alter s1 to double
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      session.executeNonQueryStatement(
          "ALTER TABLE load_and_alter ALTER COLUMN s1 SET DATA TYPE DOUBLE");
    }

    filesToLoad.forEach(File::delete);
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

    filesToLoad.forEach(File::delete);
  }
}
