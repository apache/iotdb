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
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.relational.it.session.IoTDBSessionRelationalIT.genValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlterColumnTypeIT {

  @BeforeClass
  public static void setUp() throws Exception {
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
        doWriteAndAlter(from, to, false);
        doWriteAndAlter(from, to, true);
      }
    }
  }

  private void doWriteAndAlter(TSDataType from, TSDataType to, boolean flush)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnectionWithDB("test")) {
      // create a table with type of "from"
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS write_and_alter_column_type (s1 " + from + ")");

      // write a point of "from"
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

      if (flush) {
        session.executeNonQueryStatement("FLUSH");
      }

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
      RowRecord rec = dataSet.next();
      TSDataType newType = isCompatible ? to : from;
      assertEquals(1, rec.getFields().get(0).getLongV());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, 1), rec.getFields().get(1).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, 1), rec.getFields().get(1).getDateV());
      } else {
        assertEquals(genValue(newType, 1).toString(), rec.getFields().get(1).toString());
      }

      // write a point
      tablet =
          new Tablet(
              "write_and_alter_column_type",
              Collections.singletonList("s1"),
              Collections.singletonList(newType),
              Collections.singletonList(ColumnCategory.FIELD));
      tablet.addTimestamp(0, 2);
      tablet.addValue("s1", 0, genValue(newType, 2));
      session.insert(tablet);
      tablet.reset();

      if (flush) {
        session.executeNonQueryStatement("FLUSH");
      }

      dataSet =
          session.executeQueryStatement("select * from write_and_alter_column_type order by time");
      rec = dataSet.next();
      assertEquals(1, rec.getFields().get(0).getLongV());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, 1), rec.getFields().get(1).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, 1), rec.getFields().get(1).getDateV());
      } else {
        assertEquals(genValue(newType, 1).toString(), rec.getFields().get(1).toString());
      }

      rec = dataSet.next();
      assertEquals(2, rec.getFields().get(0).getLongV());
      if (newType == TSDataType.BLOB) {
        assertEquals(genValue(newType, 2), rec.getFields().get(1).getBinaryV());
      } else if (newType == TSDataType.DATE) {
        assertEquals(genValue(newType, 2), rec.getFields().get(1).getDateV());
      } else {
        assertEquals(genValue(newType, 2).toString(), rec.getFields().get(1).toString());
      }

      dataSet =
          session.executeQueryStatement(
              "select min(s1),max(s1),first(s1),last(s1) from write_and_alter_column_type");
      rec = dataSet.next();
      for (int i = 0; i < 4; i++) {
        if (newType == TSDataType.BLOB) {
          assertEquals(genValue(newType, i % 2 + 1), rec.getFields().get(i).getBinaryV());
        } else if (newType == TSDataType.DATE) {
          assertEquals(genValue(newType, i % 2 + 1), rec.getFields().get(i).getDateV());
        } else {
          assertEquals(genValue(newType, i % 2 + 1).toString(), rec.getFields().get(i).toString());
        }
      }
      assertFalse(dataSet.hasNext());

      if (newType.isNumeric()) {
        dataSet =
            session.executeQueryStatement(
                "select avg(s1),sum(s1) from write_and_alter_column_type");
        rec = dataSet.next();
        assertEquals(1.5, rec.getFields().get(0).getDoubleV(), 0.001);
        assertEquals(3.0, rec.getFields().get(1).getDoubleV(), 0.001);
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
}
