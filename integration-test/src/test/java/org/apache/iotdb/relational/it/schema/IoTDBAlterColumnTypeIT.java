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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.relational.it.session.IoTDBSessionRelationalIT.genValue;
import static org.junit.Assert.assertEquals;

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
    for (TSDataType from : TSDataType.values()) {
      for (TSDataType to : TSDataType.values()) {
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
      session.executeNonQueryStatement(
          "INSERT INTO write_and_alter_column_type (time, s1) VALUES (1, "
              + genValue(from, 1)
              + ")");

      if (flush) {
        session.executeNonQueryStatement("FLUSH");
      }

      // alter the type to "to"
      boolean isCompatible = to.isCompatible(from);
      if (isCompatible) {
        session.executeNonQueryStatement(
            "ALTER TABLE write_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
      } else {
        try {
          session.executeNonQueryStatement(
              "ALTER TABLE write_and_alter_column_type ALTER COLUMN s1 SET DATA TYPE " + to);
        } catch (StatementExecutionException e) {
          assertEquals("", e.getMessage());
        }
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from write_and_alter_column_type order by time");
      RowRecord rec = dataSet.next();
      assertEquals(1, rec.getFields().get(0).getLongV());
      if (to == TSDataType.BLOB) {
        assertEquals(genValue(to, 1), rec.getFields().get(1).getBinaryV());
      } else if (to == TSDataType.DATE) {
        assertEquals(genValue(to, 1), rec.getFields().get(1).getDateV());
      } else {
        assertEquals(genValue(to, 1).toString(), rec.getFields().get(1).toString());
      }

      // write a point
      session.executeNonQueryStatement(
          "INSERT INTO write_and_alter_column_type (time, s1) VALUES (2, "
              + genValue(isCompatible ? to : from, 2)
              + ")");

      dataSet =
          session.executeQueryStatement("select * from write_and_alter_column_type order by time");
      rec = dataSet.next();
      assertEquals(1, rec.getFields().get(0).getLongV());
      TSDataType newType = isCompatible ? to : from;
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

      session.executeNonQueryStatement("DROP TABLE write_and_alter_column_type");
    }
  }
}
