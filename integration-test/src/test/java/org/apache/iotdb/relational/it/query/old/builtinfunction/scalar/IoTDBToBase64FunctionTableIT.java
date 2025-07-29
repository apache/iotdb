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

package org.apache.iotdb.relational.it.query.old.builtinfunction.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Base64;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBToBase64FunctionTableIT {

  private static final String DATABASE_NAME = "db_base64";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE t1(device_id STRING TAG, s1 STRING FIELD, s2 TEXT FIELD, s3 BLOB FIELD, s4 STRING FIELD)",
        "INSERT INTO t1(time, device_id, s1, s2, s3, s4) VALUES (1, 'd1', 'iotdb', 'iotdb', X'0102030405', null)",
        "INSERT INTO t1(time, device_id, s1, s2, s3, s4) VALUES (2, 'd2', '', '', X'', null)",
        // Setup for unsupported types
        "CREATE TABLE t2(device_id STRING TAG, i INT32 FIELD, b BOOLEAN FIELD, f FLOAT FIELD, d DOUBLE FIELD, dt DATE FIELD, ts TIMESTAMP FIELD)",
        "INSERT INTO t2(time, device_id, i, b, f, d, dt, ts) VALUES (1, 'd1', 1, true, 1.1, 2.2, '2024-01-01', 123456789)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("ns");
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testToBase64() {
    String expectedStr =
        Base64.getEncoder().encodeToString("iotdb".getBytes(StandardCharsets.UTF_8));
    String expectedBlob = Base64.getEncoder().encodeToString(new byte[] {1, 2, 3, 4, 5});
    String expectedEmpty = "";
    tableResultSetEqualTest(
        "SELECT to_base64(s1), to_base64(s2), to_base64(s3), to_base64(s4) FROM t1 WHERE time = 1",
        new String[] {"_col0", "_col1", "_col2", "_col3"},
        new String[] {expectedStr + "," + expectedStr + "," + expectedBlob + ",null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT to_base64(s1), to_base64(s2), to_base64(s3), to_base64(s4) FROM t1 WHERE time = 2",
        new String[] {"_col0", "_col1", "_col2", "_col3"},
        new String[] {expectedEmpty + "," + expectedEmpty + "," + expectedEmpty + ",null,"},
        DATABASE_NAME);
  }

  @Test
  public void testToBase64UnsupportedTypes() {
    // Assert that to_base64 on unsupported types throws an error
    tableAssertTestFail(
        "SELECT to_base64(i) FROM t2",
        "701: Scalar function to_base64 only accepts one argument and it must be STRING, TEXT or BLOB data type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64(b) FROM t2",
        "701: Scalar function to_base64 only accepts one argument and it must be STRING, TEXT or BLOB data type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64(f) FROM t2",
        "701: Scalar function to_base64 only accepts one argument and it must be STRING, TEXT or BLOB data type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64(d) FROM t2",
        "701: Scalar function to_base64 only accepts one argument and it must be STRING, TEXT or BLOB data type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64(dt) FROM t2",
        "701: Scalar function to_base64 only accepts one argument and it must be STRING, TEXT or BLOB data type.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT to_base64(ts) FROM t2",
        "701: Scalar function to_base64 only accepts one argument and it must be STRING, TEXT or BLOB data type.",
        DATABASE_NAME);
  }
}
