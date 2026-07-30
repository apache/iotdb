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

package org.apache.iotdb.relational.it.query.recent.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCastLiteralFunctionIT {

  private static final String DATABASE_NAME = "test_cast_float_literal";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table_a(time time, device string tag, s_int int32 field)",
        "INSERT INTO table_a(time, device, s_int) VALUES  (1, 'd1', 100)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void TestLiteralCast() {
    String[] expectedHeader = {"_col0"};

    tableResultSetEqualTest(
        "select cast(1.1 as float) from table_a",
        expectedHeader,
        new String[] {"1.1,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(1.1 as double) from table_a",
        expectedHeader,
        new String[] {"1.1,"},
        DATABASE_NAME);

    // case: 1.9 -> 2 (INT32)
    tableResultSetEqualTest(
        "select cast(1.9 as int32) from table_a",
        expectedHeader,
        new String[] {"2,"},
        DATABASE_NAME);

    // case: 1.9 -> 2 (INT64)
    tableResultSetEqualTest(
        "select cast(1.9 as int64) from table_a",
        expectedHeader,
        new String[] {"2,"},
        DATABASE_NAME);

    // String -> numeric (Parsing)
    tableResultSetEqualTest(
        "select cast('3.14159' as double) from table_a",
        expectedHeader,
        new String[] {"3.14159,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast('100' as int32) from table_a",
        expectedHeader,
        new String[] {"100,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(123.456 as string) from table_a",
        expectedHeader,
        new String[] {"123.456,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(123 as text) from table_a",
        expectedHeader,
        new String[] {"123,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(1 as boolean) from table_a",
        expectedHeader,
        new String[] {"true,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(0 as boolean) from table_a",
        expectedHeader,
        new String[] {"false,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast('true' as boolean) from table_a",
        expectedHeader,
        new String[] {"true,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(s_int as float) from table_a",
        expectedHeader,
        new String[] {"100.0,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select cast(s_int as string) from table_a",
        expectedHeader,
        new String[] {"100,"},
        DATABASE_NAME);
  }
}
