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

package org.apache.iotdb.relational.it.query.recent;

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
public class IoTDBTableFunctionIT {
  private static final String DATABASE_NAME = "test";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(s1 INT32 FIELD)",
        "INSERT INTO table1(time,s1) values(1, 1)",
        "INSERT INTO table1(time,s1) values(2, 2)",
        "INSERT INTO table1(time,s1) values(3, 3)",
        "flush",
        "INSERT INTO table1(time,s1) values(4, 4)",
        "INSERT INTO table1(time,s1) values(5, 5)",
        "INSERT INTO table1(time,s1) values(6, 6)",
        "flush",
        "INSERT INTO table1(time,s1) values(7, 7)",
        "INSERT INTO table1(time,s1) values(8, 8)",
        "INSERT INTO table1(time,s1) values(9, 9)",
        "flush",
      };

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(4);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void sessionGroupNode2SortNodeTest() {
    String[] expectedHeader = new String[] {"window_start", "window_end", "TIME", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.003Z,3,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.004Z,4,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.005Z,5,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.006Z,6,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.007Z,7,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.008Z,8,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.009Z,1970-01-01T00:00:00.009Z,9,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM SESSION (DATA => (SELECT TIME, s1 FROM table1 ORDER BY TIME DESC LIMIT 10) ORDER BY TIME, GAP => 2s)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
