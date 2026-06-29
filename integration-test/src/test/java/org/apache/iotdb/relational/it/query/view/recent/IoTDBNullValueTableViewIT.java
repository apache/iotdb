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

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNullValueTableViewIT {
  private static final String TREE_DB_NAME = "root.test";
  private static final String DATABASE_NAME = "test";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + TREE_DB_NAME,
        "create timeseries root.test.table1.d1.s1 string",
        "insert into root.test.table1.d1(time,s1) values(0, null), (1, 1)",
        "flush",
        "insert into root.test.table1.d1(time,s1) values(0, 0)",
        "flush",
        "create aligned timeseries root.test.table2.d1(s1 string)",
        "insert into root.test.table2.d1(time,s1) aligned values(0, 0)",
        "insert into root.test.table2.d1(time,s1) aligned values(1, 1)",
      };

  private static final String[] createTableViewSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create view table1(id1 tag, s1 string) as root.test.table1.**",
        "create view table2(id1 tag, s1 float) as root.test.table2.**",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createSqls);
    prepareTableData(createTableViewSqls);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void nullTest() {

    // case 1: all without time filter using previous fill without timeDuration
    String[] expectedHeader = new String[] {"time", "id1", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d1,0,", "1970-01-01T00:00:00.001Z,d1,1,",
        };
    tableResultSetEqualTest("select * from table1", expectedHeader, retArray, DATABASE_NAME);
    // case 2: For aligned series, when the data types of all series in the view are inconsistent
    // with the data types of the actual series, the corresponding time can be queried, and other
    // columns are null values.
    retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d1,null,", "1970-01-01T00:00:00.001Z,d1,null,",
        };
    tableResultSetEqualTest("select * from table2", expectedHeader, retArray, DATABASE_NAME);
  }
}
