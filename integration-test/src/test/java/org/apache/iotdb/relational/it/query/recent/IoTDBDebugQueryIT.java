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
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBDebugQueryIT {
  private static final String DATABASE_NAME = "test_db";
  private static final String[] createTableSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table table1(device string tag, value int32 field)",
        "insert into table1(time,device,value) values(2020-01-01 00:00:01.000,'d1',1)",
        "FLUSH",
      };
  private static final String[] createTreeSqls =
      new String[] {
        "create timeseries root.test.departments.department_id TEXT",
        "create timeseries root.test.departments.dep_name TEXT",
        "insert into root.test.departments(time, department_id, dep_name) values(1, 'D001', '研发部')",
        "FLUSH",
      };
  private static DataNodeWrapper dataNodeWrapper;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapperList().get(0);
    prepareTableData(createTableSqls);
    prepareData(createTreeSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void tableTest() throws IOException {
    // clear log content to reduce lines spanned in logContains check
    dataNodeWrapper.clearLogContent();

    String[] expectedHeader = new String[] {"time", "device", "value"};
    String[] retArray = new String[] {"2020-01-01T00:00:01.000Z,d1,1,"};
    String sql = "debug select time,device,value from table1";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    assertTrue(dataNodeWrapper.logContains("Cache miss: table1.d1"));
    assertTrue(dataNodeWrapper.logContains(sql));
  }

  @Test
  public void treeTest() throws IOException {
    // clear log content to reduce lines spanned in logContains check
    dataNodeWrapper.clearLogContent();

    String[] expectedHeader =
        new String[] {
          "Time", "root.test.departments.department_id", "root.test.departments.dep_name"
        };
    String[] retArray = new String[] {"1,D001,研发部,"};
    String sql = "debug select department_id, dep_name from root.test.departments";
    resultSetEqualTest(sql, expectedHeader, retArray);

    assertTrue(dataNodeWrapper.logContains("Cache miss: root.test.departments"));
    assertTrue(dataNodeWrapper.logContains(sql));
  }
}
