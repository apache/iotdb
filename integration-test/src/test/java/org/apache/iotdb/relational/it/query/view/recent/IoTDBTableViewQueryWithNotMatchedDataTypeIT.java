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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

@RunWith(Parameterized.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableViewQueryWithNotMatchedDataTypeIT {

  protected static final String DATABASE_NAME = "test";

  private final boolean aligned;
  private final String databaseName;
  private final String treeDatabaseName;

  protected static final String[] createTreeAlignedDataSqls =
      new String[] {
        "CREATE ALIGNED TIMESERIES root.db.battery.b1(current FLOAT)",
        "INSERT INTO root.db.battery.b1(time, current) aligned values (1, 1)",
      };

  protected static final String[] createTreeNonAlignedDataSqls =
      new String[] {
        "CREATE TIMESERIES root.db.battery.b1.current FLOAT",
        "INSERT INTO root.db.battery.b1(time, current) values (1, 1)",
      };

  protected static String[] createTableSqls(String databaseName, String treeDatabaseName) {
    return new String[] {
      "create database " + databaseName,
      "use " + databaseName,
      "CREATE VIEW view1 (battery TAG, current BLOB FIELD) as " + treeDatabaseName + ".battery.**",
    };
  }

  public IoTDBTableViewQueryWithNotMatchedDataTypeIT(boolean aligned) {
    this.aligned = aligned;
    databaseName = DATABASE_NAME + (aligned ? "_aligned" : "_non_aligned");
    treeDatabaseName = "root." + databaseName;
  }

  @Parameterized.Parameters(name = "aligned={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void prepareDataForCase() {
    String[] dataSqls = aligned ? createTreeAlignedDataSqls : createTreeNonAlignedDataSqls;
    String[] caseDataSqls = new String[dataSqls.length + 1];
    caseDataSqls[0] = "CREATE DATABASE " + treeDatabaseName;
    for (int i = 0; i < dataSqls.length; i++) {
      caseDataSqls[i + 1] = dataSqls[i].replace("root.db.", treeDatabaseName + ".");
    }
    prepareData(caseDataSqls);
    prepareTableData(createTableSqls(databaseName, treeDatabaseName));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // A BLOB view must exclude the FLOAT tree measurement for both aligned and non-aligned sources.
  @Test
  public void test() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + databaseName);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select * from view1 where current is not null");
      Assert.assertFalse(sessionDataSet.hasNext());
      sessionDataSet.close();
    }
  }
}
