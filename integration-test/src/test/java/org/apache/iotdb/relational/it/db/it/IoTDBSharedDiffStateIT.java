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

package org.apache.iotdb.relational.it.db.it;

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

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSharedDiffStateIT {
  private static final String DATABASE = "shared_diff_state";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(new String[] {"CREATE DATABASE " + DATABASE});
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testProjectionOrderDoesNotChangeCrossBlockState() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE projection_state (id STRING TAG, x DOUBLE FIELD, f BOOLEAN FIELD)",
          "INSERT INTO projection_state(time,id,x,f) VALUES "
              + "(1,'d',1,true),(2,'d',2,true),(3,'d',3,true)",
          "FLUSH",
          "INSERT INTO projection_state(time,id,x,f) VALUES "
              + "(4,'d',4,true),(5,'d',5,true),(6,'d',6,true)",
          "FLUSH",
          "INSERT INTO projection_state(time,id,x,f) VALUES (7,'d',7,true)",
          "FLUSH"
        });
    for (boolean caseFirst : new boolean[] {false, true}) {
      String diff = "diff(x,f) AS d";
      String caseResult = "CASE WHEN x = 7 THEN diff(x,f) ELSE 100.0 END AS a";
      String[] expected = new String[7];
      for (int i = 0; i < expected.length; i++) {
        String d = i == 0 ? "null" : "1.0";
        String a = i == 6 ? "1.0" : "100.0";
        expected[i] = caseFirst ? a + "," + d + "," : d + "," + a + ",";
      }
      tableResultSetEqualTest(
          "SELECT "
              + (caseFirst ? caseResult + "," + diff : diff + "," + caseResult)
              + " FROM projection_state ORDER BY time",
          caseFirst ? new String[] {"a", "d"} : new String[] {"d", "a"},
          expected,
          DATABASE);
    }
  }

  @Test
  public void testAdditionalOrTermDoesNotRemoveRowsAcrossBlocks() {
    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "CREATE TABLE filter_state (id STRING TAG, x DOUBLE FIELD, f BOOLEAN FIELD)",
          "INSERT INTO filter_state(time,id,x,f) VALUES "
              + "(1,'d',1,true),(2,'d',3,true),(3,'d',4,true)",
          "FLUSH",
          "INSERT INTO filter_state(time,id,x,f) VALUES (4,'d',10,true),(5,'d',11,true)",
          "FLUSH"
        });
    for (String predicate :
        new String[] {
          "diff(x,f) IS NOT NULL",
          "diff(x,f) IS NOT NULL OR CASE WHEN x > 5 THEN diff(x,f) > 0 ELSE false END"
        }) {
      tableResultSetEqualTest(
          "SELECT x FROM filter_state WHERE " + predicate + " ORDER BY time",
          new String[] {"x"},
          new String[] {"3.0,", "4.0,", "10.0,", "11.0,"},
          DATABASE);
    }
  }

  @Test
  public void testUpdateSetOrderDoesNotWriteMaskedNulls() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE update_state (id STRING TAG, src STRING ATTRIBUTE, "
              + "a STRING ATTRIBUTE, b STRING ATTRIBUTE, v DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO update_state(time,id,src,a,b,v) VALUES "
              + "(1,'d0','1','seed','seed',1),(2,'d1','9','seed','seed',1),"
              + "(3,'d2','5','seed','seed',1)");
      for (boolean diffFirst : new boolean[] {false, true}) {
        statement.execute("UPDATE update_state SET a = 'seed', b = 'seed'");
        String a = "a = CAST(CAST(src AS DOUBLE) AS STRING)";
        String b = "b = CAST(diff(CAST(src AS DOUBLE), src = 'x') AS STRING)";
        statement.execute(
            "UPDATE update_state SET "
                + (diffFirst ? b + "," + a : a + "," + b)
                + " WHERE CASE WHEN src = 'n/a' THEN false ELSE "
                + "diff(CAST(src AS DOUBLE), src = 'x') > 0 END");
        assertResultSetEqual(
            statement.executeQuery("SHOW DEVICES FROM update_state"),
            "id,src,a,b,",
            new HashSet<>(Arrays.asList("d0,1,seed,seed,", "d1,9,9.0,8.0,", "d2,5,seed,seed,")));
      }
    }
  }
}
