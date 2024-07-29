/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.query.old.orderBy;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.relational.it.query.old.aligned.TableUtils.USE_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBStreamSortIT {
  private static final String DATABASE_NAME = "db";

  // the data can be viewed in
  // https://docs.google.com/spreadsheets/d/1OWA1bKraArCwWVnuTjuhJ5yLG0PFLdD78gD6FjquepI/edit#gid=0
  private static final String[] sql1 =
      new String[] {
        "CREATE DATABASE db",
        "USE db",
        "CREATE TABLE table0 (device string id, level string id, attr1 string attribute, attr2 string attribute, num int32 measurement, bigNum int64 measurement, "
            + "floatNum double measurement, str TEXT measurement, bool BOOLEAN measurement)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l1', 'c', 'd', 0,3,2947483648,231.2121,'coconut',FALSE)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l2', 'y', 'z', 20,2,2147483648,434.12,'pineapple',TRUE)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l3', 't', 'a', 40,1,2247483648,12.123,'apricot',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l4', 80,9,2147483646,43.12,'apple',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l5', 100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l1', 31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l2',31536000100,10,3147483648,231.55,'pumelo',FALSE)",
        "insert into table0(device,level,  time,num,bigNum,floatNum,str,bool) values('d1', 'l3',31536000500,4,2147493648,213.1,'peach',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l4',31536001000,5,2149783648,56.32,'orange',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l5',31536010000,7,2147983648,213.112,'lemon',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l1',31536100000,11,2147468648,54.121,'pitaya',FALSE)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l2', 'yy', 'zz', 41536000000,12,2146483648,45.231,'strawberry',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l3',41536000020,14,2907483648,231.34,'cherry',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l4',41536900000,13,2107483648,54.12,'lychee',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l5',51536000000,15,3147483648,235.213,'watermelon',TRUE)"
      };

  private static final String[] sql2 =
      new String[] {
        "insert into table0(device,level,attr1,attr2,time,num,bigNum,floatNum,str,bool) values('d2','l1','d','c',0,3,2947483648,231.2121,'coconut',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l2',20,2,2147483648,434.12,'pineapple',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',40,1,2247483648,12.123,'apricot',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',80,9,2147483646,43.12,'apple',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l1',31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device,level,attr1,time,num,bigNum,floatNum,str,bool) values('d2','l2', 'vv', 31536000100,10,3147483648,231.55,'pumelo',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',31536000500,4,2147493648,213.1,'peach',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',31536001000,5,2149783648,56.32,'orange',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',31536010000,7,2147983648,213.112,'lemon',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l1',31536100000,11,2147468648,54.121,'pitaya',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l2',41536000000,12,2146483648,45.231,'strawberry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',41536000020,14,2907483648,231.34,'cherry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',41536900000,13,2107483648,54.12,'lychee',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',51536000000,15,3147483648,235.213,'watermelon',TRUE)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setSortBufferSize(1024 * 1024L);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sql1) {
        statement.execute(sql);
      }
      for (String sql : sql2) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ordinal data
  public static final String[][] RES =
      new String[][] {
        {"0", "3", "2947483648", "231.2121", "coconut", "false"},
        {"20", "2", "2147483648", "434.12", "pineapple", "true"},
        {"40", "1", "2247483648", "12.123", "apricot", "true"},
        {"80", "9", "2147483646", "43.12", "apple", "false"},
        {"100", "8", "2147483964", "4654.231", "papaya", "true"},
        {"31536000000", "6", "2147483650", "1231.21", "banana", "true"},
        {"31536000100", "10", "3147483648", "231.55", "pumelo", "false"},
        {"31536000500", "4", "2147493648", "213.1", "peach", "false"},
        {"31536001000", "5", "2149783648", "56.32", "orange", "false"},
        {"31536010000", "7", "2147983648", "213.112", "lemon", "true"},
        {"31536100000", "11", "2147468648", "54.121", "pitaya", "false"},
        {"41536000000", "12", "2146483648", "45.231", "strawberry", "false"},
        {"41536000020", "14", "2907483648", "231.34", "cherry", "false"},
        {"41536900000", "13", "2107483648", "54.12", "lychee", "true"},
        {"51536000000", "15", "3147483648", "235.213", "watermelon", "true"},
      };

  private void checkHeader(ResultSetMetaData resultSetMetaData, String[] title)
      throws SQLException {
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(title[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  private void testNormalOrderBy(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(metaData, new String[] {"Time", "num", "bigNum", "floatNum", "str", "bool"});
        int i = 0;
        while (resultSet.next()) {

          long actualTime = resultSet.getLong(1);
          String actualNum = resultSet.getString(2);
          String actualBigNum = resultSet.getString(3);
          double actualFloatNum = resultSet.getDouble(4);
          String actualStr = resultSet.getString(5);
          String actualBool = resultSet.getString(6);

          assertEquals(Long.parseLong(RES[ans[i]][0]), actualTime);
          assertEquals(RES[ans[i]][1], actualNum);
          assertEquals(RES[ans[i]][2], actualBigNum);
          assertEquals(Double.parseDouble(RES[ans[i]][3]), actualFloatNum, 0.0001);
          assertEquals(RES[ans[i]][4], actualStr);
          assertEquals(RES[ans[i]][5], actualBool);
          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void sortAttributeTest() {
    String[] expectedHeader =
        new String[] {
          "time", "device", "level", "attr1", "attr2", "num", "bignum", "floatnum", "str", "bool"
        };
    String[] retArray =
        new String[] {
          "1971-01-01T00:01:40.000Z,d2,l1,d,c,11,2147468648,54.121,pitaya,false,",
          "1970-01-01T00:00:00.040Z,d1,l3,t,a,1,2247483648,12.123,apricot,true,",
          "1971-01-01T00:00:00.500Z,d1,l3,t,a,4,2147493648,213.1,peach,false,",
          "1971-04-26T17:46:40.020Z,d1,l3,t,a,14,2907483648,231.34,cherry,false,",
          "1970-01-01T00:00:00.020Z,d2,l2,vv,null,2,2147483648,434.12,pineapple,true,"
        };
    tableResultSetEqualTest(
        "select \"time\", \"device\", \"level\", \"attr1\", \"attr2\", \"num\", \"bignum\", \"floatnum\", \"str\", \"bool\" from table0 order by attr1, level desc, time asc offset 5 limit 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
