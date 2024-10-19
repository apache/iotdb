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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

/** In this IT, table has more than one IDs and Attributes. */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBMultiIDsWithAttributesTableIT {
  private static final String DATABASE_NAME = "db";

  private static final String[] sql1 =
      new String[] {
        "CREATE DATABASE db",
        "USE db",
        "CREATE TABLE table0 (device string id, level string id, attr1 string attribute, attr2 string attribute, num int32 measurement, bigNum int64 measurement, "
            + "floatNum double measurement, str TEXT measurement, bool BOOLEAN measurement, date DATE measurement, blob BLOB measurement, ts TIMESTAMP measurement, stringV STRING measurement)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l1', 'c', 'd', 0,3,2947483648,231.2121,'coconut',FALSE)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool,blob,ts) values('d1', 'l2', 'y', 'z', 20,2,2147483648,434.12,'pineapple',TRUE,X'108DCD62',2024-09-24T06:15:35.000+00:00)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l3', 't', 'a', 40,1,2247483648,12.123,'apricot',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l4', 80,9,2147483646,43.12,'apple',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l5', 100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l1', 31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l2',31536000100,10,3147483648,231.55,'pumelo',FALSE)",
        "insert into table0(device,level,  time,num,bigNum,floatNum,str,bool) values('d1', 'l3',31536000500,4,2147493648,213.1,'peach',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool,blob,ts,stringV) values('d1', 'l4',31536001000,5,2149783648,56.32,'orange',FALSE,X'108DCD62',2024-09-15T06:15:35.000+00:00,'test-string1')",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool,blob,ts) values('d1', 'l5',31536010000,7,2147983648,213.112,'lemon',TRUE,X'108DCD63',2024-09-25T06:15:35.000+00:00)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l1',31536100000,11,2147468648,54.121,'pitaya',FALSE)",
        "insert into table0(device,level,attr1,attr2,time,num,bigNum,floatNum,str,bool) values('d2','l1','d','c',0,3,2947483648,231.2121,'coconut',FALSE)",
        "insert into table0(device,level,attr1,time,num,bigNum,floatNum,str,bool) values('d2','l2', 'vv', 31536000100,10,3147483648,231.55,'pumelo',FALSE)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l2', 'yy', 'zz', 41536000000,12,2146483648,45.231,'strawberry',FALSE)",
        "flush",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l3',41536000020,14,2907483648,231.34,'cherry',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l4',41536900000,13,2107483648,54.12,'lychee',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool,date) values('d1', 'l5',51536000000,15,3147483648,235.213,'watermelon',TRUE,'2022-01-01')"
      };

  private static final String[] sql2 =
      new String[] {
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l2',20,2,2147483648,434.12,'pineapple',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',40,1,2247483648,12.123,'apricot',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,blob,ts,stringV) values('d2','l4',80,9,2147483646,43.12,'apple',FALSE,X'108DCD63',2024-09-20T06:15:35.000+00:00,'test-string2')",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l1',31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',31536000500,4,2147493648,213.1,'peach',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',31536001000,5,2149783648,56.32,'orange',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',31536010000,7,2147983648,213.112,'lemon',TRUE)",
        "flush",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,ts,StringV) values('d2','l1',31536100000,11,2147468648,54.121,'pitaya',FALSE,2024-08-01T06:15:35.000+00:00,'test-string3')",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l2',41536000000,12,2146483648,45.231,'strawberry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',41536000020,14,2907483648,231.34,'cherry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',41536900000,13,2107483648,54.12,'lychee',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,date) values('d2','l5',51536000000,15,3147483648,235.213,'watermelon',TRUE,'2023-01-01')"
      };

  private static final String[] sql3 =
      new String[] {
        "CREATE TABLE table1 (device string id, level string id, attr1 string attribute, attr2 string attribute, num int32 measurement, bigNum int64 measurement, "
            + "floatNum double measurement, str TEXT measurement, bool BOOLEAN measurement, date DATE measurement, blob BLOB measurement)",
        "insert into table1(device, level, attr1, attr2, time, num, bigNum, floatNum, str, bool, date, blob) "
            + "values('d1', 'l1', 'c', 'd', 0, 3, 2947483648,231.2121, 'coconut', FALSE, '2023-01-01', X'100DCD62')",
        "insert into table1(device, level, attr1, attr2, time, num, bigNum, floatNum, str, bool, blob) "
            + "values('d1', 'l2', 'y', 'z', 20, 2, 2147483648,434.12, 'pineapple', TRUE, X'101DCD62')",
        "insert into table1(device, level, attr1, attr2, time, num, bigNum, floatNum, str, bool, blob) "
            + "values('d1', 'l3', 't', 'a', 40, 1, 2247483648,12.123, 'apricot', TRUE, null)",
        "insert into table1(device, level, time, num, bigNum, floatNum, str, bool, blob) "
            + "values('d1', 'l4', 80, 9, 2147483646, 43.12, 'apple', FALSE, X'104DCD62')",
        "insert into table1(device, level, time, num, bigNum, floatNum, str, bool, date, blob) "
            + "values('d1', 'l5', 100, 8, 2147483964, 4654.231, 'papaya', TRUE, '2023-05-01', null)",
        "flush",
        "insert into table1(device, level, time, num, bigNum, floatNum, str, bool, blob) "
            + "values('d1', 'l1', 31536000000, 6, 2147483650, 1231.21, 'banana', TRUE, X'106DCD62')",
        "insert into table1(device, time, num, bigNum, floatNum, str, bool, blob) "
            + "values('d999', 31536000000, 6, 2147483650, 1231.21, 'banana', TRUE, X'107DCD62')",
        "insert into table1(level, time, num, bigNum, floatNum, str, bool, date, blob) "
            + "values('l999', 31536000000, 6, 2147483650, 1231.21, 'banana', TRUE, '2023-10-01', X'108DCD62')",
      };

  String[] expectedHeader;
  String[] retArray;
  String sql;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setSortBufferSize(1024 * 1024L);
    EnvFactory.getEnv().initClusterEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxTsBlockLineNumber(1)
        .setMaxNumberOfPointsInPage(1);
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
      for (String sql : sql3) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void onlyQueryAttribute2Test() {
    String[] expectedHeader =
        new String[] {
          "time", "device", "attr2", "num", "str",
        };
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d1,d,3,coconut,",
          "1970-01-01T00:00:00.000Z,d2,c,3,coconut,",
          "1970-01-01T00:00:00.020Z,d1,zz,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d2,null,2,pineapple,",
          "1970-01-01T00:00:00.040Z,d1,a,1,apricot,",
          "1970-01-01T00:00:00.040Z,d2,null,1,apricot,",
          "1970-01-01T00:00:00.080Z,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,null,9,apple,",
          "1970-01-01T00:00:00.100Z,d1,null,8,papaya,",
          "1970-01-01T00:00:00.100Z,d2,null,8,papaya,",
          "1971-01-01T00:00:00.000Z,d1,d,6,banana,",
          "1971-01-01T00:00:00.000Z,d2,c,6,banana,",
          "1971-01-01T00:00:00.100Z,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.500Z,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,null,4,peach,",
          "1971-01-01T00:00:01.000Z,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,null,5,orange,",
          "1971-01-01T00:00:10.000Z,d1,null,7,lemon,",
          "1971-01-01T00:00:10.000Z,d2,null,7,lemon,",
          "1971-01-01T00:01:40.000Z,d1,d,11,pitaya,",
          "1971-01-01T00:01:40.000Z,d2,c,11,pitaya,",
          "1971-04-26T17:46:40.000Z,d1,zz,12,strawberry,",
          "1971-04-26T17:46:40.000Z,d2,null,12,strawberry,",
          "1971-04-26T17:46:40.020Z,d1,a,14,cherry,",
          "1971-04-26T17:46:40.020Z,d2,null,14,cherry,",
          "1971-04-26T18:01:40.000Z,d1,null,13,lychee,",
          "1971-04-26T18:01:40.000Z,d2,null,13,lychee,",
          "1971-08-20T11:33:20.000Z,d1,null,15,watermelon,",
          "1971-08-20T11:33:20.000Z,d2,null,15,watermelon,",
        };
    tableResultSetEqualTest(
        "select time, device, attr2, num, str from table0 order by time, device",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "num", "str", "attr2",
        };
    retArray =
        new String[] {
          "3,coconut,d,",
          "6,banana,d,",
          "11,pitaya,d,",
          "2,pineapple,zz,",
          "10,pumelo,zz,",
          "12,strawberry,zz,",
          "1,apricot,a,",
          "4,peach,a,",
          "14,cherry,a,",
          "9,apple,null,",
          "5,orange,null,",
          "13,lychee,null,",
          "8,papaya,null,",
          "7,lemon,null,",
          "15,watermelon,null,",
          "3,coconut,c,",
          "6,banana,c,",
          "11,pitaya,c,",
          "2,pineapple,null,",
          "10,pumelo,null,",
          "12,strawberry,null,",
          "1,apricot,null,",
          "4,peach,null,",
          "14,cherry,null,",
          "9,apple,null,",
          "5,orange,null,",
          "13,lychee,null,",
          "8,papaya,null,",
          "7,lemon,null,",
          "15,watermelon,null,",
        };
    tableResultSetEqualTest(
        "select num, str, attr2 from table0 order by device, level, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "time", "device", "num", "str",
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d1,3,coconut,",
          "1971-01-01T00:00:00.000Z,d1,6,banana,",
          "1971-01-01T00:01:40.000Z,d1,11,pitaya,",
        };
    tableResultSetEqualTest(
        "select time, device, num, str from table0 where attr2 = 'd' order by time,device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
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

  @Test
  public void sortAllIDTimesTest() {
    String[] expectedHeader = new String[] {"time", "level", "attr1", "device", "num"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,l1,d,d2,3,",
          "1971-01-01T00:00:00.000Z,l1,d,d2,6,",
          "1971-01-01T00:01:40.000Z,l1,d,d2,11,",
          "1970-01-01T00:00:00.000Z,l1,c,d1,3,",
          "1971-01-01T00:00:00.000Z,l1,c,d1,6,",
          "1971-01-01T00:01:40.000Z,l1,c,d1,11,",
          "1970-01-01T00:00:00.020Z,l2,yy,d1,2,",
          "1971-01-01T00:00:00.100Z,l2,yy,d1,10,",
          "1971-04-26T17:46:40.000Z,l2,yy,d1,12,",
          "1970-01-01T00:00:00.020Z,l2,vv,d2,2,",
          "1971-01-01T00:00:00.100Z,l2,vv,d2,10,",
          "1971-04-26T17:46:40.000Z,l2,vv,d2,12,",
          "1970-01-01T00:00:00.040Z,l3,null,d2,1,",
          "1971-01-01T00:00:00.500Z,l3,null,d2,4,",
          "1971-04-26T17:46:40.020Z,l3,null,d2,14,",
          "1970-01-01T00:00:00.040Z,l3,t,d1,1,",
          "1971-01-01T00:00:00.500Z,l3,t,d1,4,",
          "1971-04-26T17:46:40.020Z,l3,t,d1,14,",
          "1970-01-01T00:00:00.080Z,l4,null,d2,9,",
          "1971-01-01T00:00:01.000Z,l4,null,d2,5,",
          "1971-04-26T18:01:40.000Z,l4,null,d2,13,",
          "1970-01-01T00:00:00.080Z,l4,null,d1,9,",
          "1971-01-01T00:00:01.000Z,l4,null,d1,5,",
          "1971-04-26T18:01:40.000Z,l4,null,d1,13,",
          "1970-01-01T00:00:00.100Z,l5,null,d2,8,",
          "1971-01-01T00:00:10.000Z,l5,null,d2,7,",
          "1971-08-20T11:33:20.000Z,l5,null,d2,15,",
          "1970-01-01T00:00:00.100Z,l5,null,d1,8,",
          "1971-01-01T00:00:10.000Z,l5,null,d1,7,",
          "1971-08-20T11:33:20.000Z,l5,null,d1,15,",
        };
    tableResultSetEqualTest(
        "select time,level,attr1,device,num from table0 order by level asc, attr1 desc nulls first, device desc, time asc",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select time,level,attr1,device,num from table0 as t order by level asc, t.attr1 desc nulls first, t.device desc, time asc",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void sortIDWithExpressionTest() {
    String[] expectedHeader = new String[] {"level", "sum", "attr1", "device", "time"};
    String[] retArray =
        new String[] {
          "l1,65,d,d2,1971-01-01T00:01:40.000Z,",
          "l1,65,c,d1,1971-01-01T00:01:40.000Z,",
          "l1,234,d,d2,1970-01-01T00:00:00.000Z,",
          "l1,234,c,d1,1970-01-01T00:00:00.000Z,",
          "l1,1237,d,d2,1971-01-01T00:00:00.000Z,",
          "l1,1237,c,d1,1971-01-01T00:00:00.000Z,",
          "l2,57,yy,d1,1971-04-26T17:46:40.000Z,",
          "l2,57,vv,d2,1971-04-26T17:46:40.000Z,",
          "l2,242,yy,d1,1971-01-01T00:00:00.100Z,",
          "l2,242,vv,d2,1971-01-01T00:00:00.100Z,",
          "l2,436,yy,d1,1970-01-01T00:00:00.020Z,",
          "l2,436,vv,d2,1970-01-01T00:00:00.020Z,",
          "l3,13,t,d1,1970-01-01T00:00:00.040Z,",
          "l3,13,null,d2,1970-01-01T00:00:00.040Z,",
          "l3,217,t,d1,1971-01-01T00:00:00.500Z,",
          "l3,217,null,d2,1971-01-01T00:00:00.500Z,",
          "l3,245,t,d1,1971-04-26T17:46:40.020Z,",
          "l3,245,null,d2,1971-04-26T17:46:40.020Z,",
          "l4,52,null,d2,1970-01-01T00:00:00.080Z,",
          "l4,52,null,d1,1970-01-01T00:00:00.080Z,",
          "l4,61,null,d2,1971-01-01T00:00:01.000Z,",
          "l4,61,null,d1,1971-01-01T00:00:01.000Z,",
          "l4,67,null,d2,1971-04-26T18:01:40.000Z,",
          "l4,67,null,d1,1971-04-26T18:01:40.000Z,",
          "l5,220,null,d1,1971-01-01T00:00:10.000Z,",
          "l5,220,null,d2,1971-01-01T00:00:10.000Z,",
          "l5,250,null,d1,1971-08-20T11:33:20.000Z,",
          "l5,250,null,d2,1971-08-20T11:33:20.000Z,",
          "l5,4662,null,d1,1970-01-01T00:00:00.100Z,",
          "l5,4662,null,d2,1970-01-01T00:00:00.100Z,",
        };
    tableResultSetEqualTest(
        "select level,cast(num+floatNum as int32) as sum,attr1,device,time from table0 order by level asc, cast(num+floatNum as int32) asc, attr1 desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void dateTypeTopKTest() {
    String[] expectedHeader = new String[] {"time", "level", "attr1", "device", "num", "date"};
    String[] retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,l5,null,d2,15,2023-01-01,",
          "1971-08-20T11:33:20.000Z,l5,null,d1,15,2022-01-01,",
          "1971-04-26T18:01:40.000Z,l4,null,d2,13,null,",
          "1971-04-26T18:01:40.000Z,l4,null,d1,13,null,",
          "1971-04-26T17:46:40.020Z,l3,null,d2,14,null,",
        };
    tableResultSetEqualTest(
        "select time,level,attr1,device,num,date from table0 order by time desc,device desc limit 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void projectSortTest() {
    String[] expectedHeader = new String[] {"level", "attr1", "device", "num", "date"};
    String[] retArray =
        new String[] {
          "l2,yy,d1,2,null,",
          "l2,yy,d1,10,null,",
          "l2,yy,d1,12,null,",
          "l1,c,d1,3,null,",
          "l1,c,d1,6,null,",
          "l1,c,d1,11,null,",
        };
    tableResultSetEqualTest(
        "select level,attr1,device,num,date from table0 order by attr2 desc,time limit 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "level", "attr2", "str"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.040Z,l3,a,apricot,",
          "1970-01-01T00:00:00.040Z,l3,null,apricot,",
          "1970-01-01T00:00:00.020Z,l2,null,pineapple,",
          "1970-01-01T00:00:00.020Z,l2,zz,pineapple,",
          "1970-01-01T00:00:00.000Z,l1,d,coconut,",
          "1970-01-01T00:00:00.000Z,l1,c,coconut,",
        };
    tableResultSetEqualTest(
        "select time,level,attr2,str from table0 order by num+1,attr1 limit 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void coalesceTest() {
    String[] expectedHeader = new String[] {"time", "level", "coalesce_attr2", "str"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.040Z,l3,a,apricot,",
          "1970-01-01T00:00:00.040Z,l3,CCC,apricot,",
          "1970-01-01T00:00:00.020Z,l2,CCC,pineapple,",
          "1970-01-01T00:00:00.020Z,l2,zz,pineapple,",
          "1970-01-01T00:00:00.000Z,l1,d,coconut,",
          "1970-01-01T00:00:00.000Z,l1,c,coconut,",
        };
    tableResultSetEqualTest(
        "select time,level,coalesce(attr2, 'CCC', 'DDD') as coalesce_attr2,str from table0 order by num+1,attr1 limit 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // ========== SubQuery Test =========
  @Test
  public void subQueryTest1() {
    String[] expectedHeader = new String[] {"time", "level", "device", "add_num"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.100Z,l5,d1,9,",
          "1971-01-01T00:00:01.000Z,l4,d1,6,",
          "1971-01-01T00:00:10.000Z,l5,d1,8,",
          "1971-04-26T18:01:40.000Z,l4,d1,14,",
          "1971-08-20T11:33:20.000Z,l5,d1,16,",
          "1970-01-01T00:00:00.080Z,l4,d2,10,",
        };

    tableResultSetEqualTest(
        "SELECT time, level, device, add_num FROM (\n"
            + "SELECT time, level, device, substring(str, 2) as cast_str, attr2, bignum, num+1 as add_num FROM table0 WHERE num>1 ORDER BY level DESC, time, device LIMIT 12\n"
            + ") ORDER BY DEVICE,time OFFSET 1 LIMIT 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // =========================================================================
  // ============================ Aggregation Test ===========================
  // =========================================================================
  @Test
  public void aggregationTest() {
    String[] expectedHeader =
        new String[] {
          "count_num",
          "count_star",
          "avg_num",
          "count_num",
          "count_attr2",
          "avg_num",
          "count_device",
          "count_attr1",
          "count_device",
          "avg_floatnum",
          "count_date",
          "count_time",
          "count_star"
        };
    String[] retArray =
        new String[] {
          "30,30,8.0,30,12,8.0,30,15,30,529.0,2,30,30,",
        };
    tableResultSetEqualTest(
        "select count(num) as count_num, count(*) as count_star, avg(num) as avg_num, count(num) as count_num,\n"
            + "count(attr2) as count_attr2, avg(num) as avg_num, count(device) as count_device,\n"
            + "count(attr1) as count_attr1, count(device) as count_device, \n"
            + "round(avg(floatnum)) as avg_floatnum, count(date) as count_date, "
            + "count(time) as count_time, count(*) as count_star "
            + "from table0",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "8,8,5.5,8,3,5.5,8,4,8,1341.0,0,8,8,",
        };
    tableResultSetEqualTest(
        "select count(num) as count_num, count(*) as count_star, avg(num) as avg_num, count(num) as count_num,\n"
            + "count(attr2) as count_attr2, avg(num) as avg_num, count(device) as count_device,\n"
            + "count(attr1) as count_attr1, count(device) as count_device, \n"
            + "round(avg(floatnum)) as avg_floatnum, count(date) as count_date, "
            + "count(time) as count_time, count(*) as count_star "
            + "from table0 "
            + "where time<200 and num>1 ",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // ID has null value
    expectedHeader =
        new String[] {
          "count_num",
          "avg_num",
          "count_num",
          "count_attr2",
          "avg_num",
          "count_device",
          "count_attr1",
          "count_device",
          "avg_floatnum",
          "count_date",
          "count_level",
          "count_time",
          "count_star"
        };
    retArray =
        new String[] {
          "8,5.125,8,4,5.125,7,4,7,1134.0,3,7,8,8,",
        };
    tableResultSetEqualTest(
        "select count(num) as count_num, avg(num) as avg_num, count(num) as count_num,\n"
            + "count(attr2) as count_attr2, avg(num) as avg_num, count(device) as count_device,\n"
            + "count(attr1) as count_attr1, count(device) as count_device, \n"
            + "round(avg(floatnum)) as avg_floatnum, count(date) as count_date, count(level) as count_level,"
            + "count(time) as count_time, count(*) as count_star "
            + "from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // count star with subquery
    // TODO(beyyes) add first/last blob type test
  }

  @Test
  public void globalAggregationTest() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray = new String[] {"30,"};

    String sql = "SELECT count(num+1) from table0";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void countStarTest() {
    expectedHeader = new String[] {"_col0", "_col1"};
    retArray = new String[] {"1,1,"};
    String sql = "select count(*),count(t1) from (select avg(num+1) as t1 from table0)";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"count_star"};
    retArray =
        new String[] {
          "30,",
        };
    tableResultSetEqualTest(
        "select count(*) as count_star from table0", expectedHeader, retArray, DATABASE_NAME);

    retArray =
        new String[] {
          "1,",
        };
    tableResultSetEqualTest(
        "select count(*) as count_star from (select count(*) from table0)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "1,",
        };
    tableResultSetEqualTest(
        "select count(*) as count_star from (select count(*), avg(num) from table0)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"sum"};
    retArray =
        new String[] {
          "38.0,",
        };
    tableResultSetEqualTest(
        "select count_star + avg_num as sum from (select count(*) as count_star, avg(num) as avg_num from table0)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // TODO select count(*),count(t1) from (select avg(num+1) as t1 from table0) where time < 0
  }

  @Test
  public void groupByAggregationTest() {
    expectedHeader =
        new String[] {
          "device",
          "level",
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "sum_num"
        };
    retArray =
        new String[] {
          "d1,l1,3,3,3,0,3,3,3,20.0,",
          "d1,l2,3,3,3,0,3,3,3,24.0,",
          "d1,l3,3,3,3,0,3,3,3,19.0,",
          "d1,l4,3,3,3,0,0,0,3,27.0,",
          "d1,l5,3,3,3,1,0,0,3,30.0,",
          "d2,l1,3,3,3,0,3,3,3,20.0,",
          "d2,l2,3,3,3,0,3,0,3,24.0,",
          "d2,l3,3,3,3,0,0,0,3,19.0,",
          "d2,l4,3,3,3,0,0,0,3,27.0,",
          "d2,l5,3,3,3,1,0,0,3,30.0,",
        };
    sql =
        "select device, level, "
            + "count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num "
            + "from table0 group by device,level order by device, level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device",
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "sum_num"
        };
    retArray =
        new String[] {
          "d1,3,3,3,0,3,3,3,20.0,",
        };
    sql =
        "select device, "
            + "count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num "
            + "from table0 where device='d1' and level='l1' group by device order by device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"device", "level"};
    retArray =
        new String[] {
          "d1,l1,", "d1,l2,", "d1,l3,", "d1,l4,", "d1,l5,", "d2,l1,", "d2,l2,", "d2,l3,", "d2,l4,",
          "d2,l5,",
        };
    sql = "select device,level from table0 group by device,level order by device,level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"device", "level", "attr1", "bin"};
    retArray =
        new String[] {
          "d1,l1,c,1970-01-01T00:00:00.000Z,",
          "d1,l1,c,1971-01-01T00:00:00.000Z,",
          "d1,l2,yy,1970-01-01T00:00:00.000Z,",
          "d1,l2,yy,1971-01-01T00:00:00.000Z,",
          "d1,l2,yy,1971-04-26T00:00:00.000Z,",
          "d1,l3,t,1970-01-01T00:00:00.000Z,",
          "d1,l3,t,1971-01-01T00:00:00.000Z,",
          "d1,l3,t,1971-04-26T00:00:00.000Z,",
          "d1,l4,null,1970-01-01T00:00:00.000Z,",
          "d1,l4,null,1971-01-01T00:00:00.000Z,",
          "d1,l4,null,1971-04-26T00:00:00.000Z,",
          "d1,l5,null,1970-01-01T00:00:00.000Z,",
          "d1,l5,null,1971-01-01T00:00:00.000Z,",
          "d1,l5,null,1971-08-20T00:00:00.000Z,",
          "d2,l1,d,1970-01-01T00:00:00.000Z,",
          "d2,l1,d,1971-01-01T00:00:00.000Z,",
          "d2,l2,vv,1970-01-01T00:00:00.000Z,",
          "d2,l2,vv,1971-01-01T00:00:00.000Z,",
          "d2,l2,vv,1971-04-26T00:00:00.000Z,",
          "d2,l3,null,1970-01-01T00:00:00.000Z,",
          "d2,l3,null,1971-01-01T00:00:00.000Z,",
          "d2,l3,null,1971-04-26T00:00:00.000Z,",
          "d2,l4,null,1970-01-01T00:00:00.000Z,",
          "d2,l4,null,1971-01-01T00:00:00.000Z,",
          "d2,l4,null,1971-04-26T00:00:00.000Z,",
          "d2,l5,null,1970-01-01T00:00:00.000Z,",
          "d2,l5,null,1971-01-01T00:00:00.000Z,",
          "d2,l5,null,1971-08-20T00:00:00.000Z,",
        };
    sql =
        "select device,level,attr1,date_bin(1d,time) as bin from table0 group by 4,device,level,attr1 order by device,level,attr1,bin";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    //    expectedHeader = new String[] {"attr1", "attr2"};
    //    retArray =
    //        new String[] {
    //          "c,d,", "d,c,", "t,a,", "vv,null,", "yy,zz,", "null,null,",
    //        };
    //    sql = "select attr1,attr2 from table0 group by attr1,attr2 order by attr1,attr2";
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void groupByDateBinTest() {
    expectedHeader =
        new String[] {
          "device",
          "level",
          "bin",
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "avg_num"
        };
    retArray =
        new String[] {
          "d1,l1,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,3.0,",
          "d1,l1,1971-01-01T00:00:00.000Z,2,2,2,0,2,2,2,8.5,",
          "d1,l2,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,2.0,",
          "d1,l2,1971-01-01T00:00:00.000Z,2,2,2,0,2,2,2,11.0,",
          "d1,l3,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,1.0,",
          "d1,l3,1971-01-01T00:00:00.000Z,2,2,2,0,2,2,2,9.0,",
          "d1,l4,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,9.0,",
          "d1,l4,1971-01-01T00:00:00.000Z,2,2,2,0,0,0,2,9.0,",
          "d1,l5,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,8.0,",
          "d1,l5,1971-01-01T00:00:00.000Z,2,2,2,1,0,0,2,11.0,",
          "d2,l1,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,3.0,",
          "d2,l1,1971-01-01T00:00:00.000Z,2,2,2,0,2,2,2,8.5,",
          "d2,l2,1970-01-01T00:00:00.000Z,1,1,1,0,1,0,1,2.0,",
          "d2,l2,1971-01-01T00:00:00.000Z,2,2,2,0,2,0,2,11.0,",
          "d2,l3,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,1.0,",
          "d2,l3,1971-01-01T00:00:00.000Z,2,2,2,0,0,0,2,9.0,",
          "d2,l4,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,9.0,",
          "d2,l4,1971-01-01T00:00:00.000Z,2,2,2,0,0,0,2,9.0,",
          "d2,l5,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,8.0,",
          "d2,l5,1971-01-01T00:00:00.000Z,2,2,2,1,0,0,2,11.0,",
        };
    sql =
        "select device, level, date_bin(1y, time) as bin,"
            + "count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, avg(num) as avg_num "
            + "from table0 group by 3, device, level order by device, level, bin";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    retArray =
        new String[] {
          "d1,l1,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,3.0,",
          "d1,l1,1971-01-01T00:00:00.000Z,2,2,2,0,2,2,2,8.5,",
          "d1,l2,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,2.0,",
          "d1,l2,1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,10.0,",
          "d1,l2,1971-04-26T00:00:00.000Z,1,1,1,0,1,1,1,12.0,",
          "d1,l3,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,1.0,",
          "d1,l3,1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,4.0,",
          "d1,l3,1971-04-26T00:00:00.000Z,1,1,1,0,1,1,1,14.0,",
          "d1,l4,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,9.0,",
          "d1,l4,1971-01-01T00:00:00.000Z,1,1,1,0,0,0,1,5.0,",
          "d1,l4,1971-04-26T00:00:00.000Z,1,1,1,0,0,0,1,13.0,",
          "d1,l5,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,8.0,",
          "d1,l5,1971-01-01T00:00:00.000Z,1,1,1,0,0,0,1,7.0,",
          "d1,l5,1971-08-20T00:00:00.000Z,1,1,1,1,0,0,1,15.0,",
          "d2,l1,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,3.0,",
          "d2,l1,1971-01-01T00:00:00.000Z,2,2,2,0,2,2,2,8.5,",
          "d2,l2,1970-01-01T00:00:00.000Z,1,1,1,0,1,0,1,2.0,",
          "d2,l2,1971-01-01T00:00:00.000Z,1,1,1,0,1,0,1,10.0,",
          "d2,l2,1971-04-26T00:00:00.000Z,1,1,1,0,1,0,1,12.0,",
          "d2,l3,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,1.0,",
          "d2,l3,1971-01-01T00:00:00.000Z,1,1,1,0,0,0,1,4.0,",
          "d2,l3,1971-04-26T00:00:00.000Z,1,1,1,0,0,0,1,14.0,",
          "d2,l4,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,9.0,",
          "d2,l4,1971-01-01T00:00:00.000Z,1,1,1,0,0,0,1,5.0,",
          "d2,l4,1971-04-26T00:00:00.000Z,1,1,1,0,0,0,1,13.0,",
          "d2,l5,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,8.0,",
          "d2,l5,1971-01-01T00:00:00.000Z,1,1,1,0,0,0,1,7.0,",
          "d2,l5,1971-08-20T00:00:00.000Z,1,1,1,1,0,0,1,15.0,",
        };
    sql =
        "select device, level, date_bin(1d, time) as bin,"
            + "count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, avg(num) as avg_num "
            + "from table0 group by 3, device, level order by device, level, bin";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    retArray =
        new String[] {
          "d1,l1,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,3.0,",
          "d1,l1,1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,6.0,",
          "d1,l1,1971-01-01T00:01:40.000Z,1,1,1,0,1,1,1,11.0,",
          "d1,l2,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,2.0,",
          "d1,l2,1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,10.0,",
          "d1,l2,1971-04-26T17:46:40.000Z,1,1,1,0,1,1,1,12.0,",
          "d1,l3,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,1.0,",
          "d1,l3,1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,4.0,",
          "d1,l3,1971-04-26T17:46:40.000Z,1,1,1,0,1,1,1,14.0,",
          "d1,l4,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,9.0,",
          "d1,l4,1971-01-01T00:00:01.000Z,1,1,1,0,0,0,1,5.0,",
          "d1,l4,1971-04-26T18:01:40.000Z,1,1,1,0,0,0,1,13.0,",
          "d1,l5,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,8.0,",
          "d1,l5,1971-01-01T00:00:10.000Z,1,1,1,0,0,0,1,7.0,",
          "d1,l5,1971-08-20T11:33:20.000Z,1,1,1,1,0,0,1,15.0,",
          "d2,l1,1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,3.0,",
          "d2,l1,1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,6.0,",
          "d2,l1,1971-01-01T00:01:40.000Z,1,1,1,0,1,1,1,11.0,",
          "d2,l2,1970-01-01T00:00:00.000Z,1,1,1,0,1,0,1,2.0,",
          "d2,l2,1971-01-01T00:00:00.000Z,1,1,1,0,1,0,1,10.0,",
          "d2,l2,1971-04-26T17:46:40.000Z,1,1,1,0,1,0,1,12.0,",
          "d2,l3,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,1.0,",
          "d2,l3,1971-01-01T00:00:00.000Z,1,1,1,0,0,0,1,4.0,",
          "d2,l3,1971-04-26T17:46:40.000Z,1,1,1,0,0,0,1,14.0,",
          "d2,l4,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,9.0,",
          "d2,l4,1971-01-01T00:00:01.000Z,1,1,1,0,0,0,1,5.0,",
          "d2,l4,1971-04-26T18:01:40.000Z,1,1,1,0,0,0,1,13.0,",
          "d2,l5,1970-01-01T00:00:00.000Z,1,1,1,0,0,0,1,8.0,",
          "d2,l5,1971-01-01T00:00:10.000Z,1,1,1,0,0,0,1,7.0,",
          "d2,l5,1971-08-20T11:33:20.000Z,1,1,1,1,0,0,1,15.0,",
        };
    sql =
        "select device, level, date_bin(1s, time) as bin,"
            + "count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, avg(num) as avg_num "
            + "from table0 group by 3, device, level order by device, level, bin";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // only group by date_bin
    expectedHeader = new String[] {"bin"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,", "1971-01-01T00:00:00.000Z,", "1971-04-26T00:00:00.000Z,"
        };
    sql =
        "select date_bin(1d, time) as bin from table0 where device='d1' and level='l2' group by 1";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "bin",
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "avg_num"
        };
    sql =
        "select date_bin(1s, time) as bin,"
            + "count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, avg(num) as avg_num "
            + "from table0 where device='d1' and level='l2' group by 1";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,1,1,1,0,1,1,1,2.0,",
          "1971-01-01T00:00:00.000Z,1,1,1,0,1,1,1,10.0,",
          "1971-04-26T17:46:40.000Z,1,1,1,0,1,1,1,12.0,"
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // TODO(beyyes) test below
    //    sql = "select count(*) from (\n" +
    //            "\tselect device, level, date_bin(1d, time) as bin, \n" +
    //            "\tcount(num) as count_num, count(*) as count_star, count(device) as count_device,
    // count(date) as count_date, count(attr1) as count_attr1, count(attr2) as count_attr2,
    // count(time) as count_time, avg(num) as avg_num \n" +
    //            "\tfrom table0 \n" +
    //            "\tgroup by 3, device, level order by device, level, bin\n" +
    //            ")\n";
  }

  @Test
  public void aggregationNoDataTest() {
    expectedHeader =
        new String[] {
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "sum_num",
          "avg_num"
        };

    retArray =
        new String[] {
          "0,0,0,0,0,0,0,null,null,",
        };
    sql =
        "select count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num,"
            + "avg(num) as avg_num from table0 where time=32";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    retArray =
        new String[] {
          "2,2,2,0,2,1,2,24.0,12.0,",
        };
    sql =
        "select count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num,"
            + "avg(num) as avg_num from table0 where time=32 or time=1971-04-27T01:46:40.000+08:00";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device",
          "level",
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "sum_num",
          "avg_num"
        };
    retArray = new String[] {};
    sql =
        "select device, level, count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num,"
            + "avg(num) as avg_num from table0 where time=32 group by device, level order by device, level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
    retArray = new String[] {"d1,l2,1,1,1,0,1,1,1,12.0,12.0,", "d2,l2,1,1,1,0,1,0,1,12.0,12.0,"};
    sql =
        "select device, level, count(num) as count_num, count(*) as count_star, count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num,"
            + "avg(num) as avg_num from table0 where time=32 or time=1971-04-27T01:46:40.000+08:00 group by device, level order by device, level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device",
          "level",
          "bin",
          "count_num",
          "count_star",
          "count_device",
          "count_date",
          "count_attr1",
          "count_attr2",
          "count_time",
          "sum_num",
          "avg_num"
        };
    retArray = new String[] {};
    sql =
        "select device, level, date_bin(1d, time) as bin, count(num) as count_num, count(*) as count_star, "
            + "count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num,"
            + "avg(num) as avg_num from table0 where time=32 group by 3, device, level order by device, level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
    retArray =
        new String[] {
          "d1,l2,1971-04-26T00:00:00.000Z,1,1,1,0,1,1,1,12.0,12.0,",
          "d2,l2,1971-04-26T00:00:00.000Z,1,1,1,0,1,0,1,12.0,12.0,"
        };
    sql =
        "select device, level, date_bin(1d, time) as bin, count(num) as count_num, count(*) as count_star, "
            + "count(device) as count_device, count(date) as count_date, "
            + "count(attr1) as count_attr1, count(attr2) as count_attr2, count(time) as count_time, sum(num) as sum_num,"
            + "avg(num) as avg_num from table0 where time=32 or time=1971-04-27T01:46:40.000+08:00 group by 3, device, level order by device, level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void lastFirstMaxMinTest() {
    expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11", "_col12", "_col13",
        };
    retArray =
        new String[] {
          "1971-04-26T18:01:40.000Z,d1,l4,null,null,13,2107483648,54.12,lychee,true,null,0x108dcd62,2024-09-15T06:15:35.000Z,test-string1,",
        };
    sql =
        "select last(time),last(device),last(level),last(attr1),last(attr2),last(num),last(bignum),last(floatnum),last(str),last(bool),last(date),last(blob),last(ts),last(stringv) from table0 where device='d1' and level='l4'";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
    retArray =
        new String[] {
          "1970-01-01T00:00:00.080Z,d1,l4,null,null,9,2147483646,43.12,apple,false,null,0x108dcd62,2024-09-15T06:15:35.000Z,test-string1,",
        };
    sql =
        "select first(time),first(device),first(level),first(attr1),first(attr2),first(num),first(bignum),first(floatnum),first(str),first(bool),first(date),first(blob),first(ts),first(stringv) from table0 where device='d1' and level='l4'";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5",
        };
    retArray =
        new String[] {
          "15,3147483648,4654.231,1,2107483648,12.123,",
        };
    sql =
        "select max(num),max(bignum),max(floatnum),min(num),min(bignum),min(floatnum) from table0";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device", "level", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7",
        };
    retArray =
        new String[] {
          "d1,l1,11,2947483648,1231.21,3,2147468648,54.121,",
          "d1,l2,12,3147483648,434.12,2,2146483648,45.231,",
          "d1,l3,14,2907483648,231.34,1,2147493648,12.123,",
          "d1,l4,13,2149783648,56.32,5,2107483648,43.12,",
          "d1,l5,15,3147483648,4654.231,7,2147483964,213.112,",
          "d2,l1,11,2947483648,1231.21,3,2147468648,54.121,",
          "d2,l2,12,3147483648,434.12,2,2146483648,45.231,",
          "d2,l3,14,2907483648,231.34,1,2147493648,12.123,",
          "d2,l4,13,2149783648,56.32,5,2107483648,43.12,",
          "d2,l5,15,3147483648,4654.231,7,2147483964,213.112,",
        };
    sql =
        "select device,level,max(num),max(bignum),max(floatnum),min(num),min(bignum),min(floatnum) from table0 group by device,level order by device,level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void lastByFirstByTest() {
    String[] expectedHeader1 = buildHeaders(13);
    String[] expectedHeader2 = buildHeaders(14);

    sql =
        "select last_by(time,time),last_by(device,time),last_by(level,time),last_by(attr1,time),last_by(attr2,time),last_by(num,time),last_by(bignum,time),last_by(floatnum,time),last_by(str,time),last_by(bool,time),last_by(date,time),last_by(ts,time),last_by(stringv,time) from table0 where device='d2'";
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,d2,l5,null,null,15,3147483648,235.213,watermelon,true,2023-01-01,null,null,",
        };
    tableResultSetEqualTest(sql, expectedHeader1, retArray, DATABASE_NAME);
    sql =
        "select last_by(time,time),last_by(device,time),last_by(level,time),last_by(attr1,time),last_by(attr2,time),last_by(num,time),last_by(bignum,time),last_by(floatnum,time),last_by(str,time),last_by(bool,time),last_by(date,time),last_by(ts,time),last_by(stringv,time),last_by(blob,time) from table0 where device='d2'";
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,d2,l5,null,null,15,3147483648,235.213,watermelon,true,2023-01-01,null,null,null,",
        };
    tableResultSetEqualTest(sql, expectedHeader2, retArray, DATABASE_NAME);

    sql =
        "select last_by(time,time),last_by(time,device),last_by(time,level),last_by(time,attr1),last_by(time,attr2),last_by(time,num),last_by(time,bignum),last_by(time,floatnum),last_by(time,str),last_by(time,bool),last_by(time,date),last_by(time,ts),last_by(time,stringv) from table0 where device='d2'";
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-04-26T17:46:40.000Z,1971-01-01T00:01:40.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-01-01T00:01:40.000Z,1971-01-01T00:01:40.000Z,",
        };
    tableResultSetEqualTest(sql, expectedHeader1, retArray, DATABASE_NAME);
    sql =
        "select last_by(time,time),last_by(time,device),last_by(time,level),last_by(time,attr1),last_by(time,attr2),last_by(time,num),last_by(time,bignum),last_by(time,floatnum),last_by(time,str),last_by(time,bool),last_by(time,date),last_by(time,ts),last_by(time,stringv),last_by(time,blob) from table0 where device='d2'";
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-04-26T17:46:40.000Z,1971-01-01T00:01:40.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-01-01T00:01:40.000Z,1971-01-01T00:01:40.000Z,1970-01-01T00:00:00.080Z,",
        };
    tableResultSetEqualTest(sql, expectedHeader2, retArray, DATABASE_NAME);

    String[] expectedHeader11 = buildHeaders(expectedHeader1.length * 2);
    sql =
        "select last_by(time,time),last_by(device,time),last_by(level,time),last_by(attr1,time),last_by(attr2,time),last_by(num,time),last_by(bignum,time),last_by(floatnum,time),last_by(str,time),last_by(bool,time),last_by(date,time),last_by(ts,time),last_by(stringv,time),last_by(time,time),last_by(time,device),last_by(time,level),last_by(time,attr1),last_by(time,attr2),last_by(time,num),last_by(time,bignum),last_by(time,floatnum),last_by(time,str),last_by(time,bool),last_by(time,date),last_by(time,ts),last_by(time,stringv) from table0 where device='d2'";
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,d2,l5,null,null,15,3147483648,235.213,watermelon,true,2023-01-01,null,null,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-04-26T17:46:40.000Z,1971-01-01T00:01:40.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1971-01-01T00:01:40.000Z,1971-01-01T00:01:40.000Z,",
        };
    tableResultSetEqualTest(sql, expectedHeader11, retArray, DATABASE_NAME);

    sql =
        "select first_by(time,time),first_by(device,time),first_by(level,time),first_by(attr1,time),first_by(attr2,time),first_by(num,time),first_by(bignum,time),first_by(floatnum,time),first_by(str,time),first_by(bool,time),first_by(date,time),first_by(ts,time),first_by(stringv,time) from table0 where device='d2' and time>80";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.100Z,d2,l5,null,null,8,2147483964,4654.231,papaya,true,null,null,null,",
        };
    tableResultSetEqualTest(sql, expectedHeader1, retArray, DATABASE_NAME);

    sql =
        "select first_by(time,time),first_by(time,device),first_by(time,level),first_by(time,attr1),first_by(time,attr2),first_by(time,num),first_by(time,bignum),first_by(time,floatnum),first_by(time,str),first_by(time,bool),first_by(time,date),first_by(time,ts),first_by(time,stringv) from table0 where device='d2' and time>80";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.100Z,1971-01-01T00:00:00.000Z,1971-01-01T00:00:00.000Z,1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.100Z,1971-08-20T11:33:20.000Z,1971-01-01T00:01:40.000Z,1971-01-01T00:01:40.000Z,",
        };
    tableResultSetEqualTest(sql, expectedHeader1, retArray, DATABASE_NAME);
  }

  @Test
  public void maxByMinByExtremeTest() {
    expectedHeader = buildHeaders(10);
    sql =
        "select max_by(time,floatnum),min_by(time,floatnum),max_by(time,date),min_by(time,date),max_by(time,floatnum),min_by(time,floatnum),max_by(time,ts),min_by(time,ts),max_by(time,stringv),min_by(time,stringv) from table0";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.040Z,1971-08-20T11:33:20.000Z,1971-08-20T11:33:20.000Z,1970-01-01T00:00:00.100Z,1970-01-01T00:00:00.040Z,1971-01-01T00:00:10.000Z,1971-01-01T00:01:40.000Z,1971-01-01T00:01:40.000Z,1971-01-01T00:00:01.000Z,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(3);
    sql = "select extreme(num),extreme(bignum),extreme(floatnum) from table0";
    retArray = new String[] {"15,3147483648,4654.231,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  // ==================================================================
  // ============================ Join Test ===========================
  // ==================================================================
  // no filter
  @Test
  public void innerJoinTest1() {
    String[] expectedHeader =
        new String[] {"time", "device", "level", "num", "device", "attr2", "num", "str"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d2,l1,3,d1,d,3,coconut,",
          "1970-01-01T00:00:00.000Z,d2,l1,3,d2,c,3,coconut,",
          "1970-01-01T00:00:00.020Z,d1,l2,2,d1,zz,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d1,l2,2,d2,null,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d2,l2,2,d1,zz,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d2,l2,2,d2,null,2,pineapple,"
        };

    // join on
    String sql =
        "SELECT t1.time as time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1 JOIN table0 t2 ON t1.time = t2.time \n"
            + "ORDER BY t1.time, t1.device, t2.device OFFSET 2 LIMIT 6";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // implicit join
    sql =
        "SELECT t1.time as time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1, table0 t2 WHERE t1.time = t2.time \n"
            + "ORDER BY t1.time, t1.device, t2.device OFFSET 2 LIMIT 6";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // join using
    sql =
        "SELECT time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1 JOIN table0 t2 USING(time)\n"
            + "ORDER BY time, t1.device, t2.device OFFSET 2 LIMIT 6";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  // has filter
  @Test
  public void innerJoinTest2() {
    String[] expectedHeader =
        new String[] {"time", "device", "level", "t1_num_add", "device", "attr2", "num", "str"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.080Z,d1,l4,10,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d1,l4,10,d2,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,l4,10,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,l4,10,d2,null,9,apple,",
          "1971-01-01T00:00:00.100Z,d1,l2,11,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d1,l2,11,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,l2,11,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,l2,11,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.500Z,d1,l3,5,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d1,l3,5,d2,null,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,l3,5,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,l3,5,d2,null,4,peach,",
          "1971-01-01T00:00:01.000Z,d1,l4,6,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d1,l4,6,d2,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,l4,6,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,l4,6,d2,null,5,orange,",
        };

    // join on
    String sql =
        "SELECT t2.time,t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE TIME>=80 AND level!='l1' AND cast(num as double)>0) t1 \n"
            + "JOIN (SELECT * FROM table0 WHERE TIME<=31536001000 AND floatNum<1000 AND device in ('d1','d2')) t2 \n"
            + "ON t1.time = t2.time \n"
            + "ORDER BY t1.time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "SELECT t2.time,t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0) t1 \n"
            + "JOIN (SELECT * FROM table0) t2 ON t1.time = t2.time \n"
            + "WHERE t1.TIME>=80 AND cast(t1.num as double)>0 AND t1.level!='l1' \n"
            + "AND t2.time<=31536001000 AND t2.floatNum<1000 AND t2.device in ('d1','d2')\n"
            + "ORDER BY t1.time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "SELECT t2.time,t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE time>=80) t1 \n"
            + "JOIN (SELECT * FROM table0 WHERE floatNum<1000) t2 ON t1.time = t2.time \n"
            + "WHERE cast(t1.num as double)>0 AND t1.level!='l1' \n"
            + "AND t2.time<=31536001000 AND t2.device in ('d1','d2')\n"
            + "ORDER BY t1.time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // implicit join
    sql =
        "SELECT t2.time,t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE TIME>=80 AND level!='l1' AND cast(num as double)>0) t1, \n"
            + " (SELECT * FROM table0 WHERE TIME<=31536001000 AND floatNum<1000 AND device in ('d1','d2')) t2 \n"
            + "WHERE t1.time = t2.time \n"
            + "ORDER BY t1.time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "SELECT t2.time,t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0) t1, \n"
            + " (SELECT * FROM table0) t2 \n"
            + "WHERE t1.time=t2.time AND t1.TIME>=80 AND cast(t1.num as double)>0 AND t1.level!='l1' \n"
            + "AND t2.time<=31536001000 AND t2.floatNum<1000 AND t2.device in ('d1','d2')\n"
            + "ORDER BY t1.time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "SELECT t2.time,t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE time>=80) t1, \n"
            + " (SELECT * FROM table0 WHERE floatNum<1000) t2 \n"
            + "WHERE t1.time=t2.time AND cast(t1.num as double)>0 AND t1.level!='l1' \n"
            + "AND t2.time<=31536001000 AND t2.device in ('d1','d2')\n"
            + "ORDER BY t1.time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // join using
    sql =
        "SELECT time, t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE time>=80) t1 \n"
            + "JOIN (SELECT * FROM table0 WHERE floatNum<1000) t2 USING(time) \n"
            + "WHERE cast(t1.num as double)>0 AND t1.level!='l1' \n"
            + "AND time<=31536001000 AND t2.device in ('d1','d2')\n"
            + "ORDER BY time, t1.device, t2.device LIMIT 20";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  // no filter
  @Test
  public void fullOuterJoinTest1() {
    String[] expectedHeader =
        new String[] {"time", "device", "level", "num", "device", "attr2", "num", "str"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d1,l1,3,d1,d,3,coconut,",
          "1970-01-01T00:00:00.000Z,d1,l1,3,d2,c,3,coconut,",
          "1970-01-01T00:00:00.000Z,d2,l1,3,d1,d,3,coconut,",
          "1970-01-01T00:00:00.000Z,d2,l1,3,d2,c,3,coconut,",
          "1970-01-01T00:00:00.020Z,d1,l2,2,d1,zz,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d1,l2,2,d2,null,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d2,l2,2,d1,zz,2,pineapple,",
          "1970-01-01T00:00:00.020Z,d2,l2,2,d2,null,2,pineapple,",
          "1970-01-01T00:00:00.040Z,d1,l3,1,d1,a,1,apricot,",
          "1970-01-01T00:00:00.040Z,d1,l3,1,d2,null,1,apricot,",
          "1970-01-01T00:00:00.040Z,d2,l3,1,d1,a,1,apricot,",
          "1970-01-01T00:00:00.040Z,d2,l3,1,d2,null,1,apricot,",
          "1970-01-01T00:00:00.080Z,d1,l4,9,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d1,l4,9,d2,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,l4,9,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,l4,9,d2,null,9,apple,",
          "1970-01-01T00:00:00.100Z,d1,l5,8,d1,null,8,papaya,",
          "1970-01-01T00:00:00.100Z,d1,l5,8,d2,null,8,papaya,",
          "1970-01-01T00:00:00.100Z,d2,l5,8,d1,null,8,papaya,",
          "1970-01-01T00:00:00.100Z,d2,l5,8,d2,null,8,papaya,",
          "1971-01-01T00:00:00.000Z,d1,l1,6,d1,d,6,banana,",
          "1971-01-01T00:00:00.000Z,d1,l1,6,d2,c,6,banana,",
          "1971-01-01T00:00:00.000Z,d2,l1,6,d1,d,6,banana,",
          "1971-01-01T00:00:00.000Z,d2,l1,6,d2,c,6,banana,",
          "1971-01-01T00:00:00.100Z,d1,l2,10,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d1,l2,10,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,l2,10,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,l2,10,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.500Z,d1,l3,4,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d1,l3,4,d2,null,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,l3,4,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,l3,4,d2,null,4,peach,",
          "1971-01-01T00:00:01.000Z,d1,l4,5,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d1,l4,5,d2,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,l4,5,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,l4,5,d2,null,5,orange,",
          "1971-01-01T00:00:10.000Z,d1,l5,7,d1,null,7,lemon,",
          "1971-01-01T00:00:10.000Z,d1,l5,7,d2,null,7,lemon,",
          "1971-01-01T00:00:10.000Z,d2,l5,7,d1,null,7,lemon,",
          "1971-01-01T00:00:10.000Z,d2,l5,7,d2,null,7,lemon,",
          "1971-01-01T00:01:40.000Z,d1,l1,11,d1,d,11,pitaya,",
          "1971-01-01T00:01:40.000Z,d1,l1,11,d2,c,11,pitaya,",
          "1971-01-01T00:01:40.000Z,d2,l1,11,d1,d,11,pitaya,",
          "1971-01-01T00:01:40.000Z,d2,l1,11,d2,c,11,pitaya,",
          "1971-04-26T17:46:40.000Z,d1,l2,12,d1,zz,12,strawberry,",
          "1971-04-26T17:46:40.000Z,d1,l2,12,d2,null,12,strawberry,",
          "1971-04-26T17:46:40.000Z,d2,l2,12,d1,zz,12,strawberry,",
          "1971-04-26T17:46:40.000Z,d2,l2,12,d2,null,12,strawberry,",
          "1971-04-26T17:46:40.020Z,d1,l3,14,d1,a,14,cherry,",
          "1971-04-26T17:46:40.020Z,d1,l3,14,d2,null,14,cherry,",
          "1971-04-26T17:46:40.020Z,d2,l3,14,d1,a,14,cherry,",
          "1971-04-26T17:46:40.020Z,d2,l3,14,d2,null,14,cherry,",
          "1971-04-26T18:01:40.000Z,d1,l4,13,d1,null,13,lychee,",
          "1971-04-26T18:01:40.000Z,d1,l4,13,d2,null,13,lychee,",
          "1971-04-26T18:01:40.000Z,d2,l4,13,d1,null,13,lychee,",
          "1971-04-26T18:01:40.000Z,d2,l4,13,d2,null,13,lychee,",
          "1971-08-20T11:33:20.000Z,d1,l5,15,d1,null,15,watermelon,",
          "1971-08-20T11:33:20.000Z,d1,l5,15,d2,null,15,watermelon,",
          "1971-08-20T11:33:20.000Z,d2,l5,15,d1,null,15,watermelon,",
          "1971-08-20T11:33:20.000Z,d2,l5,15,d2,null,15,watermelon,",
        };

    // join on
    String sql =
        "SELECT t1.time as time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1 FULL JOIN table0 t2 ON t1.time = t2.time \n"
            + "ORDER BY t1.time, t1.device, t2.device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "SELECT t1.time as time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1 INNER JOIN table0 t2 ON t1.time = t2.time \n"
            + "ORDER BY t1.time, t1.device, t2.device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // join using
    sql =
        "SELECT time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1 FULL OUTER JOIN table0 t2 USING(time)\n"
            + "ORDER BY time, t1.device, t2.device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "SELECT time, t1.device, t1.level, t1.num, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM table0 t1 INNER JOIN table0 t2 USING(time)\n"
            + "ORDER BY time, t1.device, t2.device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  // has filter
  @Test
  public void fullOuterJoinTest2() {
    String[] expectedHeader =
        new String[] {"time", "device", "level", "t1_num_add", "device", "attr2", "num", "str"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,null,null,null,d1,d,3,coconut,",
          "1970-01-01T00:00:00.000Z,null,null,null,d2,c,3,coconut,",
          "1970-01-01T00:00:00.020Z,null,null,null,d1,zz,2,pineapple,",
          "1970-01-01T00:00:00.020Z,null,null,null,d2,null,2,pineapple,",
          "1970-01-01T00:00:00.040Z,null,null,null,d1,a,1,apricot,",
          "1970-01-01T00:00:00.040Z,null,null,null,d2,null,1,apricot,",
          "1970-01-01T00:00:00.080Z,d1,l4,10,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d1,l4,10,d2,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,l4,10,d1,null,9,apple,",
          "1970-01-01T00:00:00.080Z,d2,l4,10,d2,null,9,apple,",
          "1971-01-01T00:00:00.100Z,d1,l2,11,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d1,l2,11,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,l2,11,d1,zz,10,pumelo,",
          "1971-01-01T00:00:00.100Z,d2,l2,11,d2,null,10,pumelo,",
          "1971-01-01T00:00:00.500Z,d1,l3,5,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d1,l3,5,d2,null,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,l3,5,d1,a,4,peach,",
          "1971-01-01T00:00:00.500Z,d2,l3,5,d2,null,4,peach,",
          "1971-01-01T00:00:01.000Z,d1,l4,6,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d1,l4,6,d2,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,l4,6,d1,null,5,orange,",
          "1971-01-01T00:00:01.000Z,d2,l4,6,d2,null,5,orange,",
          "1971-01-01T00:00:10.000Z,d1,l5,8,null,null,null,null,",
          "1971-01-01T00:00:10.000Z,d2,l5,8,null,null,null,null,",
          "1971-04-26T17:46:40.000Z,d1,l2,13,null,null,null,null,",
          "1971-04-26T17:46:40.000Z,d2,l2,13,null,null,null,null,",
          "1971-04-26T17:46:40.020Z,d1,l3,15,null,null,null,null,",
          "1971-04-26T17:46:40.020Z,d2,l3,15,null,null,null,null,",
          "1971-04-26T18:01:40.000Z,d1,l4,14,null,null,null,null,",
          "1971-04-26T18:01:40.000Z,d2,l4,14,null,null,null,null,",
          "1971-08-20T11:33:20.000Z,d1,l5,16,null,null,null,null,",
          "1971-08-20T11:33:20.000Z,d2,l5,16,null,null,null,null,",
        };

    // join using
    String sql =
        "SELECT time, t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE TIME>=80 AND level!='l1' AND cast(num as double)>0) t1 \n"
            + "FULL JOIN (SELECT * FROM table0 WHERE TIME<=31536001000 AND floatNum<1000 AND device in ('d1','d2')) t2 \n"
            + "USING(time) \n"
            + "ORDER BY time, t1.device, t2.device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // join on
    sql =
        "SELECT COALESCE(t1.time, t2.time) as time, t1.device, t1.level, t1_num_add, t2.device, t2.attr2, t2.num, t2.str\n"
            + "FROM (SELECT *,num+1 as t1_num_add FROM table0 WHERE TIME>=80 AND level!='l1' AND cast(num as double)>0) t1 \n"
            + "FULL JOIN (SELECT * FROM table0 WHERE TIME<=31536001000 AND floatNum<1000 AND device in ('d1','d2')) t2 \n"
            + "ON t1.time = t2.time \n"
            + "ORDER BY time, t1.device, t2.device";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  public static String[] buildHeaders(int length) {
    String[] expectedHeader = new String[length];
    for (int i = 0; i < length; i++) {
      expectedHeader[i] = "_col" + i;
    }
    return expectedHeader;
  }
}
