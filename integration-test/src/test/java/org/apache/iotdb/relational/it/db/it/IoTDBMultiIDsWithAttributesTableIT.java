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
            + "floatNum double measurement, str TEXT measurement, bool BOOLEAN measurement, date DATE measurement)",
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
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',80,9,2147483646,43.12,'apple',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l1',31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',31536000500,4,2147493648,213.1,'peach',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',31536001000,5,2149783648,56.32,'orange',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',31536010000,7,2147983648,213.112,'lemon',TRUE)",
        "flush",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l1',31536100000,11,2147468648,54.121,'pitaya',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l2',41536000000,12,2146483648,45.231,'strawberry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',41536000020,14,2907483648,231.34,'cherry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',41536900000,13,2107483648,54.12,'lychee',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,date) values('d2','l5',51536000000,15,3147483648,235.213,'watermelon',TRUE,'2023-01-01')"
      };

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
}
