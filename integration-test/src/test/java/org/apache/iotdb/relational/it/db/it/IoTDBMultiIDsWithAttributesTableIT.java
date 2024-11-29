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
import java.util.Arrays;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
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
            + "floatNum FLOAT measurement, str TEXT measurement, bool BOOLEAN measurement, date DATE measurement, blob BLOB measurement, ts TIMESTAMP measurement, stringV STRING measurement, doubleNum DOUBLE measurement)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l1', 'c', 'd', 0,3,2947483648,231.2121,'coconut',FALSE)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool,blob,ts,doubleNum) values('d1', 'l2', 'y', 'z', 20,2,2147483648,434.12,'pineapple',TRUE,X'108DCD62',2024-09-24T06:15:35.000+00:00,6666.8)",
        "insert into table0(device, level, attr1, attr2, time,num,bigNum,floatNum,str,bool) values('d1', 'l3', 't', 'a', 40,1,2247483648,12.123,'apricot',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l4', 80,9,2147483646,43.12,'apple',FALSE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l5', 100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l1', 31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device, level, time,num,bigNum,floatNum,str,bool) values('d1', 'l2',31536000100,10,3147483648,231.55,'pumelo',FALSE)",
        "insert into table0(device,level,  time,num,bigNum,floatNum,str,bool,doubleNum) values('d1', 'l3',31536000500,4,2147493648,213.1,'peach',FALSE,6666.3)",
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
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,blob,ts,stringV,doubleNum) values('d2','l4',80,9,2147483646,43.12,'apple',FALSE,X'108DCD63',2024-09-20T06:15:35.000+00:00,'test-string2',6666.7)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',100,8,2147483964,4654.231,'papaya',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l1',31536000000,6,2147483650,1231.21,'banana',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l3',31536000500,4,2147493648,213.1,'peach',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',31536001000,5,2149783648,56.32,'orange',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l5',31536010000,7,2147983648,213.112,'lemon',TRUE)",
        "flush",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,ts,StringV) values('d2','l1',31536100000,11,2147468648,54.121,'pitaya',FALSE,2024-08-01T06:15:35.000+00:00,'test-string3')",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l2',41536000000,12,2146483648,45.231,'strawberry',FALSE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,doubleNum) values('d2','l3',41536000020,14,2907483648,231.34,'cherry',FALSE,6666.4)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool) values('d2','l4',41536900000,13,2107483648,54.12,'lychee',TRUE)",
        "insert into table0(device,level,time,num,bigNum,floatNum,str,bool,date) values('d2','l5',51536000000,15,3147483648,235.213,'watermelon',TRUE,'2023-01-01')"
      };

  private static final String[] sql3 =
      new String[] {
        "CREATE TABLE table1 (device string id, level string id, attr1 string attribute, attr2 string attribute, num int32 measurement, bigNum int64 measurement, "
            + "floatNum double measurement, str TEXT measurement, bool BOOLEAN measurement, date DATE measurement, blob BLOB measurement, doubleNum DOUBLE measurement)",
        "insert into table1(device, level, attr1, attr2, time, num, bigNum, floatNum, str, bool, date, blob) values('d1', 'l1', 'c', 'd', 0, 3, 2947483648,231.2121, 'coconut', FALSE, '2023-01-01', X'100DCD62')",
        "insert into table1(device, level, attr1, attr2, time, num, bigNum, floatNum, str, bool, blob) values('d1', 'l2', 'y', 'z', 20, 2, 2147483648,434.12, 'pineapple', TRUE, X'101DCD62')",
        "insert into table1(device, level, attr1, attr2, time, num, bigNum, floatNum, str, bool, blob) values('d1', 'l3', 't', 'a', 40, 1, 2247483648,12.123, 'apricot', TRUE, null)",
        "insert into table1(device, level, time, num, bigNum, floatNum, str, bool, blob) values('d1', 'l4', 80, 9, 2147483646, 43.12, 'apple', FALSE, X'104DCD62')",
        "insert into table1(device, level, time, num, bigNum, floatNum, str, bool, date, blob) values('d1', 'l5', 100, 8, 2147483964, 4654.231, 'papaya', TRUE, '2023-05-01', null)",
        "flush",
        "insert into table1(device, level, time, num, bigNum, floatNum, str, bool, blob) values('d1', 'l1', 31536000000, 6, 2147483650, 1231.21, 'banana', TRUE, X'106DCD62')",
        "insert into table1(device, time, num, bigNum, floatNum, str, bool, blob) values('d999', 31536000000, 6, 2147483650, 1231.21, 'banana', TRUE, X'107DCD62')",
        "insert into table1(level, time, num, bigNum, floatNum, str, bool, date, blob) values('l999', 31536000000, 6, 2147483650, 1231.21, 'banana', TRUE, '2023-10-01', X'108DCD62')",
        "flush",
        "insert into table1(time, device, level, attr1, attr2, num,bigNum,floatNum,str,bool) values(0, 'd11', 'l11', 'c', 'd', 3, 2947483648, 231.2121, 'coconut', FALSE)",
        "flush",
        "insert into table1(time, device, level, attr1, attr2, num,bigNum,floatNum,str,bool) values(10, 'd11', 'l11', 'c', 'd', 3, 2947483648, 231.2121, 'coconut', FALSE)",
        "flush",
        "insert into table1(time, device, level, attr1, attr2, num,bigNum,floatNum,str,bool) values(20, 'd11', 'l11', 'c', 'd', 3, 2947483648, 231.2121, 'coconut', FALSE)",
        "flush",
        "insert into table1(time, device, level, attr1, attr2, num,bigNum,floatNum,str,bool) values(30, 'd11', 'l11', 'c', 'd', 3, 2947483648, 231.2121, 'coconut', FALSE)",
        "flush",
        "insert into table1(time, device, level, attr1, attr2, num,bigNum,floatNum,str,bool) values(40, 'd11', 'l11', 'c', 'd', 3, 2947483648, 231.2121, 'coconut', FALSE)"
      };

  private static final String[] sql4 =
      new String[] {
        "create table students(region STRING ID, student_id INT32 MEASUREMENT, name STRING MEASUREMENT, genders text MEASUREMENT, date_of_birth DATE MEASUREMENT)",
        "create table teachers(region STRING ID, teacher_id INT32 MEASUREMENT, course_id INT32 MEASUREMENT, age INT32 MEASUREMENT)",
        "create table courses(course_id STRING ID, course_name STRING MEASUREMENT, teacher_id INT32 MEASUREMENT)",
        "create table grades(grade_id STRING ID, course_id INT32 MEASUREMENT, student_id INT32 MEASUREMENT, score INT32 MEASUREMENT)",
        "insert into students(time,region,student_id,name,genders,date_of_birth) values"
            + "(1,'haidian',1,'Lucy','女','2015-10-10'),(2,'haidian',2,'Jack','男','2015-09-24'),(3,'chaoyang',3,'Sam','男','2014-07-20'),(4,'chaoyang',4,'Lily','女','2015-03-28'),"
            + "(5,'xicheng',5,'Helen','女','2016-01-22'),(6,'changping',6,'Nancy','女','2017-12-20'),(7,'changping',7,'Mike','男','2016-11-22'),(8,'shunyi',8,'Bob','男','2016-05-12')",
        "insert into teachers(time,region,teacher_id,course_id,age) values"
            + "(1,'haidian',1001,10000001,25),(2,'haidian',1002,10000002,26),(3,'chaoyang',1003,10000003,28),"
            + "(4,'chaoyang',1004,10000004,27),(5,'xicheng',1005,10000005,26)",
        "insert into courses(time,course_id,course_name,teacher_id) values"
            + "(1,10000001,'数学',1001),(2,10000002,'语文',1002),(3,10000003,'英语',1003),"
            + "(4,10000004,'体育',1004),(5,10000005,'历史',1005)",
        "insert into grades(time,grade_id,course_id,student_id,score) values"
            + "(1,1111,10000001,1,99),(2,1112,10000002,2,90),(3,1113,10000003,3,85),(4,1114,10000004,4,89),(5,1115,10000005,5,98),"
            + "(6,1113,10000003,6,55),(7,1114,10000004,7,60),(8,1115,10000005,8,100),(9,1114,10000001,2,99),(10,1115,10000002,1,95)"
      };

  private static final String[] sql5 =
      new String[] {
        "create table tableA(device STRING ID, value INT32 MEASUREMENT)",
        "create table tableB(device STRING ID, value INT32 MEASUREMENT)",
        "create table tableC(device STRING ID, value INT32 MEASUREMENT)",
        "insert into tableA(time,device,value) values('2020-01-01 00:00:01.000', 'd1', 1)",
        "insert into tableA(time,device,value) values('2020-01-01 00:00:03.000', 'd1', 3)",
        "flush",
        "insert into tableA(time,device,value) values('2020-01-01 00:00:05.000', 'd2', 5)",
        "insert into tableA(time,device,value) values('2020-01-01 00:00:07.000', 'd2', 7)",
        "flush",
        "insert into tableB(time,device,value) values('2020-01-01 00:00:02.000', 'd1', 20)",
        "insert into tableB(time,device,value) values('2020-01-01 00:00:03.000', 'd1', 30)",
        "flush",
        "insert into tableB(time,device,value) values('2020-01-01 00:00:03.000', 'd333', 333)",
        "flush",
        "insert into tableB(time,device,value) values('2020-01-01 00:00:04.000', 'd2', 40)",
        "insert into tableB(time,device,value) values('2020-01-01 00:00:05.000', 'd2', 50)"
      };

  String[] expectedHeader;
  String[] retArray;
  static String sql;

  //  public static void main(String[] args) {
  //    for (String[] sqlList : Arrays.asList(sql1, sql2)) {
  //      for (String sql : sqlList) {
  //        System.out.println(sql+";");
  //      }
  //    }
  //  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setSortBufferSize(1024 * 1024L);
    EnvFactory.getEnv().initClusterEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxTsBlockLineNumber(2)
        .setMaxNumberOfPointsInPage(5);
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String[] sqlList : Arrays.asList(sql1, sql2, sql3, sql4, sql5)) {
        for (String sql : sqlList) {
          statement.execute(sql);
        }
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
            + "from table1 where device != 'd11' or device is null",
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

    expectedHeader = buildHeaders(1);
    retArray =
        new String[] {
          "10,",
        };
    tableResultSetEqualTest(
        "select count(*) from (select device from table0 group by device, level)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
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

    expectedHeader = new String[] {"attr1", "attr2"};
    retArray =
        new String[] {
          "c,d,", "d,c,", "t,a,", "vv,null,", "yy,zz,", "null,null,",
        };
    sql = "select attr1,attr2 from table0 group by attr1,attr2 order by attr1,attr2";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
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

    // flush multi times, generated multi tsfile
    expectedHeader = buildHeaders(1);
    sql = "select date_bin(40ms,time), first(time) from table1 where device='d11' group by 1";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.000Z,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.000Z,"
        };

    // TODO(beyyes) test below
    //        sql = "select count(*) from (\n" +
    //                "\tselect device, level, date_bin(1d, time) as bin, \n" +
    //                "\tcount(num) as count_num, count(*) as count_star, count(device) as
    // count_device,
    //     count(date) as count_date, count(attr1) as count_attr1, count(attr2) as count_attr2,
    //     count(time) as count_time, avg(num) as avg_num \n" +
    //                "\tfrom table0 \n" +
    //                "\tgroup by 3, device, level order by device, level, bin\n" +
    //                ")\n";
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

    // queried device is not exist
    expectedHeader = buildHeaders(3);
    sql = "select count(*), count(num), sum(num) from table0 where device='d_not_exist'";
    retArray = new String[] {"0,0,null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
    sql =
        "select count(*), count(num), sum(num) from table0 where device='d_not_exist1' or device='d_not_exist2'";
    retArray = new String[] {"0,0,null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // no data in given time range (push-down)
    sql = "select count(*), count(num), sum(num) from table0 where time>2100-04-26T18:01:40.000";
    retArray = new String[] {"0,0,null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // no data in given time range (no push-down)
    sql = "select count(*), count(num+1), sum(num) from table0 where time>2100-04-26T18:01:40.000";
    retArray = new String[] {"0,0,null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // only one device has data in queried time
    expectedHeader = buildHeaders(2);
    sql = "select count(num),sum(num) from table1 where time=0";
    retArray = new String[] {"2,6.0,"};
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

    expectedHeader = buildHeaders(30);
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,d2,l5,yy,zz,15,3147483648,4654.231,watermelon,true,2023-01-01,0x108dcd63,2024-09-25T06:15:35.000Z,test-string3,6666.8,1970-01-01T00:00:00.000Z,d1,l1,c,a,1,2107483648,12.123,apple,false,2022-01-01,0x108dcd62,2024-08-01T06:15:35.000Z,test-string1,6666.3,",
        };
    sql =
        "select max(time),max(device),max(level),max(attr1),max(attr2),max(num),max(bignum),max(floatnum),max(str),max(bool),max(date),max(blob),max(ts),max(stringv),max(doubleNum),min(time),min(device),min(level),min(attr1),min(attr2),min(num),min(bignum),min(floatnum),min(str),min(bool),min(date),min(blob),min(ts),min(stringv),min(doubleNum) from table0";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(24);
    retArray =
        new String[] {
          "1971-08-20T11:33:20.000Z,d2,l5,yy,zz,15,3147483648,4654.231,2023-01-01,1971-01-01T00:01:40.000Z,test-string3,6666.8,1970-01-01T00:00:00.000Z,d1,l1,c,a,1,2107483648,12.123,2022-01-01,1970-01-01T00:00:00.020Z,test-string1,6666.3,",
        };
    sql =
        "select max(time),max(device),max(level),max(attr1),max(attr2),max(num),max(bignum),max(floatnum),max(date),max(ts),max(stringv),max(doubleNum),min(time),min(device),min(level),min(attr1),min(attr2),min(num),min(bignum),min(floatnum),min(date),min(ts),min(stringv),min(doubleNum) from table0";
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

    // no push-down, test GroupedAccumulator
    expectedHeader =
        new String[] {
          "device", "level", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11", "_col12"
        };
    retArray =
        new String[] {
          "d1,l1,11,2947483648,1231.21,3,2147468648,54.121,pitaya,banana,true,false,3,",
          "d1,l2,12,3147483648,434.12,2,2146483648,45.231,strawberry,pineapple,true,false,3,",
          "d1,l3,14,2907483648,231.34,1,2147493648,12.123,peach,apricot,true,false,3,",
          "d1,l4,13,2149783648,56.32,5,2107483648,43.12,orange,apple,true,false,3,",
          "d1,l5,15,3147483648,4654.231,7,2147483964,213.112,watermelon,lemon,true,true,3,",
          "d2,l1,11,2947483648,1231.21,3,2147468648,54.121,pitaya,banana,true,false,3,",
          "d2,l2,12,3147483648,434.12,2,2146483648,45.231,strawberry,pineapple,true,false,3,",
          "d2,l3,14,2907483648,231.34,1,2147493648,12.123,peach,apricot,true,false,3,",
          "d2,l4,13,2149783648,56.32,5,2107483648,43.12,orange,apple,true,false,3,",
          "d2,l5,15,3147483648,4654.231,7,2147483964,213.112,watermelon,lemon,true,true,3,",
        };
    sql =
        "select device,level,max(num),max(bignum),max(floatnum),min(num),min(bignum),min(floatnum),max(str),min(str),max(bool),min(bool),count(num+1) from table0 group by device,level order by device,level";
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

    expectedHeader = buildHeaders(3);
    sql = "select last_by(time,num+1),last_by(num+1,time),last_by(num+1,floatnum+1) from table0";
    retArray = new String[] {"1971-08-20T11:33:20.000Z,16,16,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
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

    expectedHeader = buildHeaders(30);
    sql =
        "select max_by(time,blob),max_by(device,blob),max_by(level,blob),max_by(attr1,blob),max_by(attr2,blob),max_by(num,blob),max_by(bignum,blob),max_by(floatnum,blob),max_by(str,blob),max_by(bool,blob),max_by(date,blob),max_by(blob,blob),max_by(ts,blob),max_by(stringv,blob),max_by(doubleNum,blob),min_by(time,blob),min_by(device,blob),min_by(level,blob),min_by(attr1,blob),min_by(attr2,blob),min_by(num,blob),min_by(bignum,blob),min_by(floatnum,blob),min_by(str,blob),min_by(bool,blob),min_by(date,blob),min_by(blob,blob),min_by(ts,blob),min_by(stringv,blob),min_by(doubleNum,blob) from table0";
    retArray =
        new String[] {
          "1971-01-01T00:00:10.000Z,d1,l5,null,null,7,2147983648,213.112,lemon,true,null,0x108dcd63,2024-09-25T06:15:35.000Z,null,null,1970-01-01T00:00:00.020Z,d1,l2,yy,zz,2,2147483648,434.12,pineapple,true,null,0x108dcd62,2024-09-24T06:15:35.000Z,null,6666.8,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(3);
    sql = "select extreme(num),extreme(bignum),extreme(floatnum) from table0";
    retArray = new String[] {"15,3147483648,4654.231,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // no push-down, test GroupedAccumulator
    expectedHeader = buildHeaders(5);
    retArray =
        new String[] {
          "1971-01-01T00:01:40.000Z,1971-01-01T00:00:00.000Z,1971-01-01T00:00:00.000Z,1971-01-01T00:01:40.000Z,3,",
          "1971-04-26T17:46:40.000Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.020Z,1971-04-26T17:46:40.000Z,3,",
          "1971-01-01T00:00:00.500Z,1970-01-01T00:00:00.040Z,1970-01-01T00:00:00.040Z,1971-04-26T17:46:40.020Z,3,",
          "1971-01-01T00:00:01.000Z,1970-01-01T00:00:00.080Z,1971-04-26T18:01:40.000Z,1971-01-01T00:00:01.000Z,3,",
          "1971-08-20T11:33:20.000Z,1971-01-01T00:00:10.000Z,1971-08-20T11:33:20.000Z,1970-01-01T00:00:00.100Z,3,",
          "1971-01-01T00:01:40.000Z,1971-01-01T00:00:00.000Z,1971-01-01T00:00:00.000Z,1971-01-01T00:01:40.000Z,3,",
          "1971-04-26T17:46:40.000Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.020Z,1971-04-26T17:46:40.000Z,3,",
          "1971-01-01T00:00:00.500Z,1970-01-01T00:00:00.040Z,1970-01-01T00:00:00.040Z,1971-04-26T17:46:40.020Z,3,",
          "1971-01-01T00:00:01.000Z,1970-01-01T00:00:00.080Z,1971-04-26T18:01:40.000Z,1971-01-01T00:00:01.000Z,3,",
          "1971-08-20T11:33:20.000Z,1971-01-01T00:00:10.000Z,1971-08-20T11:33:20.000Z,1970-01-01T00:00:00.100Z,3,",
        };
    sql =
        "select max_by(time,str),min_by(time,str),max_by(time,bool),min_by(time,bool),count(num+1) from table0 group by device,level order by device,level";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void modeTest() {
    expectedHeader = buildHeaders(15);
    sql =
        "select mode(time),mode(device),mode(level),mode(attr1),mode(attr2),mode(num),mode(bignum),mode(floatnum),mode(date),mode(str),mode(bool),mode(date),mode(ts),mode(stringv),mode(doublenum) from table0 where device='d2' and level='l4' and time=80";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.080Z,d2,l4,null,null,9,2147483646,43.12,null,apple,false,null,2024-09-20T06:15:35.000Z,test-string2,6666.7,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(10);
    sql =
        "select mode(device),mode(level),mode(attr1),mode(attr2),mode(date),mode(bool),mode(date),mode(ts),mode(stringv),mode(doublenum) from table0 where device='d2' and level='l1'";
    retArray =
        new String[] {
          "d2,l1,d,c,null,false,null,null,null,null,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(1);
    sql =
        "select mode(stringv) from table0 where device='d2' and level='l1' and stringv is not null";
    retArray =
        new String[] {
          "test-string3,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // no push-down, test GroupedAccumulator
    expectedHeader = buildHeaders(16);
    sql =
        "select mode(time),mode(device),mode(level),mode(attr1),mode(attr2),mode(num),mode(bignum),mode(floatnum),mode(date),mode(str),mode(bool),mode(date),mode(ts),mode(stringv),mode(doublenum),count(num+1) from table0 where device='d2' and level='l4' and time=80 group by device, level";
    retArray =
        new String[] {
          "1970-01-01T00:00:00.080Z,d2,l4,null,null,9,2147483646,43.12,null,apple,false,null,2024-09-20T06:15:35.000Z,test-string2,6666.7,1,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(11);
    sql =
        "select mode(device),mode(level),mode(attr1),mode(attr2),mode(date),mode(bool),mode(date),mode(ts),mode(stringv),mode(doublenum),count(num+1) from table0 where device='d2' and level='l1' group by device, level";
    retArray =
        new String[] {
          "d2,l1,d,c,null,false,null,null,null,null,3,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(2);
    sql =
        "select mode(stringv),count(num+1) from table0 where device='d2' and level='l1' and stringv is not null group by device, level";
    retArray =
        new String[] {
          "test-string3,1,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void varianceTest() {
    expectedHeader = buildHeaders(18);
    sql =
        "select \n"
            + "round(variance(num),1),round(var_pop(num),1),round(var_samp(num),1),round(stddev(num),1),round(stddev_pop(num),1),round(stddev_samp(num),1),\n"
            + "round(variance(floatnum),1),round(var_pop(floatnum),1),round(var_samp(floatnum),1),round(stddev(floatnum),1),round(stddev_pop(floatnum),1),round(stddev_samp(floatnum),1),\n"
            + "round(variance(doublenum),1),round(var_pop(doublenum),1),round(var_samp(doublenum),1),round(stddev(doublenum),1),round(stddev_pop(doublenum),1),round(stddev_samp(doublenum),1) from table0 where device='d2' and level='l4'";
    retArray =
        new String[] {
          "16.0,10.7,16.0,4.0,3.3,4.0,50.0,33.3,50.0,7.1,5.8,7.1,null,0.0,null,null,0.0,null,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(19);
    sql =
        "select \n"
            + "round(variance(num),1),round(var_pop(num),1),round(var_samp(num),1),round(stddev(num),1),round(stddev_pop(num),1),round(stddev_samp(num),1),\n"
            + "round(variance(floatnum),1),round(var_pop(floatnum),1),round(var_samp(floatnum),1),round(stddev(floatnum),1),round(stddev_pop(floatnum),1),round(stddev_samp(floatnum),1),\n"
            + "round(variance(doublenum),1),round(var_pop(doublenum),1),round(var_samp(doublenum),1),round(stddev(doublenum),1),round(stddev_pop(doublenum),1),round(stddev_samp(doublenum),1), count(num+1) from table0 where device='d2' and level='l4' group by device, level";
    retArray =
        new String[] {
          "16.0,10.7,16.0,4.0,3.3,4.0,50.0,33.3,50.0,7.1,5.8,7.1,null,0.0,null,null,0.0,null,3,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "select \n"
            + "round(variance(num),1),round(var_pop(num),1),round(var_samp(num),1),round(stddev(num),1),round(stddev_pop(num),1),round(stddev_samp(num),1),\n"
            + "round(variance(floatnum),1),round(var_pop(floatnum),1),round(var_samp(floatnum),1),round(stddev(floatnum),1),round(stddev_pop(floatnum),1),round(stddev_samp(floatnum),1),\n"
            + "round(variance(doublenum),1),round(var_pop(doublenum),1),round(var_samp(doublenum),1),round(stddev(doublenum),1),round(stddev_pop(doublenum),1),round(stddev_samp(doublenum),1), count(num+1) from table0 group by device";
    retArray =
        new String[] {
          "20.0,18.7,20.0,4.5,4.3,4.5,1391642.5,1298866.4,1391642.5,1179.7,1139.7,1179.7,0.1,0.1,0.1,0.4,0.2,0.4,15,",
          "20.0,18.7,20.0,4.5,4.3,4.5,1391642.5,1298866.4,1391642.5,1179.7,1139.7,1179.7,0.0,0.0,0.0,0.2,0.2,0.2,15,"
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = buildHeaders(18);
    sql =
        "select \n"
            + "round(variance(num),1),round(var_pop(num),1),round(var_samp(num),1),round(stddev(num),1),round(stddev_pop(num),1),round(stddev_samp(num),1),\n"
            + "round(variance(floatnum),1),round(var_pop(floatnum),1),round(var_samp(floatnum),1),round(stddev(floatnum),1),round(stddev_pop(floatnum),1),round(stddev_samp(floatnum),1),\n"
            + "round(variance(doublenum),1),round(var_pop(doublenum),1),round(var_samp(doublenum),1),round(stddev(doublenum),1),round(stddev_pop(doublenum),1),round(stddev_samp(doublenum),1) from table0 group by device";
    retArray =
        new String[] {
          "20.0,18.7,20.0,4.5,4.3,4.5,1391642.5,1298866.4,1391642.5,1179.7,1139.7,1179.7,0.1,0.1,0.1,0.4,0.2,0.4,",
          "20.0,18.7,20.0,4.5,4.3,4.5,1391642.5,1298866.4,1391642.5,1179.7,1139.7,1179.7,0.0,0.0,0.0,0.2,0.2,0.2,"
        };
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
    expectedHeader =
        new String[] {"time", "device", "level", "num", "device", "attr2", "num", "str"};
    retArray =
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

    sql =
        "select t1.time as time1, t2.time as time2, "
            + "  t1.device as device1, "
            + "  t1.value as value1, "
            + "  t2.device as device2, "
            + "  t2.value as value2  from tableA t1 full join tableB t2 on t1.time = t2.time "
            + "order by COALESCE(time1, time2),device1,device2";
    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,null,d1,1,null,null,",
          "null,2020-01-01T00:00:02.000Z,null,null,d1,20,",
          "2020-01-01T00:00:03.000Z,2020-01-01T00:00:03.000Z,d1,3,d1,30,",
          "2020-01-01T00:00:03.000Z,2020-01-01T00:00:03.000Z,d1,3,d333,333,",
          "null,2020-01-01T00:00:04.000Z,null,null,d2,40,",
          "2020-01-01T00:00:05.000Z,2020-01-01T00:00:05.000Z,d2,5,d2,50,",
          "2020-01-01T00:00:07.000Z,null,d2,7,null,null,",
        };
    expectedHeader = new String[] {"time1", "time2", "device1", "value1", "device2", "value2"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "select time, "
            + "  t1.device as device1, "
            + "  t1.value as value1, "
            + "  t2.device as device2, "
            + "  t2.value as value2  from tableA t1 full join tableB t2 USING(time) "
            + "order by time,device1,device2";
    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,d1,1,null,null,",
          "2020-01-01T00:00:02.000Z,null,null,d1,20,",
          "2020-01-01T00:00:03.000Z,d1,3,d1,30,",
          "2020-01-01T00:00:03.000Z,d1,3,d333,333,",
          "2020-01-01T00:00:04.000Z,null,null,d2,40,",
          "2020-01-01T00:00:05.000Z,d2,5,d2,50,",
          "2020-01-01T00:00:07.000Z,d2,7,null,null,",
        };
    expectedHeader = new String[] {"time", "device1", "value1", "device2", "value2"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    // empty table join non-empty table
    sql =
        "select time, "
            + "  t1.device as device1, "
            + "  t1.value as value1, "
            + "  t2.device as device2, "
            + "  t2.value as value2  from tableC t1 full join tableB t2 USING(time) "
            + "order by time,device1,device2";
    retArray =
        new String[] {
          "2020-01-01T00:00:02.000Z,null,null,d1,20,",
          "2020-01-01T00:00:03.000Z,null,null,d1,30,",
          "2020-01-01T00:00:03.000Z,null,null,d333,333,",
          "2020-01-01T00:00:04.000Z,null,null,d2,40,",
          "2020-01-01T00:00:05.000Z,null,null,d2,50,",
        };
    expectedHeader = new String[] {"time", "device1", "value1", "device2", "value2"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "select time, "
            + "  t1.device as device1, "
            + "  t1.value as value1, "
            + "  t2.device as device2, "
            + "  t2.value as value2 "
            + "from (select * from tableA where device='d1') t1 full join (select * from tableB where device='d2') t2 USING(time) order by time, device1, device2";
    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,d1,1,null,null,",
          "2020-01-01T00:00:03.000Z,d1,3,null,null,",
          "2020-01-01T00:00:04.000Z,null,null,d2,40,",
          "2020-01-01T00:00:05.000Z,null,null,d2,50,",
        };
    expectedHeader = new String[] {"time", "device1", "value1", "device2", "value2"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  // has filter
  @Test
  public void fullOuterJoinTest2() {
    expectedHeader =
        new String[] {"time", "device", "level", "t1_num_add", "device", "attr2", "num", "str"};
    retArray =
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
          "1970-01-01T00:00:00.100Z,d1,l5,9,null,null,null,null,",
          "1970-01-01T00:00:00.100Z,d2,l5,9,null,null,null,null,",
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

  @Test
  public void fourTableJoinTest() {
    expectedHeader =
        new String[] {
          "time", "s_id", "s_name", "s_birth", "t_id", "t_c_id", "c_name", "g_id", "score"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,1,Lucy,2015-10-10,1001,10000001,数学,1111,99,",
          "1970-01-01T00:00:00.002Z,2,Jack,2015-09-24,1002,10000002,语文,1112,90,",
          "1970-01-01T00:00:00.003Z,3,Sam,2014-07-20,1003,10000003,英语,1113,85,",
          "1970-01-01T00:00:00.004Z,4,Lily,2015-03-28,1004,10000004,体育,1114,89,",
          "1970-01-01T00:00:00.005Z,5,Helen,2016-01-22,1005,10000005,历史,1115,98,",
        };
    sql =
        "select s.time,"
            + "         s.student_id as s_id, s.name as s_name, s.date_of_birth as s_birth,"
            + "         t.teacher_id as t_id, t.course_id as t_c_id,"
            + "         c.course_name as c_name,"
            + "         g.grade_id as g_id, g.score as score "
            + "from students s, teachers t, courses c, grades g "
            + "where s.time=t.time AND c.time=g.time AND s.time=c.time "
            + "order by s.student_id, t.teacher_id, c.course_id,g.grade_id";
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);

    sql =
        "select s.region, s.name,"
            + "           t.teacher_id,"
            + "           c.course_name,"
            + "           g.score "
            + "from students s, teachers t, courses c, grades g "
            + "where s.time=c.time and c.time=g.time";
    tableAssertTestFail(
        sql,
        "701: Cross join is not supported in current version, each table must have at least one equiJoinClause",
        DATABASE_NAME);
  }

  @Test
  public void innerJoinTest() {
    expectedHeader = new String[] {"time", "device1", "value1", "device2", "value2"};
    sql =
        "SELECT "
            + "  t1.time, "
            + "  t1.device as device1, "
            + "  t1.value as value1, "
            + "  t2.device as device2, "
            + "  t2.value as value2 "
            + "FROM "
            + "  tableA t1 JOIN tableB t2 "
            + "ON t1.time = t2.time order by t1.time, device1, device2";
    retArray =
        new String[] {
          "2020-01-01T00:00:03.000Z,d1,3,d1,30,",
          "2020-01-01T00:00:03.000Z,d1,3,d333,333,",
          "2020-01-01T00:00:05.000Z,d2,5,d2,50,",
        };
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
