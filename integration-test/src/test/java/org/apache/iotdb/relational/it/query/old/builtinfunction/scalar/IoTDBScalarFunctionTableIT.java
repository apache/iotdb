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

package org.apache.iotdb.relational.it.query.old.builtinfunction.scalar;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.*;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBScalarFunctionTableIT {
  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        // absSQL
        "create table absTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO absTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO absTable(Time,device_id,s2) values(2, 'd1',  -1)",
        "INSERT INTO absTable(Time,device_id,s2) values(3, 'd1',  -2)",
        "INSERT INTO absTable(Time,device_id,s3) values(2, 'd1',  -1)",
        "INSERT INTO absTable(Time,device_id,s3) values(3, 'd1',  -2)",
        "INSERT INTO absTable(Time,device_id,s4) values(2, 'd1',  -1.5)",
        "INSERT INTO absTable(Time,device_id,s4) values(3, 'd1',  -2.5)",
        "INSERT INTO absTable(Time,device_id,s5) values(2, 'd1',  -1.5)",
        "INSERT INTO absTable(Time,device_id,s5) values(3, 'd1',  -2.5)",
        // acosSQL
        "create table acosTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO acosTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1, 'abcd', X'abcd')",
        "INSERT INTO acosTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO acosTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO acosTable(Time,device_id,s4) values(2, 'd1',  0.5)",
        "INSERT INTO acosTable(Time,device_id,s5) values(2, 'd1',  0.5)",
        // asinSQL
        "create table asinTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO asinTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1, 'abcd', X'abcd')",
        "INSERT INTO asinTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO asinTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO asinTable(Time,device_id,s4) values(2, 'd1',  0.5)",
        "INSERT INTO asinTable(Time,device_id,s5) values(2, 'd1',  0.5)",
        // atanSQL
        "create table atanTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO atanTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO atanTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO atanTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO atanTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO atanTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO atanTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO atanTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO atanTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO atanTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // ceilSQL
        "create table ceilTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO ceilTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO ceilTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO ceilTable(Time,device_id,s2) values(3, 'd1',  -2)",
        "INSERT INTO ceilTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO ceilTable(Time,device_id,s3) values(3, 'd1',  -2)",
        "INSERT INTO ceilTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO ceilTable(Time,device_id,s4) values(3, 'd1',  -2.5)",
        "INSERT INTO ceilTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO ceilTable(Time,device_id,s5) values(3, 'd1',  -2.5)",
        // concatSQL
        "create table concatTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO concatTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO concatTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO concatTable(Time,device_id,s1) values(3, 'd1', 'efgh')",
        "INSERT INTO concatTable(Time,device_id,s1) values(4, 'd1', null)",
        "INSERT INTO concatTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO concatTable(Time,device_id,s9) values(3, 'd1', 'efgh')",
        "INSERT INTO concatTable(Time,device_id,s9) values(4, 'd1', 'haha')",
        // cosSQL
        "create table cosTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO cosTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO cosTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO cosTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO cosTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO cosTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO cosTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO cosTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO cosTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO cosTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // coshSQL
        "create table coshTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO coshTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO coshTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO coshTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO coshTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO coshTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO coshTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO coshTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO coshTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO coshTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // degreesSQL
        "create table degreesTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO degreesTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO degreesTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO degreesTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO degreesTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO degreesTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO degreesTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO degreesTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO degreesTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO degreesTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // endsWithSQL
        "create table endsWithTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO endsWithTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO endsWithTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO endsWithTable(Time,device_id,s1) values(3, 'd1', 'efgh')",
        "INSERT INTO endsWithTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO endsWithTable(Time,device_id,s9) values(3, 'd1', 'efgh')",
        // expSQL
        "create table expTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO expTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO expTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO expTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO expTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO expTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO expTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO expTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO expTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO expTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // floorSQL
        "create table floorTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO floorTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO floorTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO floorTable(Time,device_id,s2) values(3, 'd1',  -2)",
        "INSERT INTO floorTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO floorTable(Time,device_id,s3) values(3, 'd1',  -2)",
        "INSERT INTO floorTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO floorTable(Time,device_id,s4) values(3, 'd1',  -2.5)",
        "INSERT INTO floorTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO floorTable(Time,device_id,s5) values(3, 'd1',  -2.5)",
        // lengthSQL
        "create table lengthTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO lengthTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO lengthTable(Time,device_id,s1) values(2, 'd1', 'test')",
        "INSERT INTO lengthTable(Time,device_id,s1) values(3, 'd1', 'abcdefg')",
        "INSERT INTO lengthTable(Time,device_id,s9) values(2, 'd1', 'test')",
        "INSERT INTO lengthTable(Time,device_id,s9) values(3, 'd1', 'abcdefg')",
        // lnSQL
        "create table lnTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO lnTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO lnTable(Time,device_id,s2) values(2, 'd1',  0)",
        "INSERT INTO lnTable(Time,device_id,s2) values(3, 'd1',  -3)",
        "INSERT INTO lnTable(Time,device_id,s3) values(2, 'd1',  0)",
        "INSERT INTO lnTable(Time,device_id,s3) values(3, 'd1',  -3)",
        "INSERT INTO lnTable(Time,device_id,s4) values(2, 'd1',  0.0)",
        "INSERT INTO lnTable(Time,device_id,s4) values(3, 'd1',  -3.5)",
        "INSERT INTO lnTable(Time,device_id,s5) values(2, 'd1',  0.0)",
        "INSERT INTO lnTable(Time,device_id,s5) values(3, 'd1',  -3.5)",
        // log10SQL
        "create table log10Table(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO log10Table(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO log10Table(Time,device_id,s2) values(2, 'd1',  0)",
        "INSERT INTO log10Table(Time,device_id,s2) values(3, 'd1',  -3)",
        "INSERT INTO log10Table(Time,device_id,s3) values(2, 'd1',  0)",
        "INSERT INTO log10Table(Time,device_id,s3) values(3, 'd1',  -3)",
        "INSERT INTO log10Table(Time,device_id,s4) values(2, 'd1',  0.0)",
        "INSERT INTO log10Table(Time,device_id,s4) values(3, 'd1',  -3.5)",
        "INSERT INTO log10Table(Time,device_id,s5) values(2, 'd1',  0.0)",
        "INSERT INTO log10Table(Time,device_id,s5) values(3, 'd1',  -3.5)",
        // lowerSQL
        "create table lowerTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO lowerTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'ABCD', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ABCD', X'abcd')",
        "INSERT INTO lowerTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO lowerTable(Time,device_id,s1) values(3, 'd1', 'Abcdefg')",
        "INSERT INTO lowerTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO lowerTable(Time,device_id,s9) values(3, 'd1', 'Abcdefg')",
        // radiansSQL
        "create table radiansTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO radiansTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO radiansTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO radiansTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO radiansTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO radiansTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO radiansTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO radiansTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO radiansTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO radiansTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // regexpLikeSQL
        "create table regexpLikeTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO regexpLikeTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO regexpLikeTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO regexpLikeTable(Time,device_id,s1) values(3, 'd1', 'efgh')",
        "INSERT INTO regexpLikeTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO regexpLikeTable(Time,device_id,s9) values(3, 'd1', '[e-g]+')",
        // signSQL
        "create table signTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO signTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO signTable(Time,device_id,s2) values(2, 'd1',  0)",
        "INSERT INTO signTable(Time,device_id,s2) values(3, 'd1',  -1)",
        "INSERT INTO signTable(Time,device_id,s3) values(2, 'd1',  0)",
        "INSERT INTO signTable(Time,device_id,s3) values(3, 'd1',  -1)",
        "INSERT INTO signTable(Time,device_id,s4) values(2, 'd1',  0.0)",
        "INSERT INTO signTable(Time,device_id,s4) values(3, 'd1',  -1.0)",
        "INSERT INTO signTable(Time,device_id,s5) values(2, 'd1',  0.0)",
        "INSERT INTO signTable(Time,device_id,s5) values(3, 'd1',  -1.0)",
        // sinSQL
        "create table sinTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO sinTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO sinTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO sinTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO sinTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO sinTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO sinTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO sinTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO sinTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO sinTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // sinhSQL
        "create table sinhTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO sinhTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO sinhTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO sinhTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO sinhTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO sinhTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO sinhTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO sinhTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO sinhTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO sinhTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // sqrtSQL
        "create table sqrtTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO sqrtTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO sqrtTable(Time,device_id,s2) values(2, 'd1',  0)",
        "INSERT INTO sqrtTable(Time,device_id,s2) values(3, 'd1',  -1)",
        "INSERT INTO sqrtTable(Time,device_id,s3) values(2, 'd1',  0)",
        "INSERT INTO sqrtTable(Time,device_id,s3) values(3, 'd1',  -1)",
        "INSERT INTO sqrtTable(Time,device_id,s4) values(2, 'd1',  0.0)",
        "INSERT INTO sqrtTable(Time,device_id,s4) values(3, 'd1',  -1.5)",
        "INSERT INTO sqrtTable(Time,device_id,s5) values(2, 'd1',  0.0)",
        "INSERT INTO sqrtTable(Time,device_id,s5) values(3, 'd1',  -1.5)",
        // startsWithSQL
        "create table startsWithTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO startsWithTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO startsWithTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO startsWithTable(Time,device_id,s1) values(3, 'd1', 'efgh')",
        "INSERT INTO startsWithTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO startsWithTable(Time,device_id,s9) values(3, 'd1', 'efgh')",
        // strcmpSQL
        "create table strcmpTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO strcmpTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO strcmpTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO strcmpTable(Time,device_id,s1) values(3, 'd1', 'efgh')",
        "INSERT INTO strcmpTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO strcmpTable(Time,device_id,s9) values(3, 'd1', 'efgh')",
        // strposSQL
        "create table strposTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO strposTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO strposTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO strposTable(Time,device_id,s1) values(3, 'd1', 'efgh')",
        "INSERT INTO strposTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO strposTable(Time,device_id,s9) values(3, 'd1', 'efgh')",
        // tanSQL
        "create table tanTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO tanTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO tanTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO tanTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO tanTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO tanTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO tanTable(Time,device_id,s4) values(2, 'd1',  1.57079632675)",
        "INSERT INTO tanTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO tanTable(Time,device_id,s5) values(2, 'd1',  1.57079632675)",
        "INSERT INTO tanTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // tanhSQL
        "create table tanhTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO tanhTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO tanhTable(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO tanhTable(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO tanhTable(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO tanhTable(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO tanhTable(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO tanhTable(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO tanhTable(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO tanhTable(Time,device_id,s5) values(3, 'd1',  3.5)",
        // trimSQL
        "create table trimTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO trimTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO trimTable(Time,device_id,s1) values(2, 'd1', 'xyTestxy')",
        "INSERT INTO trimTable(Time,device_id,s1) values(3, 'd1', '  Test  ')",
        "INSERT INTO trimTable(Time,device_id,s9) values(2, 'd1', 'xy')",
        "INSERT INTO trimTable(Time,device_id,s9) values(3, 'd1', ' T')",
        // upperSQL
        "create table upperTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO upperTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO upperTable(Time,device_id,s1) values(2, 'd1', 'Test')",
        "INSERT INTO upperTable(Time,device_id,s1) values(3, 'd1', 'Abcdefg')",
        "INSERT INTO upperTable(Time,device_id,s9) values(2, 'd1', 'Test')",
        "INSERT INTO upperTable(Time,device_id,s9) values(3, 'd1', 'Abcdefg')",
        // no args SQL
        "create table NoArgTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO NoArgTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 0, 0, 0, 0, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        // dateBinSQL use s8 to calculate
        "create table dateBinTable(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        // 2024-01-01T00:00:00.000Z
        "INSERT INTO dateBinTable(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'Test', 1, 1, 1, 1, true, '2024-01-01', 1704067200000, 'abcd', X'abcd')",
        // 2024-01-01T01:00:00.000Z
        "INSERT INTO dateBinTable(Time,device_id,s1,s8) values(2, 'd1', 'Test', 1704070800000)",
        // 2024-01-01T01:59:00.000Z
        "INSERT INTO dateBinTable(Time,device_id,s1,s8) values(3, 'd1', 'Test', 1704074340000)",
        // 2023-12-31T23:59:00.000Z
        "INSERT INTO dateBinTable(Time,device_id,s1,s8) values(4, 'd1', 'Test', 1704067140000)",
        // 1969-12-31T23:59:00.000Z
        "INSERT INTO dateBinTable(Time,device_id,s1,s8) values(5, 'd1', 'Test', -60000)",
        // null
        "INSERT INTO dateBinTable(Time,device_id,s1,s8) values(6, 'd1', 'Test', null)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void absTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE
    String[] expectedHeader =
        new String[] {"time", "s2", "_col2", "s3", "_col4", "s4", "_col6", "s5", "_col8"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,1,1,1,1,1.0,1.0,1.0,1.0,",
          "1970-01-01T00:00:00.002Z,-1,1,-1,1,-1.5,1.5,-1.5,1.5,",
          "1970-01-01T00:00:00.003Z,-2,2,-2,2,-2.5,2.5,-2.5,2.5,",
        };
    tableResultSetEqualTest(
        "select time,s2,abs(s2),s3,abs(s3),s4,abs(s4),s5,abs(s5) from absTable",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void testINT64NotIn() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE
    String[] expectedHeader = new String[] {"time", "s3"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.002Z,-1,", "1970-01-01T00:00:00.003Z,-2,",
        };
    tableResultSetEqualTest(
        "select time,s3 from absTable where s3 not in (1)",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void testBlobCompare() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE
    String[] expectedHeader = new String[] {"s10", "res1", "res2", "res3"};
    String[] expectedAns =
        new String[] {
          "0xabcd,true,true,true,", "null,null,null,null,", "null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select s10, s10 > x'2d' as res1, s10 <> x'2d' as res2, s10 = X'abcd' as res3 from absTable",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void testDateCompare() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE
    String[] expectedHeader = new String[] {"s7", "res1", "res2", "res3"};
    String[] expectedAns =
        new String[] {
          "2021-10-01,true,true,true,", "null,null,null,null,", "null,null,null,null,",
        };
    // add it back while supporting Implicit conversion
    //    tableResultSetEqualTest(
    //        "select s7, s7 < '2022-12-12' as res1, s7 <> '2022-12-12' as res2, s7 = '2021-10-01'
    // as res3 from absTable",
    //        expectedHeader,
    //        expectedAns,
    //        DATABASE_NAME);
    tableResultSetEqualTest(
        "select s7, s7 < CAST('2022-12-12' AS DATE) as res1, s7 <> CAST('2022-12-12' AS DATE) AS res2, s7 = CAST('2021-10-01' AS DATE) as res3 from absTable",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);

    expectedHeader = new String[] {"s3", "s7"};
    expectedAns =
        new String[] {
          "1,2021-10-01,",
        };

    tableResultSetEqualTest(
        "select s3, s7 from absTable where s7 in (CAST('2021-10-01' AS DATE), CAST('2021-10-02' AS DATE))",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void testTimestampCompare() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE
    String[] expectedHeader = new String[] {"s2", "s8"};
    String[] expectedAns =
        new String[] {
          "1,2021-10-01T00:00:00.000Z,",
        };

    tableResultSetEqualTest(
        "select s2, s8 from absTable where s8 IN (CAST('2021-10-01T08:00:00.000+08:00' AS TIMESTAMP), CAST('2021-10-01T00:00:00.000Z' AS TIMESTAMP))",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select s2, s8 from absTable where s8=CAST('2021-10-01T08:00:00.000+08:00' AS TIMESTAMP)",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select s2, s8 from absTable where s8=2021-10-01T08:00:00.000+08:00",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void absTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,abs(s2,1) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function abs only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,abs(s1) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function abs only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,abs(s6) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function abs only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,abs(s7) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function abs only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,abs(s9) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function abs only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,abs(s10) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function abs only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s7, s7 < '2022-12-12', s7 <> '2022-12-12', s7 = '2021-10-01' from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": Cannot apply operator: DATE < STRING",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select CAST(s1 AS INT32) from absTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": Cannot cast abcd to INT32 type",
        DATABASE_NAME);
  }

  @Test
  public void acosTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP And range of input value is [-1, 1]
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.acos(1), Math.acos(2)};
    Double[] expectedResultLong = new Double[] {Math.acos(1), Math.acos(2)};
    Double[] expectedResultFloat = new Double[] {Math.acos(1.0f), Math.acos(0.5f)};
    Double[] expectedResultDouble = new Double[] {Math.acos(1.0), Math.acos(0.5)};
    testDoubleResult(
        "select time,acos(s2),acos(s3),acos(s4),acos(s5) from acosTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void acosTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,acos(s2,1) from acosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function acos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,acos(s1) from acosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function acos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,acos(s6) from acosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function acos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,acos(s7) from acosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function acos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,acos(s9) from acosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function acos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,acos(s10) from acosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function acos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void asinTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP And range of input value is [-1, 1]
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.asin(1), Math.asin(2)};
    Double[] expectedResultLong = new Double[] {Math.asin(1), Math.asin(2)};
    Double[] expectedResultFloat = new Double[] {Math.asin(1.0f), Math.asin(0.5f)};
    Double[] expectedResultDouble = new Double[] {Math.asin(1.0), Math.asin(0.5)};
    testDoubleResult(
        "select time,asin(s2),asin(s3),asin(s4),asin(s5) from asinTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void asinTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,asin(s2,1) from asinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function asin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,asin(s1) from asinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function asin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,asin(s6) from asinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function asin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,asin(s7) from asinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function asin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,asin(s9) from asinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function asin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,asin(s10) from asinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function asin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void atanTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.atan(1), Math.atan(2), Math.atan(3)};
    Double[] expectedResultLong = new Double[] {Math.atan(1), Math.atan(2), Math.atan(3)};
    Double[] expectedResultFloat = new Double[] {Math.atan(1.0f), Math.atan(2.5f), Math.atan(3.5f)};
    Double[] expectedResultDouble = new Double[] {Math.atan(1.0), Math.atan(2.5), Math.atan(3.5)};
    testDoubleResult(
        "select time,atan(s2),atan(s3),atan(s4),atan(s5) from atanTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void atanTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,atan(s2,1) from atanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function atan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,atan(s1) from atanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function atan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,atan(s6) from atanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function atan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,atan(s7) from atanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function atan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,atan(s9) from atanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function atan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,atan(s10) from atanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function atan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void ceilTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.ceil(1), Math.ceil(2), Math.ceil(-2)};
    Double[] expectedResultLong = new Double[] {Math.ceil(1), Math.ceil(2), Math.ceil(-2)};
    Double[] expectedResultFloat =
        new Double[] {Math.ceil(1.0f), Math.ceil(2.5f), Math.ceil(-2.5f)};
    Double[] expectedResultDouble = new Double[] {Math.ceil(1.0), Math.ceil(2.5), Math.ceil(-2.5)};
    testDoubleResult(
        "select time,ceil(s2),ceil(s3),ceil(s4),ceil(s5) from ceilTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void ceilTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,ceil(s2,1) from ceilTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ceil only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,ceil(s1) from ceilTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ceil only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,ceil(s6) from ceilTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ceil only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,ceil(s7) from ceilTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ceil only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,ceil(s9) from ceilTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ceil only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,ceil(s10) from ceilTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ceil only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void concatTestNormal() {
    // support the (measurement, ConstantArgument)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcdes,ab,abes,",
          "1970-01-01T00:00:00.002Z,Test,Testes,Test,Testes,",
          "1970-01-01T00:00:00.003Z,efgh,efghes,efgh,efghes,",
          "1970-01-01T00:00:00.004Z,null,es,haha,hahaes,",
        };
    tableResultSetEqualTest(
        "select time,s1,concat(s1,'es'),s9,concat(s9,'es') from concatTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (ConstantArgument, measurement)
    expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,esabcd,ab,esab,",
          "1970-01-01T00:00:00.002Z,Test,esTest,Test,esTest,",
          "1970-01-01T00:00:00.003Z,efgh,esefgh,efgh,esefgh,",
          "1970-01-01T00:00:00.004Z,null,es,haha,eshaha,",
        };
    tableResultSetEqualTest(
        "select time,s1,concat('es',s1),s9,concat('es',s9) from concatTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (measurement, measurement)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,abcdab,",
          "1970-01-01T00:00:00.002Z,Test,Test,TestTest,",
          "1970-01-01T00:00:00.003Z,efgh,efgh,efghefgh,",
          "1970-01-01T00:00:00.004Z,null,haha,haha,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,concat(s1,s9) from concatTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (string1,string2,string3...stringN)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,headerabcdbodyabtail,",
          "1970-01-01T00:00:00.002Z,Test,Test,headerTestbodyTesttail,",
          "1970-01-01T00:00:00.003Z,efgh,efgh,headerefghbodyefghtail,",
          "1970-01-01T00:00:00.004Z,null,haha,headerbodyhahatail,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,concat('header',s1,'body',s9,'tail') from concatTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void concatTestFail() {
    // case 1: less than two argument
    tableAssertTestFail(
        "select s1,concat(s1) from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s2,concat(s2, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s3,concat(s3, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s4,concat(s4, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s5,concat(s5, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s6,concat(s6, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s7,concat(s7, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s8,concat(s8, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s10,concat(s10, 'es') from concatTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function concat only accepts two or more arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void cosTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.cos(1), Math.cos(2), Math.cos(3)};
    Double[] expectedResultLong = new Double[] {Math.cos(1), Math.cos(2), Math.cos(3)};
    Double[] expectedResultFloat = new Double[] {Math.cos(1), Math.cos(2.5), Math.cos(3.5)};
    Double[] expectedResultDouble = new Double[] {Math.cos(1), Math.cos(2.5), Math.cos(3.5)};
    testDoubleResult(
        "select time,cos(s2),cos(s3),cos(s4),cos(s5) from cosTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void cosTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,cos(s2,1) from cosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,cos(s1) from cosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,cos(s6) from cosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,cos(s7) from cosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,cos(s9) from cosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,cos(s10) from cosTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cos only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void coshTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.cosh(1), Math.cosh(2), Math.cosh(3)};
    Double[] expectedResultLong = new Double[] {Math.cosh(1), Math.cosh(2), Math.cosh(3)};
    Double[] expectedResultFloat = new Double[] {Math.cosh(1), Math.cosh(2.5), Math.cosh(3.5)};
    Double[] expectedResultDouble = new Double[] {Math.cosh(1), Math.cosh(2.5), Math.cosh(3.5)};
    testDoubleResult(
        "select time,cosh(s2),cosh(s3),cosh(s4),cosh(s5) from coshTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void coshTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,cosh(s2,1) from coshTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cosh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,cosh(s1) from coshTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cosh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,cosh(s6) from coshTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cosh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,cosh(s7) from coshTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cosh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,cosh(s9) from coshTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cosh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,cosh(s10) from coshTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function cosh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void degreesTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt =
        new Double[] {Math.toDegrees(1), Math.toDegrees(2), Math.toDegrees(3)};
    Double[] expectedResultLong =
        new Double[] {Math.toDegrees(1), Math.toDegrees(2), Math.toDegrees(3)};
    Double[] expectedResultFloat =
        new Double[] {Math.toDegrees(1), Math.toDegrees(2.5), Math.toDegrees(3.5)};
    Double[] expectedResultDouble =
        new Double[] {Math.toDegrees(1), Math.toDegrees(2.5), Math.toDegrees(3.5)};
    testDoubleResult(
        "select time,degrees(s2),degrees(s3),degrees(s4),degrees(s5) from degreesTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void degreesTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,degrees(s2,1) from degreesTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function degrees only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,degrees(s1) from degreesTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function degrees only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,degrees(s6) from degreesTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function degrees only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,degrees(s7) from degreesTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function degrees only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,degrees(s9) from degreesTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function degrees only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,degrees(s10) from degreesTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function degrees only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void endsWithTestNormal() {
    // support the (measurement, ConstantArgument)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,false,ab,false,",
          "1970-01-01T00:00:00.002Z,Test,false,Test,false,",
          "1970-01-01T00:00:00.003Z,efgh,true,efgh,true,",
        };
    tableResultSetEqualTest(
        "select time,s1,ends_with(s1,'gh'),s9,ends_with(s9,'gh') from endsWithTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (measurement, measurement)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,false,",
          "1970-01-01T00:00:00.002Z,Test,Test,true,",
          "1970-01-01T00:00:00.003Z,efgh,efgh,true,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,ends_with(s1,s9) from endsWithTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void endsWithTestFail() {
    // case 1: more than two argument
    tableAssertTestFail(
        "select s1,ends_with(s1, 'es', 'ab') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: less than two argument
    tableAssertTestFail(
        "select s1,ends_with(s1) from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s2,ends_with(s2, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s3,ends_with(s3, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s4,ends_with(s4, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s5,ends_with(s5, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s6,ends_with(s6, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s7,ends_with(s7, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s8,ends_with(s8, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 10: wrong data type
    tableAssertTestFail(
        "select s10,ends_with(s10, 'es') from endsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ends_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void expTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.exp(1), Math.exp(2), Math.exp(3)};
    Double[] expectedResultLong = new Double[] {Math.exp(1), Math.exp(2), Math.exp(3)};
    Double[] expectedResultFloat = new Double[] {Math.exp(1), Math.exp(2.5), Math.exp(3.5)};
    Double[] expectedResultDouble = new Double[] {Math.exp(1), Math.exp(2.5), Math.exp(3.5)};
    testDoubleResult(
        "select time,exp(s2),exp(s3),exp(s4),exp(s5) from expTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void expTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,exp(s2,1) from expTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function exp only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,exp(s1) from expTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function exp only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,exp(s6) from expTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function exp only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,exp(s7) from expTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function exp only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,exp(s9) from expTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function exp only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,exp(s10) from expTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function exp only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void floorTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.floor(1), Math.floor(2), Math.floor(-2)};
    Double[] expectedResultLong = new Double[] {Math.floor(1), Math.floor(2), Math.floor(-2)};
    Double[] expectedResultFloat =
        new Double[] {Math.floor(1.0f), Math.floor(2.5f), Math.floor(-2.5f)};
    Double[] expectedResultDouble =
        new Double[] {Math.floor(1.0), Math.floor(2.5), Math.floor(-2.5)};
    testDoubleResult(
        "select time,floor(s2),floor(s3),floor(s4),floor(s5) from floorTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void floorTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,floor(s2,1) from floorTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function floor only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,floor(s1) from floorTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function floor only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,floor(s6) from floorTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function floor only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,floor(s7) from floorTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function floor only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,floor(s9) from floorTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function floor only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,floor(s10) from floorTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function floor only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void lengthTestNormal() {
    // case 1: support Text data type
    String[] expectedHeader = new String[] {"time", "s1", "_col2"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,4,",
          "1970-01-01T00:00:00.002Z,test,4,",
          "1970-01-01T00:00:00.003Z,abcdefg,7,",
        };
    tableResultSetEqualTest(
        "select time,s1,Length(s1) from lengthTable", expectedHeader, expectedAns, DATABASE_NAME);

    // case 2: support String data type
    expectedHeader = new String[] {"time", "s9", "_col2"};
    expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,4,",
          "1970-01-01T00:00:00.002Z,test,4,",
          "1970-01-01T00:00:00.003Z,abcdefg,7,",
        };
    tableResultSetEqualTest(
        "select time,s9,Length(s9) from lengthTable", expectedHeader, expectedAns, DATABASE_NAME);
  }

  @Test
  public void lengthTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s1,Length(s1,1) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,Length(s2) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s1,Length(s3) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s1,Length(s4) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s1,Length(s5) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s1,Length(s6) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s1,Length(s7) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s1,Length(s8) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s1,Length(s10) from lengthTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function length only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void lnTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.log(1), Math.log(0), Math.log(-3)};
    Double[] expectedResultLong = new Double[] {Math.log(1), Math.log(0), Math.log(-3)};
    Double[] expectedResultFloat = new Double[] {Math.log(1), Math.log(0), Math.log(-3.5)};
    Double[] expectedResultDouble = new Double[] {Math.log(1), Math.log(0), Math.log(-3.5)};
    testDoubleResult(
        "select time,ln(s2),ln(s3),ln(s4),ln(s5) from lnTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void lnTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,ln(s2,1) from lnTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ln only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,ln(s1) from lnTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ln only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,ln(s6) from lnTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ln only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,ln(s7) from lnTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ln only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,ln(s9) from lnTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ln only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,ln(s10) from lnTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function ln only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void log10TestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.log10(1), Math.log10(0), Math.log10(-3)};
    Double[] expectedResultLong = new Double[] {Math.log10(1), Math.log10(0), Math.log10(-3)};
    Double[] expectedResultFloat = new Double[] {Math.log10(1), Math.log10(0), Math.log10(-3.5)};
    Double[] expectedResultDouble = new Double[] {Math.log10(1), Math.log10(0), Math.log10(-3.5)};
    testDoubleResult(
        "select time,log10(s2),log10(s3),log10(s4),log10(s5) from log10Table",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void log10TestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,log10(s2,1) from log10Table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function log10 only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,log10(s1) from log10Table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function log10 only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,log10(s6) from log10Table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function log10 only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,log10(s7) from log10Table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function log10 only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,log10(s9) from log10Table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function log10 only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,log10(s10) from log10Table",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function log10 only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void lowerTestNormal() {
    // Normal
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,ABCD,abcd,ABCD,abcd,",
          "1970-01-01T00:00:00.002Z,Test,test,Test,test,",
          "1970-01-01T00:00:00.003Z,Abcdefg,abcdefg,Abcdefg,abcdefg,",
        };
    tableResultSetEqualTest(
        "select time,s1,lower(s1),s9,lower(s9) from lowerTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lowerTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s1,lower(s1, 1) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s2,lower(s2) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s3,lower(s3) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s4,lower(s4) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s5,lower(s5) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s6,lower(s6) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s7,lower(s7) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s8,lower(s8) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s10,lower(s10) from lowerTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function lower only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void radiansTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt =
        new Double[] {Math.toRadians(1), Math.toRadians(2), Math.toRadians(3)};
    Double[] expectedResultLong =
        new Double[] {Math.toRadians(1), Math.toRadians(2), Math.toRadians(3)};
    Double[] expectedResultFloat =
        new Double[] {Math.toRadians(1), Math.toRadians(2.5), Math.toRadians(3.5)};
    Double[] expectedResultDouble =
        new Double[] {Math.toRadians(1), Math.toRadians(2.5), Math.toRadians(3.5)};
    testDoubleResult(
        "select time,radians(s2),radians(s3),radians(s4),radians(s5) from radiansTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  private void testDoubleResult(
      String sql,
      String[] expectedHeader,
      String database,
      Double[] expectedResultInt,
      Double[] expectedResultLong,
      Double[] expectedResultFloat,
      Double[] expectedResultDouble) {
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
          }
          assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(
                expectedResultInt[cnt], Double.parseDouble(resultSet.getString(2)), 0.00001);
            assertEquals(
                expectedResultLong[cnt], Double.parseDouble(resultSet.getString(3)), 0.00001);
            assertEquals(
                expectedResultFloat[cnt], Double.parseDouble(resultSet.getString(4)), 0.00001);
            assertEquals(
                expectedResultDouble[cnt], Double.parseDouble(resultSet.getString(5)), 0.00001);
            cnt++;
          }
          assertEquals(expectedResultInt.length, cnt);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void radiansTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,radians(s2,1) from radiansTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,radians(s1) from radiansTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,radians(s6) from radiansTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,radians(s7) from radiansTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,radians(s9) from radiansTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,radians(s10) from radiansTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void regexpLikeTestNormal() {
    // support the (measurement, ConstantArgument)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,true,ab,true,",
          "1970-01-01T00:00:00.002Z,Test,false,Test,false,",
          "1970-01-01T00:00:00.003Z,efgh,false,[e-g]+,false,",
        };
    tableResultSetEqualTest(
        "select time,s1,regexp_like(s1,'^abcd$'),s9,regexp_like(s9,'[a-h]+') from regexpLikeTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (measurement, measurement)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,false,",
          "1970-01-01T00:00:00.002Z,Test,Test,true,",
          "1970-01-01T00:00:00.003Z,efgh,[e-g]+,false,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,regexp_like(s1,s9) from regexpLikeTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void regexpLikeTestFail() {
    // case 1: more than two argument
    tableAssertTestFail(
        "select s1,regexp_like(s1, 'es', 'ab') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: less than two argument
    tableAssertTestFail(
        "select s1,regexp_like(s1) from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s2,regexp_like(s2, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s3,regexp_like(s3, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s4,regexp_like(s4, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s5,regexp_like(s5, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s6,regexp_like(s6, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s7,regexp_like(s7, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s8,regexp_like(s8, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 10: wrong data type
    tableAssertTestFail(
        "select s10,regexp_like(s10, 'es') from regexpLikeTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function regexp_like only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void signTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader =
        new String[] {"time", "s2", "_col2", "s3", "_col4", "s4", "_col6", "s5", "_col8"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,1,1,1,1,1.0,1.0,1.0,1.0,",
          "1970-01-01T00:00:00.002Z,0,0,0,0,0.0,0.0,0.0,0.0,",
          "1970-01-01T00:00:00.003Z,-1,-1,-1,-1,-1.0,-1.0,-1.0,-1.0,",
        };
    tableResultSetEqualTest(
        "select time,s2,sign(s2),s3,sign(s3),s4,sign(s4),s5,sign(s5) from signTable",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void signTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,sign(s2,1) from signTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sign only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,sign(s1) from signTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sign only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,sign(s6) from signTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sign only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,sign(s7) from signTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sign only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,sign(s9) from signTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sign only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,sign(s10) from signTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sign only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void sinTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.sin(1), Math.sin(2), Math.sin(3)};
    Double[] expectedResultLong = new Double[] {Math.sin(1), Math.sin(2), Math.sin(3)};
    Double[] expectedResultFloat = new Double[] {Math.sin(1), Math.sin(2.5), Math.sin(3.5)};
    Double[] expectedResultDouble = new Double[] {Math.sin(1), Math.sin(2.5), Math.sin(3.5)};
    testDoubleResult(
        "select time,sin(s2),sin(s3),sin(s4),sin(s5) from sinTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void sinTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,sin(s2,1) from sinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,sin(s1) from sinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,sin(s6) from sinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,sin(s7) from sinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,sin(s9) from sinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,sin(s10) from sinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sin only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void sinhTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.sinh(1), Math.sinh(2), Math.sinh(3)};
    Double[] expectedResultLong = new Double[] {Math.sinh(1), Math.sinh(2), Math.sinh(3)};
    Double[] expectedResultFloat = new Double[] {Math.sinh(1), Math.sinh(2.5), Math.sinh(3.5)};
    Double[] expectedResultDouble = new Double[] {Math.sinh(1), Math.sinh(2.5), Math.sinh(3.5)};
    testDoubleResult(
        "select time,sinh(s2),sinh(s3),sinh(s4),sinh(s5) from sinhTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void sinhTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,sinh(s2,1) from sinhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sinh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,sinh(s1) from sinhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sinh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,sinh(s6) from sinhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sinh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,sinh(s7) from sinhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sinh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,sinh(s9) from sinhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sinh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,sinh(s10) from sinhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sinh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void sqrtTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.sqrt(1), Math.sqrt(0), Math.sqrt(-1)};
    Double[] expectedResultLong = new Double[] {Math.sqrt(1), Math.sqrt(0), Math.sqrt(-1)};
    Double[] expectedResultFloat = new Double[] {Math.sqrt(1), Math.sqrt(0), Math.sqrt(-1.5)};
    Double[] expectedResultDouble = new Double[] {Math.sqrt(1), Math.sqrt(0), Math.sqrt(-1.5)};
    testDoubleResult(
        "select time,sqrt(s2),sqrt(s3),sqrt(s4),sqrt(s5) from sqrtTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void sqrtTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,sqrt(s2,1) from sqrtTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sqrt only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,sqrt(s1) from sqrtTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sqrt only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,sqrt(s6) from sqrtTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sqrt only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,sqrt(s7) from sqrtTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sqrt only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,sqrt(s9) from sqrtTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sqrt only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,sqrt(s10) from sqrtTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function sqrt only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void startsWithTestNormal() {
    // support the (measurement, ConstantArgument)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,false,ab,false,",
          "1970-01-01T00:00:00.002Z,Test,true,Test,true,",
          "1970-01-01T00:00:00.003Z,efgh,false,efgh,false,",
        };
    tableResultSetEqualTest(
        "select time,s1,starts_with(s1,'Te'),s9,starts_with(s9,'Te') from startsWithTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (measurement, measurement)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,true,",
          "1970-01-01T00:00:00.002Z,Test,Test,true,",
          "1970-01-01T00:00:00.003Z,efgh,efgh,true,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,starts_with(s1,s9) from startsWithTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void startsWithTestFail() {
    // case 1: more than two argument
    tableAssertTestFail(
        "select s1,starts_with(s1, 'es', 'ab') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: less than two argument
    tableAssertTestFail(
        "select s1,starts_with(s1) from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s2,starts_with(s2, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s3,starts_with(s3, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s4,starts_with(s4, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s5,starts_with(s5, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s6,starts_with(s6, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s7,starts_with(s7, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s8,starts_with(s8, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 10: wrong data type
    tableAssertTestFail(
        "select s10,starts_with(s10, 'es') from startsWithTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function starts_with only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void strcmpTestNormal() {
    // support the (measurement, ConstantArgument)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,-1,ab,0,",
          "1970-01-01T00:00:00.002Z,Test,-1,Test,-1,",
          "1970-01-01T00:00:00.003Z,efgh,0,efgh,1,",
        };
    tableResultSetEqualTest(
        "select time,s1,strcmp(s1,'efgh'),s9,strcmp(s9,'ab') from strcmpTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (measurement, measurement)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,1,",
          "1970-01-01T00:00:00.002Z,Test,Test,0,",
          "1970-01-01T00:00:00.003Z,efgh,efgh,0,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,strcmp(s1,s9) from strcmpTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void strcmpTestFail() {
    // case 1: more than two argument
    tableAssertTestFail(
        "select s1,strcmp(s1, 'es', 'ab') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: less than two argument
    tableAssertTestFail(
        "select s1,strcmp(s1) from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s2,strcmp(s2, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s3,strcmp(s3, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s4,strcmp(s4, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s5,strcmp(s5, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s6,strcmp(s6, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s7,strcmp(s7, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s8,strcmp(s8, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 10: wrong data type
    tableAssertTestFail(
        "select s10,strcmp(s10, 'es') from strcmpTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strcmp only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void strposTestNormal() {
    // support the (measurement, ConstantArgument)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,0,ab,0,",
          "1970-01-01T00:00:00.002Z,Test,2,Test,2,",
          "1970-01-01T00:00:00.003Z,efgh,0,efgh,0,",
        };
    tableResultSetEqualTest(
        "select time,s1,strpos(s1,'es'),s9,strpos(s9,'es') from strposTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the (measurement, measurement)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,1,",
          "1970-01-01T00:00:00.002Z,Test,Test,1,",
          "1970-01-01T00:00:00.003Z,efgh,efgh,1,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,strpos(s1,s9) from strposTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void strposTestFail() {
    // case 1: more than two argument
    tableAssertTestFail(
        "select s1,strpos(s1, 'es', 'ab') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: less than two argument
    tableAssertTestFail(
        "select s1,strpos(s1) from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s2,strpos(s2, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s3,strpos(s3, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s4,strpos(s4, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s5,strpos(s5, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s6,strpos(s6, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s7,strpos(s7, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s8,strpos(s8, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 10: wrong data type
    tableAssertTestFail(
        "select s10,strpos(s10, 'es') from strposTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function strpos only accepts two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void tanTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.tan(1), Math.tan(2), Math.tan(3)};
    Double[] expectedResultLong = new Double[] {Math.tan(1), Math.tan(2), Math.tan(3)};
    Double[] expectedResultFloat =
        new Double[] {Math.tan(1), Math.tan((float) 1.57079632675), Math.tan(3.5)};
    Double[] expectedResultDouble =
        new Double[] {Math.tan(1), Math.tan(1.57079632675), Math.tan(3.5)};
    testDoubleResult(
        "select time,tan(s2),tan(s3),tan(s4),tan(s5) from tanTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void tanTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,tan(s2,1) from tanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,tan(s1) from tanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,tan(s6) from tanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,tan(s7) from tanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,tan(s9) from tanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,tan(s10) from tanTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void tanhTestNormal() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.tanh(1), Math.tanh(2), Math.tanh(3)};
    Double[] expectedResultLong = new Double[] {Math.tanh(1), Math.tanh(2), Math.tanh(3)};
    Double[] expectedResultFloat = new Double[] {Math.tanh(1), Math.tanh(2.5), Math.tanh(3.5)};
    Double[] expectedResultDouble = new Double[] {Math.tanh(1), Math.tanh(2.5), Math.tanh(3.5)};
    testDoubleResult(
        "select time,tanh(s2),tanh(s3),tanh(s4),tanh(s5) from tanhTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void tanhTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,tanh(s2,1) from tanhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tanh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,tanh(s1) from tanhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tanh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,tanh(s6) from tanhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tanh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,tanh(s7) from tanhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tanh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,tanh(s9) from tanhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tanh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,tanh(s10) from tanhTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tanh only accepts one argument and it must be Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }

  @Test
  public void trimTestNormal() {
    // support the trim(trimSource) trim(specification From trimSource)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcd,ab,ab,",
          "1970-01-01T00:00:00.002Z,xyTestxy,xyTestxy,xy,xy,",
          "1970-01-01T00:00:00.003Z,  Test  ,Test  , T,T,",
        };
    tableResultSetEqualTest(
        "select time,s1,trim(LEADING FROM s1),s9,trim(s9) from trimTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the trim(trimSource, trimChar) trim(trimChar From trimSource)
    expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,bc,ab,b,",
          "1970-01-01T00:00:00.002Z,xyTestxy,yTestxy,xy,y,",
          "1970-01-01T00:00:00.003Z,  Test  ,  Test  , T, T,",
        };
    tableResultSetEqualTest(
        "select time,s1,trim(s1, 'axd'),s9,trim('ax' FROM s9) from trimTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the trim(trimSpecification trimChar From trimSource)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3", "_col4"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,cd,abcd,",
          "1970-01-01T00:00:00.002Z,xyTestxy,xy,Test,xyTest,",
          "1970-01-01T00:00:00.003Z,  Test  , T,est,  Test,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,trim(BOTH s9 FROM s1), trim(TRAILING s9 FROM s1)from trimTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void trimTestFail() {
    // case 1: wrong data type
    tableAssertTestFail(
        "select s2,trim(s2, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s3,trim(s3, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s4,trim(s4, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s5,trim(s5, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s6,trim(s6, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s7,trim(s7, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s8,trim(s8, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s10,trim(s10, 'es') from trimTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }

  @Test
  public void upperTestNormal() {
    // Normal
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ABCD,abcd,ABCD,",
          "1970-01-01T00:00:00.002Z,Test,TEST,Test,TEST,",
          "1970-01-01T00:00:00.003Z,Abcdefg,ABCDEFG,Abcdefg,ABCDEFG,",
        };
    tableResultSetEqualTest(
        "select time,s1,upper(s1),s9,upper(s9) from upperTable",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void upperTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s1,upper(s1, 1) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s2,upper(s2) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s3,upper(s3) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s4,upper(s4) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s5,upper(s5) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s6,upper(s6) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s7,upper(s7) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s8,upper(s8) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);

    // case 9: wrong data type
    tableAssertTestFail(
        "select s10,upper(s10) from upperTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function upper only accepts one argument and it must be text or string data type.",
        DATABASE_NAME);
  }

  private void testOneRowDoubleResult(
      String sql, String[] expectedHeader, String database, Double[] expectedResult) {
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
          }
          assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());
          resultSet.next();
          assertEquals(expectedResult[0], Double.parseDouble(resultSet.getString(1)), 0.00001);
          assertEquals(expectedResult[1], Double.parseDouble(resultSet.getString(2)), 0.00001);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void piTestNormal() {
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.PI};
    Double[] expectedResultLong = new Double[] {Math.PI};
    Double[] expectedResultFloat = new Double[] {Math.PI};
    Double[] expectedResultDouble = new Double[] {Math.PI};
    testDoubleResult(
        "select time, s2 + pi(), s3 + pi(), s4 + pi(), s5 + pi() from NoArgTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void piTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,pi(s2) from NoArgTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": Scalar function pi accepts no argument.",
        DATABASE_NAME);
  }

  @Test
  public void eTestNormal() {
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4"};
    Double[] expectedResultInt = new Double[] {Math.E};
    Double[] expectedResultLong = new Double[] {Math.E};
    Double[] expectedResultFloat = new Double[] {Math.E};
    Double[] expectedResultDouble = new Double[] {Math.E};
    testDoubleResult(
        "select time, s2 + e(), s3 + e(), s4 + e(), s5 + e() from NoArgTable",
        expectedHeader,
        DATABASE_NAME,
        expectedResultInt,
        expectedResultLong,
        expectedResultFloat,
        expectedResultDouble);
  }

  @Test
  public void eTestFail() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s1,e(s1) from NoArgTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": Scalar function e accepts no argument.",
        DATABASE_NAME);
  }

  @Test
  public void dateBinTestNormal() {
    String[] expectedHeader = new String[] {"time", "s1", "s8", "_col3"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,Test,2024-01-01T00:00:00.000Z,2024-01-01T00:00:00.000Z,",
          "1970-01-01T00:00:00.002Z,Test,2024-01-01T01:00:00.000Z,2024-01-01T01:00:00.000Z,",
          "1970-01-01T00:00:00.003Z,Test,2024-01-01T01:59:00.000Z,2024-01-01T01:00:00.000Z,",
          "1970-01-01T00:00:00.004Z,Test,2023-12-31T23:59:00.000Z,2023-12-31T23:00:00.000Z,",
          "1970-01-01T00:00:00.005Z,Test,1969-12-31T23:59:00.000Z,1969-12-31T23:00:00.000Z,",
          "1970-01-01T00:00:00.006Z,Test,null,null,",
        };
    tableResultSetEqualTest(
        "select time,s1,s8,date_bin(1H, s8) from dateBinTable",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "s8", "_col2"};
    expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,2024-01-01T00:00:00.000Z,2023-12-28T00:00:00.000Z,",
          "1970-01-01T00:00:00.002Z,2024-01-01T01:00:00.000Z,2023-12-28T00:00:00.000Z,",
          "1970-01-01T00:00:00.003Z,2024-01-01T01:59:00.000Z,2023-12-28T00:00:00.000Z,",
          "1970-01-01T00:00:00.004Z,2023-12-31T23:59:00.000Z,2023-12-28T00:00:00.000Z,",
          "1970-01-01T00:00:00.005Z,1969-12-31T23:59:00.000Z,1969-12-25T00:00:00.000Z,",
          "1970-01-01T00:00:00.006Z,null,null,",
        };
    tableResultSetEqualTest(
        "select time,s8,date_bin(1W, s8) from dateBinTable",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void dateBinTestFail() {
    tableAssertTestFail(
        "select time,s1,s8,date_bin(1H,s8,0,0) from dateBinTable",
        TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
            + ": line 1:35: mismatched input ','. Expecting: ')'",
        DATABASE_NAME);

    tableAssertTestFail(
        "select time,s1,s8,date_bin(1H,s1) from dateBinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function date_bin only accepts two or three arguments and the second and third must be TimeStamp data type.",
        DATABASE_NAME);

    tableAssertTestFail(
        "select time,s1,s8,date_bin(1MONTH 1DAY,s8) from dateBinTable",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Simultaneous setting of monthly and non-monthly intervals is not supported.",
        DATABASE_NAME);
  }
}
