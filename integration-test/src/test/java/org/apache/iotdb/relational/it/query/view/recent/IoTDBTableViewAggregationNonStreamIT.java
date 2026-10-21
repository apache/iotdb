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
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.relational.it.db.it.IoTDBMultiTAGsWithAttributesTableIT.buildHeaders;
import static org.apache.iotdb.relational.it.db.it.IoTDBMultiTAGsWithAttributesTableIT.repeatTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableViewAggregationNonStreamIT {
  protected static final String TREE_DB_NAME = "root.test";
  protected static final String DATABASE_NAME = "test";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + TREE_DB_NAME,
        "CREATE ALIGNED TIMESERIES root.test.table1.d01(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d02(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d03(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d04(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d05(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d06(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d07(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d08(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d09(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d10(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d11(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d12(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d13(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d14(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d15(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d16(province STRING, city STRING, region1 STRING, s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "INSERT INTO root.test.table1.d01(time,province,city,region1,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','huangpu',30,30.0,'shanghai_huangpu_red_A_d01_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO root.test.table1.d01(time,province,city,region1,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'shanghai','shanghai','huangpu',35000,35.0,35.0,'shanghai_huangpu_red_A_d01_35','shanghai_huangpu_red_A_d01_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d01(time,province,city,region1,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu',40,40.0,true,'shanghai_huangpu_red_A_d01_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d01(time,province,city,region1,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','huangpu',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d01(time,province,city,region1,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','huangpu',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d02(time,province,city,region1,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','huangpu',36,true,'shanghai_huangpu_red_B_d02_36','shanghai_huangpu_red_B_d02_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO root.test.table1.d02(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu',40,40.0,'shanghai_huangpu_red_B_d02_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d02(time,province,city,region1,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','huangpu',50000,'shanghai_huangpu_red_B_d02_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO root.test.table1.d03(time,province,city,region1,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'shanghai','shanghai','huangpu',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO root.test.table1.d03(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','huangpu',36,36.0,'shanghai_huangpu_yellow_A_d03_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d03(time,province,city,region1,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'shanghai','shanghai','huangpu',41,41.0,false,'shanghai_huangpu_yellow_A_d03_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO root.test.table1.d03(time,province,city,region1,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'shanghai','shanghai','huangpu',46000,46.0,'shanghai_huangpu_yellow_A_d03_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO root.test.table1.d03(time,province,city,region1,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'shanghai','shanghai','huangpu',51.0,'shanghai_huangpu_yellow_A_d03_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO root.test.table1.d04(time,province,city,region1,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','huangpu',30.0,true,'shanghai_huangpu_yellow_B_d04_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d04(time,province,city,region1,s2,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d04(time,province,city,region1,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','huangpu',55,55.0,'shanghai_huangpu_yellow_B_d04_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d05(time,province,city,region1,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','pudong',30,30.0,'shanghai_pudong_red_A_d05_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO root.test.table1.d05(time,province,city,region1,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'shanghai','shanghai','pudong',35000,35.0,35.0,'shanghai_pudong_red_A_d05_35','shanghai_pudong_red_A_d05_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d05(time,province,city,region1,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong',40,40.0,true,'shanghai_pudong_red_A_d05_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d05(time,province,city,region1,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','pudong',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d05(time,province,city,region1,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','pudong',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d06(time,province,city,region1,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','pudong',36,true,'shanghai_pudong_red_B_d06_36','shanghai_pudong_red_B_d06_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO root.test.table1.d06(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong',40,40.0,'shanghai_pudong_red_B_d06_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d06(time,province,city,region1,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','pudong',50000,'shanghai_pudong_red_B_d06_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO root.test.table1.d07(time,province,city,region1,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'shanghai','shanghai','pudong',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO root.test.table1.d07(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','pudong',36,36.0,'shanghai_pudong_yellow_A_d07_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d07(time,province,city,region1,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'shanghai','shanghai','pudong',41,41.0,false,'shanghai_pudong_yellow_A_d07_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO root.test.table1.d07(time,province,city,region1,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'shanghai','shanghai','pudong',46000,46.0,'shanghai_pudong_yellow_A_d07_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO root.test.table1.d07(time,province,city,region1,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'shanghai','shanghai','pudong',51.0,'shanghai_pudong_yellow_A_d07_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO root.test.table1.d08(time,province,city,region1,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','pudong',30.0,true,'shanghai_pudong_yellow_B_d08_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d08(time,province,city,region1,s2,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d08(time,province,city,region1,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','pudong',55,55.0,'shanghai_pudong_yellow_B_d08_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d09(time,province,city,region1,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','chaoyang',30,30.0,'beijing_chaoyang_red_A_d09_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO root.test.table1.d09(time,province,city,region1,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'beijing','beijing','chaoyang',35000,35.0,35.0,'beijing_chaoyang_red_A_d09_35','beijing_chaoyang_red_A_d09_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d09(time,province,city,region1,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang',40,40.0,true,'beijing_chaoyang_red_A_d09_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d09(time,province,city,region1,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','chaoyang',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d09(time,province,city,region1,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','chaoyang',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d10(time,province,city,region1,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','chaoyang',36,true,'beijing_chaoyang_red_B_d10_36','beijing_chaoyang_red_B_d10_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO root.test.table1.d10(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang',40,40.0,'beijing_chaoyang_red_B_d10_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d10(time,province,city,region1,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','chaoyang',50000,'beijing_chaoyang_red_B_d10_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO root.test.table1.d11(time,province,city,region1,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'beijing','beijing','chaoyang',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO root.test.table1.d11(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','chaoyang',36,36.0,'beijing_chaoyang_yellow_A_d11_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d11(time,province,city,region1,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'beijing','beijing','chaoyang',41,41.0,false,'beijing_chaoyang_yellow_A_d11_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO root.test.table1.d11(time,province,city,region1,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'beijing','beijing','chaoyang',46000,46.0,'beijing_chaoyang_yellow_A_d11_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO root.test.table1.d11(time,province,city,region1,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'beijing','beijing','chaoyang',51.0,'beijing_chaoyang_yellow_A_d11_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO root.test.table1.d12(time,province,city,region1,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','chaoyang',30.0,true,'beijing_chaoyang_yellow_B_d12_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d12(time,province,city,region1,s2,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d12(time,province,city,region1,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','chaoyang',55,55.0,'beijing_chaoyang_yellow_B_d12_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d13(time,province,city,region1,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','haidian',30,30.0,'beijing_haidian_red_A_d13_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO root.test.table1.d13(time,province,city,region1,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'beijing','beijing','haidian',35000,35.0,35.0,'beijing_haidian_red_A_d13_35','beijing_haidian_red_A_d13_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d13(time,province,city,region1,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian',40,40.0,true,'beijing_haidian_red_A_d13_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d13(time,province,city,region1,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','haidian',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d13(time,province,city,region1,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','haidian',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO root.test.table1.d14(time,province,city,region1,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','haidian',36,true,'beijing_haidian_red_B_d14_36','beijing_haidian_red_B_d14_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO root.test.table1.d14(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian',40,40.0,'beijing_haidian_red_B_d14_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d14(time,province,city,region1,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','haidian',50000,'beijing_haidian_red_B_d14_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO root.test.table1.d15(time,province,city,region1,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'beijing','beijing','haidian',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO root.test.table1.d15(time,province,city,region1,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','haidian',36,36.0,'beijing_haidian_yellow_A_d15_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d15(time,province,city,region1,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'beijing','beijing','haidian',41,41.0,false,'beijing_haidian_yellow_A_d15_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO root.test.table1.d15(time,province,city,region1,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'beijing','beijing','haidian',46000,46.0,'beijing_haidian_yellow_A_d15_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO root.test.table1.d15(time,province,city,region1,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'beijing','beijing','haidian',51.0,'beijing_haidian_yellow_A_d15_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO root.test.table1.d16(time,province,city,region1,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','haidian',30.0,true,'beijing_haidian_yellow_B_d16_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d16(time,province,city,region1,s2,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d16(time,province,city,region1,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','haidian',55,55.0,'beijing_haidian_yellow_B_d16_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "FLUSH",
      };

  protected static final String[] createTableViewSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE VIEW table1(province STRING FIELD, city STRING FIELD, region STRING FIELD FROM region1, device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD, s3 FLOAT FIELD, s4 DOUBLE FIELD, s5 BOOLEAN FIELD, s6 TEXT FIELD, s7 STRING FIELD, s8 BLOB FIELD, s9 TIMESTAMP FIELD, s10 DATE FIELD) as root.test.table1.**",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createSqls);
    prepareTableData(createTableViewSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ==================================================================
  // ==================== Normal Aggregation Test =====================
  // ==================================================================
  @Test
  public void countTest() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray =
        new String[] {
          "5,",
        };
    tableResultSetEqualTest(
        "select count(*) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select count('a') from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "end_time", "device_id", "_col3"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d01,1,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d01,1,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d01,1,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d01,1,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d01,1,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), (date_bin(5s, time) + 5000) as end_time, device_id, count(*) from table1 where device_id = 'd01' group by 1,device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin(5s, time), (date_bin(5s, time) + 5000) as end_time, device_id, count(1) from table1 where device_id = 'd01' group by 1,device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "province", "city", "region", "device_id", "_col5"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,1,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,1,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,1,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,1,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,1,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,1,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,1,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,1,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,1,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,1,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,1,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,1,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,1,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,1,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,1,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,1,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,1,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,1,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,1,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,1,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,1,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,1,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,1,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,1,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,1,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,1,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,1,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,1,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id, count(*) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id, count(1) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "_col0",
          "province",
          "city",
          "region",
          "device_id",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14"
        };
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,1,0,1,0,0,1,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,0,1,1,1,0,1,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,1,0,1,0,1,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,0,1,0,0,1,0,0,0,1,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,1,0,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,1,0,0,0,1,1,1,0,1,0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,0,1,0,0,0,0,1,1,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,0,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,1,0,1,0,1,1,0,1,1,0,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,0,1,0,1,0,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,0,0,1,0,0,1,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,0,0,1,0,1,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,0,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,1,0,0,1,0,1,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,1,0,1,0,0,1,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,0,1,1,1,0,1,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,1,0,1,0,1,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,0,1,0,0,1,0,0,0,1,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,1,0,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,1,0,0,0,1,1,1,0,1,0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,0,1,0,0,0,0,1,1,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,0,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,1,0,1,0,1,1,0,1,1,0,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,0,1,0,1,0,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,0,0,1,0,0,1,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,0,0,1,0,1,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,0,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,1,0,0,1,0,1,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,1,0,1,0,0,1,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,0,1,1,1,0,1,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,1,0,1,0,1,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,0,1,0,0,1,0,0,0,1,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,1,0,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,1,0,0,0,1,1,1,0,1,0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,0,1,0,0,0,0,1,1,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,0,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,1,0,1,0,1,1,0,1,1,0,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,0,1,0,1,0,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,0,0,1,0,0,1,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,0,0,1,0,1,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,0,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,1,0,0,1,0,1,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,1,0,1,0,0,1,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,0,1,1,1,0,1,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,1,0,1,0,1,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,0,1,0,0,1,0,0,0,1,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,1,0,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,1,0,0,0,1,1,1,0,1,0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,0,1,0,0,0,0,1,1,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,0,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,1,0,0,1,0,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,1,0,1,0,1,1,0,1,1,0,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,0,1,0,1,0,0,1,0,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,0,0,1,0,0,1,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,0,0,1,0,1,0,1,0,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,0,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,1,0,0,1,0,1,0,1,1,0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id, count(s1), count(s2), count(s3), count(s4), count(s5), count(s6), count(s7), count(s8), count(s9), count(s10) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,3,2,3,2,2,2,2,2,5,2,",
          "beijing,beijing,chaoyang,d10,2,1,0,1,1,1,3,1,3,1,",
          "beijing,beijing,chaoyang,d11,2,2,2,2,1,2,2,2,5,1,",
          "beijing,beijing,chaoyang,d12,1,1,1,1,1,1,1,1,3,1,",
          "beijing,beijing,haidian,d13,3,2,3,2,2,2,2,2,5,2,",
          "beijing,beijing,haidian,d14,2,1,0,1,1,1,3,1,3,1,",
          "beijing,beijing,haidian,d15,2,2,2,2,1,2,2,2,5,1,",
          "beijing,beijing,haidian,d16,1,1,1,1,1,1,1,1,3,1,",
          "shanghai,shanghai,huangpu,d01,3,2,3,2,2,2,2,2,5,2,",
          "shanghai,shanghai,huangpu,d02,2,1,0,1,1,1,3,1,3,1,",
          "shanghai,shanghai,huangpu,d03,2,2,2,2,1,2,2,2,5,1,",
          "shanghai,shanghai,huangpu,d04,1,1,1,1,1,1,1,1,3,1,",
          "shanghai,shanghai,pudong,d05,3,2,3,2,2,2,2,2,5,2,",
          "shanghai,shanghai,pudong,d06,2,1,0,1,1,1,3,1,3,1,",
          "shanghai,shanghai,pudong,d07,2,2,2,2,1,2,2,2,5,1,",
          "shanghai,shanghai,pudong,d08,1,1,1,1,1,1,1,1,3,1,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id, count(s1), count(s2), count(s3), count(s4), count(s5), count(s6), count(s7), count(s8), count(s9), count(s10) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,5,",
          "beijing,beijing,chaoyang,d10,3,",
          "beijing,beijing,chaoyang,d11,5,",
          "beijing,beijing,chaoyang,d12,3,",
          "beijing,beijing,haidian,d13,5,",
          "beijing,beijing,haidian,d14,3,",
          "beijing,beijing,haidian,d15,5,",
          "beijing,beijing,haidian,d16,3,",
          "shanghai,shanghai,huangpu,d01,5,",
          "shanghai,shanghai,huangpu,d02,3,",
          "shanghai,shanghai,huangpu,d03,5,",
          "shanghai,shanghai,huangpu,d04,3,",
          "shanghai,shanghai,pudong,d05,5,",
          "shanghai,shanghai,pudong,d06,3,",
          "shanghai,shanghai,pudong,d07,5,",
          "shanghai,shanghai,pudong,d08,3,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,count(*) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select province,city,region,device_id,count(1) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,16,",
          "beijing,beijing,haidian,16,",
          "shanghai,shanghai,huangpu,16,",
          "shanghai,shanghai,pudong,16,",
        };
    tableResultSetEqualTest(
        "select province,city,region,count(*) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select province,city,region,count(1) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2"};
    retArray =
        new String[] {
          "beijing,beijing,32,", "shanghai,shanghai,32,",
        };
    tableResultSetEqualTest(
        "select province,city,count(*) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select province,city,count(1) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,32,", "shanghai,32,",
        };
    tableResultSetEqualTest(
        "select province,count(*) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select province,count(1) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray =
        new String[] {
          "64,",
        };
    tableResultSetEqualTest("select count(*) from table1", expectedHeader, retArray, DATABASE_NAME);
    tableResultSetEqualTest("select count(1) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void countIfTest() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray =
        new String[] {
          "5,",
        };
    tableResultSetEqualTest(
        "select count_if(device_id = 'd01') from table1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray =
        new String[] {
          "64,",
        };
    tableResultSetEqualTest(
        "select count_if(true) from table1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "end_time", "device_id", "_col3"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d01,1,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d01,1,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d01,1,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d01,1,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d01,1,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d02,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d02,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d02,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d03,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d03,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d03,0,",
          "2024-09-24T06:15:45.000Z,2024-09-24T06:15:50.000Z,d03,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d03,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d04,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d04,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d04,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d05,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d05,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d05,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d05,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d05,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d06,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d06,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d06,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d07,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d07,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d07,0,",
          "2024-09-24T06:15:45.000Z,2024-09-24T06:15:50.000Z,d07,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d07,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d08,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d08,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d08,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d09,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d09,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d09,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d09,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d09,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d10,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d10,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d10,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d11,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d11,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d11,0,",
          "2024-09-24T06:15:45.000Z,2024-09-24T06:15:50.000Z,d11,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d11,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d12,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d12,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d12,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d13,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d13,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d13,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d13,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d13,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d14,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d14,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d14,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d15,0,",
          "2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,d15,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d15,0,",
          "2024-09-24T06:15:45.000Z,2024-09-24T06:15:50.000Z,d15,0,",
          "2024-09-24T06:15:50.000Z,2024-09-24T06:15:55.000Z,d15,0,",
          "2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,d16,0,",
          "2024-09-24T06:15:40.000Z,2024-09-24T06:15:45.000Z,d16,0,",
          "2024-09-24T06:15:55.000Z,2024-09-24T06:16:00.000Z,d16,0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), (date_bin(5s, time) + 5000) as end_time, device_id, count_if(device_id = 'd01') from table1 group by 1,device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "_col0",
          "province",
          "city",
          "region",
          "device_id",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14"
        };
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,1,1,1,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,0,0,1,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,1,0,0,0,0,0,0,1,1,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,0,0,0,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,1,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,1,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,0,0,1,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,1,1,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,1,0,1,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,1,0,0,0,1,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,1,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,1,1,1,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,0,0,1,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,1,0,0,0,0,0,0,1,1,1,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,0,0,0,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,1,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,1,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,0,0,1,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,1,1,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,1,0,1,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,1,0,0,0,1,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,1,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,1,1,1,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,0,0,1,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,1,0,0,0,0,0,0,1,1,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,0,0,0,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,1,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,1,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,0,0,1,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,1,1,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,1,0,1,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,1,0,0,0,1,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,1,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,0,0,0,0,0,1,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,1,1,1,1,0,1,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,0,0,1,0,1,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,1,0,0,0,0,0,0,1,1,1,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,0,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,0,0,0,0,1,1,1,1,1,0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,1,0,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,1,1,0,0,0,0,0,0,1,0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,0,0,0,1,0,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,0,0,1,0,0,1,0,0,1,0,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,1,1,0,1,0,0,0,1,1,0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,1,0,1,0,0,1,0,1,1,0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,1,0,0,0,1,0,0,1,1,1,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,1,1,0,0,0,0,0,1,1,0,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,0,0,0,0,0,1,0,0,1,0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id, count_if(s1 is null), count_if(s2 < 50000), count_if(s3 > 30), count_if(s4 < 55), count_if(s5), count_if(s6 like '%pudong%'), count_if(s7 = 'shanghai_pudong_red_B_d06_36'), count_if(s8 is null), count(s9 is null), count_if(s10 is not null) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader, retArray, DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2,1,2,1,1,0,0,3,5,2,",
          "beijing,beijing,chaoyang,d10,1,0,0,1,1,0,0,2,3,1,",
          "beijing,beijing,chaoyang,d11,3,2,2,2,0,0,0,3,5,1,",
          "beijing,beijing,chaoyang,d12,2,1,0,0,1,0,0,2,3,1,",
          "beijing,beijing,haidian,d13,2,1,2,1,1,0,0,3,5,2,",
          "beijing,beijing,haidian,d14,1,0,0,1,1,0,0,2,3,1,",
          "beijing,beijing,haidian,d15,3,2,2,2,0,0,0,3,5,1,",
          "beijing,beijing,haidian,d16,2,1,0,0,1,0,0,2,3,1,",
          "shanghai,shanghai,huangpu,d01,2,1,2,1,1,0,0,3,5,2,",
          "shanghai,shanghai,huangpu,d02,1,0,0,1,1,0,0,2,3,1,",
          "shanghai,shanghai,huangpu,d03,3,2,2,2,0,0,0,3,5,1,",
          "shanghai,shanghai,huangpu,d04,2,1,0,0,1,0,0,2,3,1,",
          "shanghai,shanghai,pudong,d05,2,1,2,1,1,2,0,3,5,2,",
          "shanghai,shanghai,pudong,d06,1,0,0,1,1,1,1,2,3,1,",
          "shanghai,shanghai,pudong,d07,3,2,2,2,0,2,0,3,5,1,",
          "shanghai,shanghai,pudong,d08,2,1,0,0,1,1,0,2,3,1,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id, count_if(s1 is null), count_if(s2 < 50000), count_if(s3 > 30), count_if(s4 < 55), count_if(s5), count_if(s6 like '%pudong%'), count_if(s7 = 'shanghai_pudong_red_B_d06_36'), count_if(s8 is null), count(s9 is null), count_if(s10 is not null) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,5,",
          "beijing,beijing,haidian,5,",
          "shanghai,shanghai,huangpu,5,",
          "shanghai,shanghai,pudong,5,",
        };
    tableResultSetEqualTest(
        "select province,city,region,count(s3 > 30 and s4 < 55) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,6,", "shanghai,6,",
        };
    tableResultSetEqualTest(
        "select province,count_if(s5) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "select count_if(device_id) from table1",
        "701: Aggregate functions [count_if] should only have one boolean expression as argument",
        DATABASE_NAME);

    tableAssertTestFail(
        "select count_if(s5, device_id != 'd01') from table1",
        "701: Aggregate functions [count_if] should only have one boolean expression as argument",
        DATABASE_NAME);
  }

  @Test
  public void avgTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "d01,45.0,",
        };
    tableResultSetEqualTest(
        "select device_id, avg(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, avg(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "device_id", "_col2"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,30.0,",
          "2024-09-24T06:15:35.000Z,d01,35.0,",
          "2024-09-24T06:15:40.000Z,d01,40.0,",
          "2024-09-24T06:15:50.000Z,d01,null,",
          "2024-09-24T06:15:55.000Z,d01,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, avg(s3) from table1 where device_id = 'd01' group by 1, 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "province", "city", "region", "device_id", "_col5"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,55.0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,55.0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id, avg(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,42500.0,",
          "beijing,beijing,chaoyang,d10,50000.0,",
          "beijing,beijing,chaoyang,d11,38500.0,",
          "beijing,beijing,chaoyang,d12,40000.0,",
          "beijing,beijing,haidian,d13,42500.0,",
          "beijing,beijing,haidian,d14,50000.0,",
          "beijing,beijing,haidian,d15,38500.0,",
          "beijing,beijing,haidian,d16,40000.0,",
          "shanghai,shanghai,huangpu,d01,42500.0,",
          "shanghai,shanghai,huangpu,d02,50000.0,",
          "shanghai,shanghai,huangpu,d03,38500.0,",
          "shanghai,shanghai,huangpu,d04,40000.0,",
          "shanghai,shanghai,pudong,d05,42500.0,",
          "shanghai,shanghai,pudong,d06,50000.0,",
          "shanghai,shanghai,pudong,d07,38500.0,",
          "shanghai,shanghai,pudong,d08,40000.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id, avg(s2) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,44.5,",
          "beijing,beijing,haidian,44.5,",
          "shanghai,shanghai,huangpu,44.5,",
          "shanghai,shanghai,pudong,44.5,",
        };
    tableResultSetEqualTest(
        "select province,city,region,avg(s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2"};
    retArray =
        new String[] {
          "beijing,beijing,44.5,", "shanghai,shanghai,44.5,",
        };
    tableResultSetEqualTest(
        "select province,city,avg(s4) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,44.5,", "shanghai,44.5,",
        };
    tableResultSetEqualTest(
        "select province,avg(s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"44.5,"};
    tableResultSetEqualTest("select avg(s4) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void sumTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1"};
    String[] retArray =
        new String[] {
          "d01,90.0,",
        };
    tableResultSetEqualTest(
        "select device_id, sum(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1"};
    retArray =
        new String[] {
          "d01,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, sum(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "device_id", "_col2"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,30.0,",
          "2024-09-24T06:15:35.000Z,d01,35.0,",
          "2024-09-24T06:15:40.000Z,d01,40.0,",
          "2024-09-24T06:15:50.000Z,d01,null,",
          "2024-09-24T06:15:55.000Z,d01,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, sum(s3) from table1 where device_id = 'd01' group by 1, 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "province", "city", "region", "device_id", "_col5"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,55.0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,55.0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id, sum(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,85000.0,",
          "beijing,beijing,chaoyang,d10,50000.0,",
          "beijing,beijing,chaoyang,d11,77000.0,",
          "beijing,beijing,chaoyang,d12,40000.0,",
          "beijing,beijing,haidian,d13,85000.0,",
          "beijing,beijing,haidian,d14,50000.0,",
          "beijing,beijing,haidian,d15,77000.0,",
          "beijing,beijing,haidian,d16,40000.0,",
          "shanghai,shanghai,huangpu,d01,85000.0,",
          "shanghai,shanghai,huangpu,d02,50000.0,",
          "shanghai,shanghai,huangpu,d03,77000.0,",
          "shanghai,shanghai,huangpu,d04,40000.0,",
          "shanghai,shanghai,pudong,d05,85000.0,",
          "shanghai,shanghai,pudong,d06,50000.0,",
          "shanghai,shanghai,pudong,d07,77000.0,",
          "shanghai,shanghai,pudong,d08,40000.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id, sum(s2) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,267.0,",
          "beijing,beijing,haidian,267.0,",
          "shanghai,shanghai,huangpu,267.0,",
          "shanghai,shanghai,pudong,267.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,sum(s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2"};
    retArray =
        new String[] {
          "beijing,beijing,534.0,", "shanghai,shanghai,534.0,",
        };
    tableResultSetEqualTest(
        "select province,city,sum(s4) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,534.0,", "shanghai,534.0,",
        };
    tableResultSetEqualTest(
        "select province,sum(s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"908.0,"};
    tableResultSetEqualTest("select sum(s3) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void minTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3", "_col4"};
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:30.000Z,35.0,2024-09-24T06:15:30.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select device_id, min(time),min(s4), min(s9), min(s10) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:40.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, min(time),min(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "device_id", "_col2", "_col3"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,2024-09-24T06:15:30.000Z,30.0,",
          "2024-09-24T06:15:35.000Z,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,d01,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,d01,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,d01,2024-09-24T06:15:55.000Z,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, min(time),min(s3) from table1 where device_id = 'd01' group by 1, 2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"_col0", "province", "city", "region", "device_id", "_col5", "_col6"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id,min(time),min(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"_col0", "province", "city", "region", "device_id", "_col5", "_col6"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,30,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:41.000Z,41,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:46.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,30,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:41.000Z,41,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:46.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,30,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:41.000Z,41,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:46.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,30,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,55,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:36.000Z,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:41.000Z,41,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:46.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,55,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id,min(time),min(s1) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,35000,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,50000,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,31000,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,40000,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,35000,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,50000,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,31000,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,40000,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,35000,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,50000,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,31000,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,40000,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,35000,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,50000,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,31000,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,40000,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,min(time),min(s2) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,2024-09-24T06:15:30.000Z,35.0,",
          "beijing,beijing,haidian,2024-09-24T06:15:30.000Z,35.0,",
          "shanghai,shanghai,huangpu,2024-09-24T06:15:30.000Z,35.0,",
          "shanghai,shanghai,pudong,2024-09-24T06:15:30.000Z,35.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,min(time),min(s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,2024-09-24T06:15:30.000Z,35.0,",
          "shanghai,shanghai,2024-09-24T06:15:30.000Z,35.0,",
        };
    tableResultSetEqualTest(
        "select province,city,min(time),min(s4) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1", "_col2"};
    retArray =
        new String[] {
          "beijing,2024-09-24T06:15:30.000Z,35.0,", "shanghai,2024-09-24T06:15:30.000Z,35.0,",
        };
    tableResultSetEqualTest(
        "select province,min(time),min(s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "_col1"};
    retArray = new String[] {"2024-09-24T06:15:30.000Z,30.0,"};
    tableResultSetEqualTest(
        "select min(time),min(s3) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void minByTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:35.000Z,35.0,",
        };
    tableResultSetEqualTest(
        "select device_id, min_by(time, s4), min(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, min_by(time, s4), min(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:30.000Z,30.0,",
          "d01,2024-09-24T06:15:35.000Z,35.0,",
          "d01,2024-09-24T06:15:40.000Z,40.0,",
          "d01,null,null,",
          "d01,null,null,",
        };

    tableResultSetEqualTest(
        "select device_id, min_by(time, s3), min(s3) from table1 where device_id = 'd01' group by date_bin(5s, time), 1 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "device_id", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id, date_bin(5s, time),min_by(time, s4), min(s4) from table1 group by 1,2,3,4,date_bin(5s, time) order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "device_id", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id, date_bin(5s, time),min_by(time, s1), min(s1) from table1 group by date_bin(5s, time),1,2,3,4 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,35000,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,50000,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,31000,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,40000,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,35000,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,50000,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,31000,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,40000,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,35000,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,50000,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,31000,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,40000,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,35000,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,50000,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,31000,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,40000,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id,min_by(time, s2), min(s2) from table1 group by 1,2,3,4  order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"40,"};
    tableResultSetEqualTest(
        "select min_by(s1, s10) from table1 where s1=40", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void maxTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3", "_col4"};
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55.0,2024-09-24T06:15:55.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select device_id,max(time),max(s4),max(s9),max(s10) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id,max(time),max(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "device_id", "_col2", "_col3"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,2024-09-24T06:15:30.000Z,30.0,",
          "2024-09-24T06:15:35.000Z,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,d01,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,d01,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,d01,2024-09-24T06:15:55.000Z,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, max(time),max(s3) from table1 where device_id = 'd01' group by 1, 2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"_col0", "province", "city", "region", "device_id", "_col5", "_col6"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id,max(time),max(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"_col0", "province", "city", "region", "device_id", "_col5", "_col6"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:36.000Z,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:41.000Z,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:46.000Z,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id,max(time),max(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,40.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,46.0,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,40.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,46.0,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,40.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,46.0,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,40.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,46.0,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,max(time),max(s4) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,max(time),max(s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select province,city,max(time),max(s4) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1", "_col2"};
    retArray =
        new String[] {
          "beijing,2024-09-24T06:15:55.000Z,55.0,", "shanghai,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select province,max(time),max(s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "_col1"};
    retArray = new String[] {"2024-09-24T06:15:55.000Z,51.0,"};
    tableResultSetEqualTest(
        "select max(time),max(s3) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void maxByTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, max_by(time, s4), max(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2"};
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, max_by(time, s4), max(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3"};
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "d01,2024-09-24T06:15:50.000Z,null,null,",
          "d01,2024-09-24T06:15:55.000Z,null,null,",
        };

    tableResultSetEqualTest(
        "select device_id, date_bin(5s, time), max_by(time, s3), max(s3) from table1 where device_id = 'd01' group by date_bin(5s, time), 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "device_id", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,46.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55.0,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id, date_bin(5s, time),max_by(time, s4), max(s4) from table1 group by 1,2,3,4,date_bin(5s, time) order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "device_id", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id, date_bin(5s, time),max_by(time, s1), max(s1) from table1 group by date_bin(5s, time),1,2,3,4 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,50000,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,50000,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:46.000Z,46000,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,40000,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,50000,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,50000,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:46.000Z,46000,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,40000,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,50000,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,50000,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:46.000Z,46000,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,40000,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,50000,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,50000,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:46.000Z,46000,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,40000,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id,max_by(time, s2), max(s2) from table1 group by 1,2,3,4  order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"40,"};
    tableResultSetEqualTest(
        "select max_by(s1, s10) from table1 where s1=40", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void firstTest() {
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,30,35000,30.0,35.0,true,shanghai_huangpu_red_A_d01_30,shanghai_huangpu_red_A_d01_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select first(time),first(s1),first(s2),first(s3),first(s4),first(s5),first(s6),first(s7),first(s8),first(s9),first(s10) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "_col0",
          "device_id",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_huangpu_red_A_d01_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,d01,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "2024-09-24T06:15:40.000Z,d01,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_huangpu_red_A_d01_40,null,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,d01,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "2024-09-24T06:15:55.000Z,d01,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, first(time),first(s1),first(s2),first(s3),first(s4),first(s5),first(s6),first(s7),first(s8),first(s9),first(s10) from table1 where device_id = 'd01' group by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14",
          "_col15"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,beijing_chaoyang_red_A_d09_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,beijing_chaoyang_red_A_d09_35,beijing_chaoyang_red_A_d09_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,beijing_chaoyang_red_A_d09_40,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,beijing_chaoyang_red_B_d10_36,beijing_chaoyang_red_B_d10_36,null,2024-09-24T06:15:36.000Z,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,beijing_chaoyang_red_B_d10_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,beijing_chaoyang_red_B_d10_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,beijing_chaoyang_yellow_A_d11_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,beijing_chaoyang_yellow_A_d11_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,beijing_chaoyang_yellow_A_d11_46,null,2024-09-24T06:15:46.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,beijing_chaoyang_yellow_A_d11_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,beijing_chaoyang_yellow_B_d12_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_chaoyang_yellow_B_d12_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,beijing_haidian_red_A_d13_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,beijing_haidian_red_A_d13_35,beijing_haidian_red_A_d13_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,beijing_haidian_red_A_d13_40,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,beijing_haidian_red_B_d14_36,beijing_haidian_red_B_d14_36,null,2024-09-24T06:15:36.000Z,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,beijing_haidian_red_B_d14_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,beijing_haidian_red_B_d14_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,beijing_haidian_yellow_A_d15_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,beijing_haidian_yellow_A_d15_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,beijing_haidian_yellow_A_d15_46,null,2024-09-24T06:15:46.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,beijing_haidian_yellow_A_d15_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,beijing_haidian_yellow_B_d16_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_haidian_yellow_B_d16_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_huangpu_red_A_d01_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_huangpu_red_A_d01_40,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,shanghai_huangpu_red_B_d02_36,shanghai_huangpu_red_B_d02_36,null,2024-09-24T06:15:36.000Z,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,shanghai_huangpu_red_B_d02_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,shanghai_huangpu_red_B_d02_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,shanghai_huangpu_yellow_A_d03_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,shanghai_huangpu_yellow_A_d03_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,shanghai_huangpu_yellow_A_d03_46,null,2024-09-24T06:15:46.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,shanghai_huangpu_yellow_A_d03_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,shanghai_huangpu_yellow_B_d04_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_pudong_red_A_d05_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_pudong_red_A_d05_35,shanghai_pudong_red_A_d05_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_pudong_red_A_d05_40,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,shanghai_pudong_red_B_d06_36,shanghai_pudong_red_B_d06_36,null,2024-09-24T06:15:36.000Z,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,shanghai_pudong_red_B_d06_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,shanghai_pudong_red_B_d06_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,shanghai_pudong_yellow_A_d07_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,shanghai_pudong_yellow_A_d07_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,shanghai_pudong_yellow_A_d07_46,null,2024-09-24T06:15:46.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,shanghai_pudong_yellow_A_d07_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,shanghai_pudong_yellow_B_d08_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_pudong_yellow_B_d08_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,date_bin(5s, time), first(time),first(s1),first(s2),first(s3),first(s4),first(s5),first(s6),first(s7),first(s8),first(s9),first(s10) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,30,35000,30.0,35.0,true,beijing_chaoyang_red_A_d09_30,beijing_chaoyang_red_A_d09_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,36,50000,null,40.0,true,beijing_chaoyang_red_B_d10_36,beijing_chaoyang_red_B_d10_36,0xcafebabe50,2024-09-24T06:15:36.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,36,31000,41.0,36.0,false,beijing_chaoyang_yellow_A_d11_41,beijing_chaoyang_yellow_A_d11_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,55,40000,30.0,55.0,true,beijing_chaoyang_yellow_B_d12_55,beijing_chaoyang_yellow_B_d12_30,0xcafebabe55,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,30,35000,30.0,35.0,true,beijing_haidian_red_A_d13_30,beijing_haidian_red_A_d13_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,36,50000,null,40.0,true,beijing_haidian_red_B_d14_36,beijing_haidian_red_B_d14_36,0xcafebabe50,2024-09-24T06:15:36.000Z,2024-09-24,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,36,31000,41.0,36.0,false,beijing_haidian_yellow_A_d15_41,beijing_haidian_yellow_A_d15_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,55,40000,30.0,55.0,true,beijing_haidian_yellow_B_d16_55,beijing_haidian_yellow_B_d16_30,0xcafebabe55,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,30,35000,30.0,35.0,true,shanghai_huangpu_red_A_d01_30,shanghai_huangpu_red_A_d01_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,36,50000,null,40.0,true,shanghai_huangpu_red_B_d02_36,shanghai_huangpu_red_B_d02_36,0xcafebabe50,2024-09-24T06:15:36.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,36,31000,41.0,36.0,false,shanghai_huangpu_yellow_A_d03_41,shanghai_huangpu_yellow_A_d03_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,55,40000,30.0,55.0,true,shanghai_huangpu_yellow_B_d04_55,shanghai_huangpu_yellow_B_d04_30,0xcafebabe55,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,30,35000,30.0,35.0,true,shanghai_pudong_red_A_d05_30,shanghai_pudong_red_A_d05_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,36,50000,null,40.0,true,shanghai_pudong_red_B_d06_36,shanghai_pudong_red_B_d06_36,0xcafebabe50,2024-09-24T06:15:36.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,36,31000,41.0,36.0,false,shanghai_pudong_yellow_A_d07_41,shanghai_pudong_yellow_A_d07_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,55,40000,30.0,55.0,true,shanghai_pudong_yellow_B_d08_55,shanghai_pudong_yellow_B_d08_30,0xcafebabe55,2024-09-24T06:15:30.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id, first(time),first(s1),first(s2),first(s3),first(s4),first(s5),first(s6),first(s7),first(s8),first(s9),first(s10) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void firstByTest() {
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:35.000Z,35000,",
        };
    tableResultSetEqualTest(
        "select first_by(time,s2),first(s2) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "_col1"};
    retArray =
        new String[] {
          "null,2024-09-24T06:15:30.000Z,",
        };
    tableResultSetEqualTest(
        "select first_by(s2,time),first(time) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "device_id", "_col2", "_col3"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,null,null,",
          "2024-09-24T06:15:35.000Z,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,d01,null,null,",
          "2024-09-24T06:15:50.000Z,d01,null,null,",
          "2024-09-24T06:15:55.000Z,d01,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, first_by(time, s4), first(s4) from table1 where device_id = 'd01' group by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "device_id", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,null,null,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,date_bin(5s, time), first_by(time,s3), first(s3) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,beijing_chaoyang_red_A_d09_35,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,beijing_chaoyang_red_B_d10_36,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:36.000Z,beijing_chaoyang_yellow_A_d11_36,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,beijing_chaoyang_yellow_B_d12_30,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,beijing_haidian_red_A_d13_35,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,beijing_haidian_red_B_d14_36,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:36.000Z,beijing_haidian_yellow_A_d15_36,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,beijing_haidian_yellow_B_d16_30,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,shanghai_huangpu_red_A_d01_35,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,shanghai_huangpu_red_B_d02_36,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:36.000Z,shanghai_huangpu_yellow_A_d03_36,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,shanghai_huangpu_yellow_B_d04_30,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,shanghai_pudong_red_A_d05_35,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,shanghai_pudong_red_B_d06_36,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:36.000Z,shanghai_pudong_yellow_A_d07_36,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,shanghai_pudong_yellow_B_d08_30,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,first_by(time,s7),first(s7) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"city", "region", "device_id", "_col3"};
    retArray =
        new String[] {
          "beijing,chaoyang,d09,null,",
          "beijing,chaoyang,d10,true,",
          "beijing,chaoyang,d11,null,",
          "beijing,chaoyang,d12,true,",
          "beijing,haidian,d13,null,",
          "beijing,haidian,d14,true,",
          "beijing,haidian,d15,null,",
          "beijing,haidian,d16,true,",
          "shanghai,huangpu,d01,null,",
          "shanghai,huangpu,d02,true,",
          "shanghai,huangpu,d03,null,",
          "shanghai,huangpu,d04,true,",
          "shanghai,pudong,d05,null,",
          "shanghai,pudong,d06,true,",
          "shanghai,pudong,d07,null,",
          "shanghai,pudong,d08,true,",
        };
    tableResultSetEqualTest(
        "select city,region,device_id,first_by(s5,time,time) from table1 group by city,region,device_id order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastTest() {
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:55.000Z,55,50000,40.0,55.0,false,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_40,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select last(time),last(s1),last(s2),last(s3),last(s4),last(s5),last(s6),last(s7),last(s8),last(s9),last(s10) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "_col0",
          "device_id",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_huangpu_red_A_d01_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "2024-09-24T06:15:35.000Z,d01,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "2024-09-24T06:15:40.000Z,d01,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_huangpu_red_A_d01_40,null,2024-09-24T06:15:40.000Z,null,",
          "2024-09-24T06:15:50.000Z,d01,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "2024-09-24T06:15:55.000Z,d01,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id, last(time),last(s1),last(s2),last(s3),last(s4),last(s5),last(s6),last(s7),last(s8),last(s9),last(s10) from table1 where device_id = 'd01' group by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14",
          "_col15"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,beijing_chaoyang_red_A_d09_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,beijing_chaoyang_red_A_d09_35,beijing_chaoyang_red_A_d09_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,beijing_chaoyang_red_A_d09_40,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,beijing_chaoyang_red_B_d10_36,beijing_chaoyang_red_B_d10_36,null,2024-09-24T06:15:36.000Z,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,beijing_chaoyang_red_B_d10_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,beijing_chaoyang_red_B_d10_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,beijing_chaoyang_yellow_A_d11_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,beijing_chaoyang_yellow_A_d11_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,beijing_chaoyang_yellow_A_d11_46,null,2024-09-24T06:15:46.000Z,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,beijing_chaoyang_yellow_A_d11_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,beijing_chaoyang_yellow_B_d12_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_chaoyang_yellow_B_d12_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,beijing_haidian_red_A_d13_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,beijing_haidian_red_A_d13_35,beijing_haidian_red_A_d13_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,beijing_haidian_red_A_d13_40,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,beijing_haidian_red_B_d14_36,beijing_haidian_red_B_d14_36,null,2024-09-24T06:15:36.000Z,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,beijing_haidian_red_B_d14_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,beijing_haidian_red_B_d14_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,beijing_haidian_yellow_A_d15_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,beijing_haidian_yellow_A_d15_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,beijing_haidian_yellow_A_d15_46,null,2024-09-24T06:15:46.000Z,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,beijing_haidian_yellow_A_d15_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,beijing_haidian_yellow_B_d16_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_haidian_yellow_B_d16_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_huangpu_red_A_d01_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_huangpu_red_A_d01_40,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,shanghai_huangpu_red_B_d02_36,shanghai_huangpu_red_B_d02_36,null,2024-09-24T06:15:36.000Z,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,shanghai_huangpu_red_B_d02_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,shanghai_huangpu_red_B_d02_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,shanghai_huangpu_yellow_A_d03_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,shanghai_huangpu_yellow_A_d03_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,shanghai_huangpu_yellow_A_d03_46,null,2024-09-24T06:15:46.000Z,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,shanghai_huangpu_yellow_A_d03_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,shanghai_huangpu_yellow_B_d04_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_pudong_red_A_d05_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_pudong_red_A_d05_35,shanghai_pudong_red_A_d05_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_pudong_red_A_d05_40,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,null,true,shanghai_pudong_red_B_d06_36,shanghai_pudong_red_B_d06_36,null,2024-09-24T06:15:36.000Z,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,null,40.0,null,null,shanghai_pudong_red_B_d06_40,null,2024-09-24T06:15:40.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,shanghai_pudong_red_B_d06_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,2024-09-24T06:15:31.000Z,null,31000,null,null,null,null,null,0xcafebabe31,2024-09-24T06:15:31.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,2024-09-24T06:15:36.000Z,36,null,null,36.0,null,null,shanghai_pudong_yellow_A_d07_36,null,2024-09-24T06:15:36.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41,null,41.0,null,false,shanghai_pudong_yellow_A_d07_41,null,0xcafebabe41,2024-09-24T06:15:41.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,2024-09-24T06:15:46.000Z,null,46000,null,46.0,null,null,shanghai_pudong_yellow_A_d07_46,null,2024-09-24T06:15:46.000Z,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,shanghai_pudong_yellow_A_d07_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,shanghai_pudong_yellow_B_d08_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_pudong_yellow_B_d08_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,date_bin(5s, time), last(time),last(s1),last(s2),last(s3),last(s4),last(s5),last(s6),last(s7),last(s8),last(s9),last(s10) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14",
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55,50000,40.0,55.0,false,beijing_chaoyang_red_A_d09_35,beijing_chaoyang_red_A_d09_40,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,40,50000,null,40.0,true,beijing_chaoyang_red_B_d10_36,beijing_chaoyang_red_B_d10_50,0xcafebabe50,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,41,46000,51.0,46.0,false,beijing_chaoyang_yellow_A_d11_51,beijing_chaoyang_yellow_A_d11_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55,40000,30.0,55.0,true,beijing_chaoyang_yellow_B_d12_55,beijing_chaoyang_yellow_B_d12_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,55,50000,40.0,55.0,false,beijing_haidian_red_A_d13_35,beijing_haidian_red_A_d13_40,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,40,50000,null,40.0,true,beijing_haidian_red_B_d14_36,beijing_haidian_red_B_d14_50,0xcafebabe50,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,41,46000,51.0,46.0,false,beijing_haidian_yellow_A_d15_51,beijing_haidian_yellow_A_d15_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,55,40000,30.0,55.0,true,beijing_haidian_yellow_B_d16_55,beijing_haidian_yellow_B_d16_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55,50000,40.0,55.0,false,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_40,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,40,50000,null,40.0,true,shanghai_huangpu_red_B_d02_36,shanghai_huangpu_red_B_d02_50,0xcafebabe50,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,41,46000,51.0,46.0,false,shanghai_huangpu_yellow_A_d03_51,shanghai_huangpu_yellow_A_d03_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55,40000,30.0,55.0,true,shanghai_huangpu_yellow_B_d04_55,shanghai_huangpu_yellow_B_d04_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,55,50000,40.0,55.0,false,shanghai_pudong_red_A_d05_35,shanghai_pudong_red_A_d05_40,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,40,50000,null,40.0,true,shanghai_pudong_red_B_d06_36,shanghai_pudong_red_B_d06_50,0xcafebabe50,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,41,46000,51.0,46.0,false,shanghai_pudong_yellow_A_d07_51,shanghai_pudong_yellow_A_d07_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,55,40000,30.0,55.0,true,shanghai_pudong_yellow_B_d08_55,shanghai_pudong_yellow_B_d08_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id, last(time),last(s1),last(s2),last(s3),last(s4),last(s5),last(s6),last(s7),last(s8),last(s9),last(s10) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastByTest() {
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:50.000Z,50000,",
        };
    repeatTest(
        "select last_by(time,s2),last(s2) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME,
        2);

    expectedHeader = new String[] {"_col0", "_col1"};
    retArray =
        new String[] {
          "null,2024-09-24T06:15:55.000Z,",
        };
    repeatTest(
        "select last_by(s2, time),last(time) from table1 where device_id = 'd01'",
        expectedHeader,
        retArray,
        DATABASE_NAME,
        2);

    expectedHeader = new String[] {"_col0", "device_id", "_col2", "_col3"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,null,null,",
          "2024-09-24T06:15:35.000Z,d01,2024-09-24T06:15:35.000Z,35.0,",
          "2024-09-24T06:15:40.000Z,d01,null,null,",
          "2024-09-24T06:15:50.000Z,d01,null,null,",
          "2024-09-24T06:15:55.000Z,d01,2024-09-24T06:15:55.000Z,55.0,",
        };
    repeatTest(
        "select date_bin(5s, time), device_id, last_by(time, s4), last(s4) from table1 where device_id = 'd01' group by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME,
        2);

    expectedHeader =
        new String[] {"province", "city", "region", "device_id", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:30.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:35.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:45.000Z,null,null,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,null,null,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,35.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40.0,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:30.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:35.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:40.000Z,2024-09-24T06:15:41.000Z,41.0,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:45.000Z,null,null,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:50.000Z,2024-09-24T06:15:51.000Z,51.0,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30.0,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,null,null,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,null,null,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,date_bin(5s, time), first_by(time,s3), first(s3) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,0xcafebabe50,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:41.000Z,0xcafebabe41,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,0xcafebabe50,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:41.000Z,0xcafebabe41,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,0xcafebabe50,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:41.000Z,0xcafebabe41,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,0xcafebabe55,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,0xcafebabe50,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:41.000Z,0xcafebabe41,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,0xcafebabe55,",
        };
    repeatTest(
        "select province,city,region,device_id,last_by(time,s8),last(s8) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME,
        2);

    expectedHeader = new String[] {"city", "region", "device_id", "_col3"};
    retArray =
        new String[] {
          "beijing,chaoyang,d09,null,",
          "beijing,chaoyang,d10,null,",
          "beijing,chaoyang,d11,null,",
          "beijing,chaoyang,d12,null,",
          "beijing,haidian,d13,null,",
          "beijing,haidian,d14,null,",
          "beijing,haidian,d15,null,",
          "beijing,haidian,d16,null,",
          "shanghai,huangpu,d01,null,",
          "shanghai,huangpu,d02,null,",
          "shanghai,huangpu,d03,null,",
          "shanghai,huangpu,d04,null,",
          "shanghai,pudong,d05,null,",
          "shanghai,pudong,d06,null,",
          "shanghai,pudong,d07,null,",
          "shanghai,pudong,d08,null,",
        };
    repeatTest(
        "select city,region,device_id,last_by(s5,time) from table1 group by city,region,device_id order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME,
        2);
  }

  @Test
  public void extremeTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3"};
    String[] retArray = new String[] {"d01,55,50000,55.0,"};

    tableResultSetEqualTest(
        "select device_id, extreme(s1), extreme(s2), extreme(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "_col1"};
    retArray = new String[] {"d01,40.0,"};
    tableResultSetEqualTest(
        "select device_id, extreme(s3) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "device_id", "_col2"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,d01,30.0,",
          "2024-09-24T06:15:35.000Z,d01,35.0,",
          "2024-09-24T06:15:40.000Z,d01,40.0,",
          "2024-09-24T06:15:50.000Z,d01,null,",
          "2024-09-24T06:15:55.000Z,d01,null,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time), device_id,extreme(s3) from table1 where device_id = 'd01' group by 1, 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "province", "city", "region", "device_id", "_col5"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,55.0,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,35.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,55.0,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,40.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,36.0,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,46.0,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,55.0,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,35.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,55.0,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,40.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,36.0,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,46.0,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,55.0,",
        };

    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id,extreme(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "province", "city", "region", "device_id", "_col5"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d09,30,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d09,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d09,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d09,55,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d10,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d10,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d10,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,chaoyang,d11,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d11,41,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,chaoyang,d11,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,chaoyang,d12,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,chaoyang,d12,55,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d13,30,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d13,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d13,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d13,55,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d14,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d14,40,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d14,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:35.000Z,beijing,beijing,haidian,d15,36,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d15,41,",
          "2024-09-24T06:15:45.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:50.000Z,beijing,beijing,haidian,d15,null,",
          "2024-09-24T06:15:30.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:40.000Z,beijing,beijing,haidian,d16,null,",
          "2024-09-24T06:15:55.000Z,beijing,beijing,haidian,d16,55,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d01,30,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d01,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d01,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d01,55,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d02,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d02,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d02,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,huangpu,d03,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d03,41,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,huangpu,d03,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,huangpu,d04,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,huangpu,d04,55,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d05,30,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d05,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d05,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d05,55,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d06,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d06,40,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d06,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:35.000Z,shanghai,shanghai,pudong,d07,36,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d07,41,",
          "2024-09-24T06:15:45.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:50.000Z,shanghai,shanghai,pudong,d07,null,",
          "2024-09-24T06:15:30.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:40.000Z,shanghai,shanghai,pudong,d08,null,",
          "2024-09-24T06:15:55.000Z,shanghai,shanghai,pudong,d08,55,",
        };
    tableResultSetEqualTest(
        "select date_bin(5s, time),province,city,region,device_id,extreme(s1) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,50000,",
          "beijing,beijing,chaoyang,d10,50000,",
          "beijing,beijing,chaoyang,d11,46000,",
          "beijing,beijing,chaoyang,d12,40000,",
          "beijing,beijing,haidian,d13,50000,",
          "beijing,beijing,haidian,d14,50000,",
          "beijing,beijing,haidian,d15,46000,",
          "beijing,beijing,haidian,d16,40000,",
          "shanghai,shanghai,huangpu,d01,50000,",
          "shanghai,shanghai,huangpu,d02,50000,",
          "shanghai,shanghai,huangpu,d03,46000,",
          "shanghai,shanghai,huangpu,d04,40000,",
          "shanghai,shanghai,pudong,d05,50000,",
          "shanghai,shanghai,pudong,d06,50000,",
          "shanghai,shanghai,pudong,d07,46000,",
          "shanghai,shanghai,pudong,d08,40000,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,extreme(s2) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,55.0,",
          "beijing,beijing,haidian,55.0,",
          "shanghai,shanghai,huangpu,55.0,",
          "shanghai,shanghai,pudong,55.0,",
        };
    tableResultSetEqualTest(
        "select province,city,region,extreme(s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2"};
    retArray =
        new String[] {
          "beijing,beijing,55.0,", "shanghai,shanghai,55.0,",
        };
    tableResultSetEqualTest(
        "select province,city,extreme(s4) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,55.0,", "shanghai,55.0,",
        };
    tableResultSetEqualTest(
        "select province,extreme(s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray =
        new String[] {
          "51.0,",
        };
    tableResultSetEqualTest(
        "select extreme(s3) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void groupByValueTest() {

    String[] expectedHeader = new String[] {"s1", "_col1"};
    String[] retArray =
        new String[] {
          "30,4,", "36,8,", "40,8,", "41,4,", "55,8,", "null,32,",
        };
    tableResultSetEqualTest(
        "select s1, count(*) from table1 group by s1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s1", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,30,1,",
          "beijing,beijing,chaoyang,d09,40,1,",
          "beijing,beijing,chaoyang,d09,55,1,",
          "beijing,beijing,chaoyang,d09,null,2,",
          "beijing,beijing,chaoyang,d10,36,1,",
          "beijing,beijing,chaoyang,d10,40,1,",
          "beijing,beijing,chaoyang,d10,null,1,",
          "beijing,beijing,chaoyang,d11,36,1,",
          "beijing,beijing,chaoyang,d11,41,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,55,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,30,1,",
          "beijing,beijing,haidian,d13,40,1,",
          "beijing,beijing,haidian,d13,55,1,",
          "beijing,beijing,haidian,d13,null,2,",
          "beijing,beijing,haidian,d14,36,1,",
          "beijing,beijing,haidian,d14,40,1,",
          "beijing,beijing,haidian,d14,null,1,",
          "beijing,beijing,haidian,d15,36,1,",
          "beijing,beijing,haidian,d15,41,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,55,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,30,1,",
          "shanghai,shanghai,huangpu,d01,40,1,",
          "shanghai,shanghai,huangpu,d01,55,1,",
          "shanghai,shanghai,huangpu,d01,null,2,",
          "shanghai,shanghai,huangpu,d02,36,1,",
          "shanghai,shanghai,huangpu,d02,40,1,",
          "shanghai,shanghai,huangpu,d02,null,1,",
          "shanghai,shanghai,huangpu,d03,36,1,",
          "shanghai,shanghai,huangpu,d03,41,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,55,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,30,1,",
          "shanghai,shanghai,pudong,d05,40,1,",
          "shanghai,shanghai,pudong,d05,55,1,",
          "shanghai,shanghai,pudong,d05,null,2,",
          "shanghai,shanghai,pudong,d06,36,1,",
          "shanghai,shanghai,pudong,d06,40,1,",
          "shanghai,shanghai,pudong,d06,null,1,",
          "shanghai,shanghai,pudong,d07,36,1,",
          "shanghai,shanghai,pudong,d07,41,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,55,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s1,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s2", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,35000,1,",
          "beijing,beijing,chaoyang,d09,50000,1,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,50000,1,",
          "beijing,beijing,chaoyang,d10,null,2,",
          "beijing,beijing,chaoyang,d11,31000,1,",
          "beijing,beijing,chaoyang,d11,46000,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,40000,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,35000,1,",
          "beijing,beijing,haidian,d13,50000,1,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,50000,1,",
          "beijing,beijing,haidian,d14,null,2,",
          "beijing,beijing,haidian,d15,31000,1,",
          "beijing,beijing,haidian,d15,46000,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,40000,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,35000,1,",
          "shanghai,shanghai,huangpu,d01,50000,1,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,50000,1,",
          "shanghai,shanghai,huangpu,d02,null,2,",
          "shanghai,shanghai,huangpu,d03,31000,1,",
          "shanghai,shanghai,huangpu,d03,46000,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,40000,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,35000,1,",
          "shanghai,shanghai,pudong,d05,50000,1,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,50000,1,",
          "shanghai,shanghai,pudong,d06,null,2,",
          "shanghai,shanghai,pudong,d07,31000,1,",
          "shanghai,shanghai,pudong,d07,46000,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,40000,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s2,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s3", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,30.0,1,",
          "beijing,beijing,chaoyang,d09,35.0,1,",
          "beijing,beijing,chaoyang,d09,40.0,1,",
          "beijing,beijing,chaoyang,d09,null,2,",
          "beijing,beijing,chaoyang,d10,null,3,",
          "beijing,beijing,chaoyang,d11,41.0,1,",
          "beijing,beijing,chaoyang,d11,51.0,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,30.0,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,30.0,1,",
          "beijing,beijing,haidian,d13,35.0,1,",
          "beijing,beijing,haidian,d13,40.0,1,",
          "beijing,beijing,haidian,d13,null,2,",
          "beijing,beijing,haidian,d14,null,3,",
          "beijing,beijing,haidian,d15,41.0,1,",
          "beijing,beijing,haidian,d15,51.0,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,30.0,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,30.0,1,",
          "shanghai,shanghai,huangpu,d01,35.0,1,",
          "shanghai,shanghai,huangpu,d01,40.0,1,",
          "shanghai,shanghai,huangpu,d01,null,2,",
          "shanghai,shanghai,huangpu,d02,null,3,",
          "shanghai,shanghai,huangpu,d03,41.0,1,",
          "shanghai,shanghai,huangpu,d03,51.0,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,30.0,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,30.0,1,",
          "shanghai,shanghai,pudong,d05,35.0,1,",
          "shanghai,shanghai,pudong,d05,40.0,1,",
          "shanghai,shanghai,pudong,d05,null,2,",
          "shanghai,shanghai,pudong,d06,null,3,",
          "shanghai,shanghai,pudong,d07,41.0,1,",
          "shanghai,shanghai,pudong,d07,51.0,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,30.0,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s3,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,35.0,1,",
          "beijing,beijing,chaoyang,d09,55.0,1,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,40.0,1,",
          "beijing,beijing,chaoyang,d10,null,2,",
          "beijing,beijing,chaoyang,d11,36.0,1,",
          "beijing,beijing,chaoyang,d11,46.0,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,55.0,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,35.0,1,",
          "beijing,beijing,haidian,d13,55.0,1,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,40.0,1,",
          "beijing,beijing,haidian,d14,null,2,",
          "beijing,beijing,haidian,d15,36.0,1,",
          "beijing,beijing,haidian,d15,46.0,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,55.0,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,35.0,1,",
          "shanghai,shanghai,huangpu,d01,55.0,1,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,40.0,1,",
          "shanghai,shanghai,huangpu,d02,null,2,",
          "shanghai,shanghai,huangpu,d03,36.0,1,",
          "shanghai,shanghai,huangpu,d03,46.0,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,55.0,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,35.0,1,",
          "shanghai,shanghai,pudong,d05,55.0,1,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,40.0,1,",
          "shanghai,shanghai,pudong,d06,null,2,",
          "shanghai,shanghai,pudong,d07,36.0,1,",
          "shanghai,shanghai,pudong,d07,46.0,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,55.0,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s4,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s5", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,false,1,",
          "beijing,beijing,chaoyang,d09,true,1,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,true,1,",
          "beijing,beijing,chaoyang,d10,null,2,",
          "beijing,beijing,chaoyang,d11,false,1,",
          "beijing,beijing,chaoyang,d11,null,4,",
          "beijing,beijing,chaoyang,d12,true,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,false,1,",
          "beijing,beijing,haidian,d13,true,1,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,true,1,",
          "beijing,beijing,haidian,d14,null,2,",
          "beijing,beijing,haidian,d15,false,1,",
          "beijing,beijing,haidian,d15,null,4,",
          "beijing,beijing,haidian,d16,true,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,false,1,",
          "shanghai,shanghai,huangpu,d01,true,1,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,true,1,",
          "shanghai,shanghai,huangpu,d02,null,2,",
          "shanghai,shanghai,huangpu,d03,false,1,",
          "shanghai,shanghai,huangpu,d03,null,4,",
          "shanghai,shanghai,huangpu,d04,true,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,false,1,",
          "shanghai,shanghai,pudong,d05,true,1,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,true,1,",
          "shanghai,shanghai,pudong,d06,null,2,",
          "shanghai,shanghai,pudong,d07,false,1,",
          "shanghai,shanghai,pudong,d07,null,4,",
          "shanghai,shanghai,pudong,d08,true,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s5,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s6", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,beijing_chaoyang_red_A_d09_30,1,",
          "beijing,beijing,chaoyang,d09,beijing_chaoyang_red_A_d09_35,1,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,beijing_chaoyang_red_B_d10_36,1,",
          "beijing,beijing,chaoyang,d10,null,2,",
          "beijing,beijing,chaoyang,d11,beijing_chaoyang_yellow_A_d11_41,1,",
          "beijing,beijing,chaoyang,d11,beijing_chaoyang_yellow_A_d11_51,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,beijing_chaoyang_yellow_B_d12_55,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,beijing_haidian_red_A_d13_30,1,",
          "beijing,beijing,haidian,d13,beijing_haidian_red_A_d13_35,1,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,beijing_haidian_red_B_d14_36,1,",
          "beijing,beijing,haidian,d14,null,2,",
          "beijing,beijing,haidian,d15,beijing_haidian_yellow_A_d15_41,1,",
          "beijing,beijing,haidian,d15,beijing_haidian_yellow_A_d15_51,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,beijing_haidian_yellow_B_d16_55,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,shanghai_huangpu_red_A_d01_30,1,",
          "shanghai,shanghai,huangpu,d01,shanghai_huangpu_red_A_d01_35,1,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,shanghai_huangpu_red_B_d02_36,1,",
          "shanghai,shanghai,huangpu,d02,null,2,",
          "shanghai,shanghai,huangpu,d03,shanghai_huangpu_yellow_A_d03_41,1,",
          "shanghai,shanghai,huangpu,d03,shanghai_huangpu_yellow_A_d03_51,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,shanghai_huangpu_yellow_B_d04_55,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,shanghai_pudong_red_A_d05_30,1,",
          "shanghai,shanghai,pudong,d05,shanghai_pudong_red_A_d05_35,1,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,shanghai_pudong_red_B_d06_36,1,",
          "shanghai,shanghai,pudong,d06,null,2,",
          "shanghai,shanghai,pudong,d07,shanghai_pudong_yellow_A_d07_41,1,",
          "shanghai,shanghai,pudong,d07,shanghai_pudong_yellow_A_d07_51,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,shanghai_pudong_yellow_B_d08_55,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s6,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s7", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,beijing_chaoyang_red_A_d09_35,1,",
          "beijing,beijing,chaoyang,d09,beijing_chaoyang_red_A_d09_40,1,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,beijing_chaoyang_red_B_d10_36,1,",
          "beijing,beijing,chaoyang,d10,beijing_chaoyang_red_B_d10_40,1,",
          "beijing,beijing,chaoyang,d10,beijing_chaoyang_red_B_d10_50,1,",
          "beijing,beijing,chaoyang,d11,beijing_chaoyang_yellow_A_d11_36,1,",
          "beijing,beijing,chaoyang,d11,beijing_chaoyang_yellow_A_d11_46,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,beijing_chaoyang_yellow_B_d12_30,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,beijing_haidian_red_A_d13_35,1,",
          "beijing,beijing,haidian,d13,beijing_haidian_red_A_d13_40,1,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,beijing_haidian_red_B_d14_36,1,",
          "beijing,beijing,haidian,d14,beijing_haidian_red_B_d14_40,1,",
          "beijing,beijing,haidian,d14,beijing_haidian_red_B_d14_50,1,",
          "beijing,beijing,haidian,d15,beijing_haidian_yellow_A_d15_36,1,",
          "beijing,beijing,haidian,d15,beijing_haidian_yellow_A_d15_46,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,beijing_haidian_yellow_B_d16_30,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,shanghai_huangpu_red_A_d01_35,1,",
          "shanghai,shanghai,huangpu,d01,shanghai_huangpu_red_A_d01_40,1,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,shanghai_huangpu_red_B_d02_36,1,",
          "shanghai,shanghai,huangpu,d02,shanghai_huangpu_red_B_d02_40,1,",
          "shanghai,shanghai,huangpu,d02,shanghai_huangpu_red_B_d02_50,1,",
          "shanghai,shanghai,huangpu,d03,shanghai_huangpu_yellow_A_d03_36,1,",
          "shanghai,shanghai,huangpu,d03,shanghai_huangpu_yellow_A_d03_46,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,shanghai_huangpu_yellow_B_d04_30,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,shanghai_pudong_red_A_d05_35,1,",
          "shanghai,shanghai,pudong,d05,shanghai_pudong_red_A_d05_40,1,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,shanghai_pudong_red_B_d06_36,1,",
          "shanghai,shanghai,pudong,d06,shanghai_pudong_red_B_d06_40,1,",
          "shanghai,shanghai,pudong,d06,shanghai_pudong_red_B_d06_50,1,",
          "shanghai,shanghai,pudong,d07,shanghai_pudong_yellow_A_d07_36,1,",
          "shanghai,shanghai,pudong,d07,shanghai_pudong_yellow_A_d07_46,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,shanghai_pudong_yellow_B_d08_30,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s7,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select province,city,region,device_id,s7,count(1) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s8", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,0xcafebabe30,1,",
          "beijing,beijing,chaoyang,d09,0xcafebabe55,1,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,0xcafebabe50,1,",
          "beijing,beijing,chaoyang,d10,null,2,",
          "beijing,beijing,chaoyang,d11,0xcafebabe31,1,",
          "beijing,beijing,chaoyang,d11,0xcafebabe41,1,",
          "beijing,beijing,chaoyang,d11,null,3,",
          "beijing,beijing,chaoyang,d12,0xcafebabe55,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,0xcafebabe30,1,",
          "beijing,beijing,haidian,d13,0xcafebabe55,1,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,0xcafebabe50,1,",
          "beijing,beijing,haidian,d14,null,2,",
          "beijing,beijing,haidian,d15,0xcafebabe31,1,",
          "beijing,beijing,haidian,d15,0xcafebabe41,1,",
          "beijing,beijing,haidian,d15,null,3,",
          "beijing,beijing,haidian,d16,0xcafebabe55,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,0xcafebabe30,1,",
          "shanghai,shanghai,huangpu,d01,0xcafebabe55,1,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,0xcafebabe50,1,",
          "shanghai,shanghai,huangpu,d02,null,2,",
          "shanghai,shanghai,huangpu,d03,0xcafebabe31,1,",
          "shanghai,shanghai,huangpu,d03,0xcafebabe41,1,",
          "shanghai,shanghai,huangpu,d03,null,3,",
          "shanghai,shanghai,huangpu,d04,0xcafebabe55,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,0xcafebabe30,1,",
          "shanghai,shanghai,pudong,d05,0xcafebabe55,1,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,0xcafebabe50,1,",
          "shanghai,shanghai,pudong,d06,null,2,",
          "shanghai,shanghai,pudong,d07,0xcafebabe31,1,",
          "shanghai,shanghai,pudong,d07,0xcafebabe41,1,",
          "shanghai,shanghai,pudong,d07,null,3,",
          "shanghai,shanghai,pudong,d08,0xcafebabe55,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s8,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s9", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,1,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,1,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,1,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,1,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,1,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:36.000Z,1,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:40.000Z,1,",
          "beijing,beijing,chaoyang,d10,2024-09-24T06:15:50.000Z,1,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:31.000Z,1,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:36.000Z,1,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:41.000Z,1,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:46.000Z,1,",
          "beijing,beijing,chaoyang,d11,2024-09-24T06:15:51.000Z,1,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,1,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,1,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,1,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:30.000Z,1,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:35.000Z,1,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:40.000Z,1,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:50.000Z,1,",
          "beijing,beijing,haidian,d13,2024-09-24T06:15:55.000Z,1,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:36.000Z,1,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:40.000Z,1,",
          "beijing,beijing,haidian,d14,2024-09-24T06:15:50.000Z,1,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:31.000Z,1,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:36.000Z,1,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:41.000Z,1,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:46.000Z,1,",
          "beijing,beijing,haidian,d15,2024-09-24T06:15:51.000Z,1,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:30.000Z,1,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:40.000Z,1,",
          "beijing,beijing,haidian,d16,2024-09-24T06:15:55.000Z,1,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,1,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,1,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,1,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,1,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,1,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:36.000Z,1,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:40.000Z,1,",
          "shanghai,shanghai,huangpu,d02,2024-09-24T06:15:50.000Z,1,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:31.000Z,1,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:36.000Z,1,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:41.000Z,1,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:46.000Z,1,",
          "shanghai,shanghai,huangpu,d03,2024-09-24T06:15:51.000Z,1,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,1,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,1,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,1,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:30.000Z,1,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:35.000Z,1,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:40.000Z,1,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:50.000Z,1,",
          "shanghai,shanghai,pudong,d05,2024-09-24T06:15:55.000Z,1,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:36.000Z,1,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:40.000Z,1,",
          "shanghai,shanghai,pudong,d06,2024-09-24T06:15:50.000Z,1,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:31.000Z,1,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:36.000Z,1,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:41.000Z,1,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:46.000Z,1,",
          "shanghai,shanghai,pudong,d07,2024-09-24T06:15:51.000Z,1,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:30.000Z,1,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:40.000Z,1,",
          "shanghai,shanghai,pudong,d08,2024-09-24T06:15:55.000Z,1,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s9,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "s10", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24,2,",
          "beijing,beijing,chaoyang,d09,null,3,",
          "beijing,beijing,chaoyang,d10,2024-09-24,1,",
          "beijing,beijing,chaoyang,d10,null,2,",
          "beijing,beijing,chaoyang,d11,2024-09-24,1,",
          "beijing,beijing,chaoyang,d11,null,4,",
          "beijing,beijing,chaoyang,d12,2024-09-24,1,",
          "beijing,beijing,chaoyang,d12,null,2,",
          "beijing,beijing,haidian,d13,2024-09-24,2,",
          "beijing,beijing,haidian,d13,null,3,",
          "beijing,beijing,haidian,d14,2024-09-24,1,",
          "beijing,beijing,haidian,d14,null,2,",
          "beijing,beijing,haidian,d15,2024-09-24,1,",
          "beijing,beijing,haidian,d15,null,4,",
          "beijing,beijing,haidian,d16,2024-09-24,1,",
          "beijing,beijing,haidian,d16,null,2,",
          "shanghai,shanghai,huangpu,d01,2024-09-24,2,",
          "shanghai,shanghai,huangpu,d01,null,3,",
          "shanghai,shanghai,huangpu,d02,2024-09-24,1,",
          "shanghai,shanghai,huangpu,d02,null,2,",
          "shanghai,shanghai,huangpu,d03,2024-09-24,1,",
          "shanghai,shanghai,huangpu,d03,null,4,",
          "shanghai,shanghai,huangpu,d04,2024-09-24,1,",
          "shanghai,shanghai,huangpu,d04,null,2,",
          "shanghai,shanghai,pudong,d05,2024-09-24,2,",
          "shanghai,shanghai,pudong,d05,null,3,",
          "shanghai,shanghai,pudong,d06,2024-09-24,1,",
          "shanghai,shanghai,pudong,d06,null,2,",
          "shanghai,shanghai,pudong,d07,2024-09-24,1,",
          "shanghai,shanghai,pudong,d07,null,4,",
          "shanghai,shanghai,pudong,d08,2024-09-24,1,",
          "shanghai,shanghai,pudong,d08,null,2,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,s10,count(*) from table1 group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastQueryTest() {

    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id='d01'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11"
        };
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d04,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d09,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d12,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_chaoyang_yellow_B_d12_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select device_id,last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id in ('d01', 'd04', 'd09', 'd12') group by device_id order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_chaoyang_yellow_B_d12_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id,last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id in ('d01', 'd04', 'd09', 'd12') group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_huangpu_red_A_d01_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_huangpu_red_A_d01_40,null,2024-09-24T06:15:40.000Z,null,",
          "d01,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d04,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,shanghai_huangpu_yellow_B_d04_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "d04,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,beijing_chaoyang_red_A_d09_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,beijing_chaoyang_red_A_d09_35,beijing_chaoyang_red_A_d09_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,beijing_chaoyang_red_A_d09_40,null,2024-09-24T06:15:40.000Z,null,",
          "d09,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d12,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,beijing_chaoyang_yellow_B_d12_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "d12,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_chaoyang_yellow_B_d12_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select device_id,date_bin(5s,time),last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id in ('d01', 'd04', 'd09', 'd12') group by province,city,region,device_id,date_bin(5s,time) order by device_id,date_bin(5s,time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "device_id",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13",
          "_col14",
          "_col15"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,beijing_chaoyang_red_A_d09_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,beijing_chaoyang_red_A_d09_35,beijing_chaoyang_red_A_d09_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,beijing_chaoyang_red_A_d09_40,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d09,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,beijing_chaoyang_yellow_B_d12_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "beijing,beijing,chaoyang,d12,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_chaoyang_yellow_B_d12_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,30,null,30.0,null,null,shanghai_huangpu_red_A_d01_30,null,0xcafebabe30,2024-09-24T06:15:30.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:35.000Z,2024-09-24T06:15:35.000Z,null,35000,35.0,35.0,null,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_35,null,2024-09-24T06:15:35.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,40,null,40.0,null,true,null,shanghai_huangpu_red_A_d01_40,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:50.000Z,2024-09-24T06:15:50.000Z,null,50000,null,null,false,null,null,null,2024-09-24T06:15:50.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,null,null,30.0,null,true,null,shanghai_huangpu_yellow_B_d04_30,null,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:40.000Z,2024-09-24T06:15:40.000Z,null,40000,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "shanghai,shanghai,huangpu,d04,2024-09-24T06:15:55.000Z,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select province,city,region,device_id,date_bin(5s,time),last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id in ('d01', 'd04', 'd09', 'd12') group by 1,2,3,4,5 order by 1,2,3,4,5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "beijing,beijing,haidian,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_haidian_yellow_B_d16_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,huangpu,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,pudong,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select province,city,region,last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id NOT in ('d01', 'd08', 'd12', 'd13') group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_haidian_yellow_B_d16_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,shanghai,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select province,city,last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id NOT in ('d01', 'd05', 'd08', 'd09', 'd12', 'd13') group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11"
        };
    retArray =
        new String[] {
          "beijing,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,beijing_haidian_yellow_B_d16_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "shanghai,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
        };

    tableResultSetEqualTest(
        "select province,last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where device_id NOT in ('d01', 'd05', 'd08', 'd09', 'd12', 'd13') group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11"
        };
    retArray =
        new String[] {
          "d01,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d02,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,shanghai_huangpu_red_B_d02_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "d03,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,shanghai_huangpu_yellow_A_d03_51,null,null,2024-09-24T06:15:51.000Z,null,",
          "d04,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,shanghai_huangpu_yellow_B_d04_55,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d05,2024-09-24T06:15:55.000Z,55,null,null,55.0,null,null,null,0xcafebabe55,2024-09-24T06:15:55.000Z,null,",
          "d06,2024-09-24T06:15:50.000Z,null,50000,null,null,null,null,shanghai_pudong_red_B_d06_50,0xcafebabe50,2024-09-24T06:15:50.000Z,null,",
          "d07,2024-09-24T06:15:51.000Z,null,null,51.0,null,null,shanghai_pudong_yellow_A_d07_51,null,null,2024-09-24T06:15:51.000Z,null,",
        };

    tableResultSetEqualTest(
        "select device_id, last(time),last_by(s1,time),last_by(s2,time),last_by(s3,time),last_by(s4,time),last_by(s5,time),last_by(s6,time),last_by(s7,time),last_by(s8,time),last_by(s9,time),last_by(s10,time) from table1 where city = 'shanghai' group by province,city,region,device_id order by device_id limit 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void specialCasesTest() {
    String[] expectedHeader = new String[] {"device_id"};
    String[] retArray =
        new String[] {
          "d01,", "d02,", "d03,", "d04,", "d05,", "d06,", "d07,", "d08,", "d09,", "d10,", "d11,",
          "d12,", "d13,", "d14,", "d15,", "d16,",
        };
    tableResultSetEqualTest(
        "SELECT device_id FROM table1 GROUP BY device_id order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time"};
    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,",
          "2024-09-24T06:15:31.000Z,",
          "2024-09-24T06:15:35.000Z,",
          "2024-09-24T06:15:36.000Z,",
          "2024-09-24T06:15:40.000Z,",
          "2024-09-24T06:15:41.000Z,",
          "2024-09-24T06:15:46.000Z,",
          "2024-09-24T06:15:50.000Z,",
          "2024-09-24T06:15:51.000Z,",
          "2024-09-24T06:15:55.000Z,"
        };
    tableResultSetEqualTest(
        "select time from table1 group by time order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void modeTest() {
    // AggTableScan + Agg mixed test
    String[] expectedHeader = buildHeaders(10);
    String[] retArray =
        new String[] {
          "null,null,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
          "null,null,null,null,null,null,null,null,2024-09-24T06:15:40.000Z,null,",
        };
    tableResultSetEqualTest(
        "select mode(s1),mode(s2),mode(s3),mode(s4),mode(s5),mode(s6),mode(s7),mode(s8),mode(s9),mode(s10) from table1 group by city",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void approxCountDistinctTest() {
    String[] expectedHeader = buildHeaders(15);
    String[] retArray = new String[] {"10,2,2,4,16,5,5,5,5,2,24,32,5,10,1,"};
    tableResultSetEqualTest(
        "select approx_count_distinct(time), approx_count_distinct(province), approx_count_distinct(city), approx_count_distinct(region), approx_count_distinct(device_id), approx_count_distinct(s1), approx_count_distinct(s2), approx_count_distinct(s3), approx_count_distinct(s4), approx_count_distinct(s5), approx_count_distinct(s6), approx_count_distinct(s7), approx_count_distinct(s8), approx_count_distinct(s9), approx_count_distinct(s10) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select approx_count_distinct(time, 0.02), approx_count_distinct(province, 0.02), approx_count_distinct(city, 0.02), approx_count_distinct(region, 0.02), approx_count_distinct(device_id, 0.02), approx_count_distinct(s1, 0.02), approx_count_distinct(s2, 0.02), approx_count_distinct(s3, 0.02), approx_count_distinct(s4, 0.02), approx_count_distinct(s5, 0.02), approx_count_distinct(s6, 0.02), approx_count_distinct(s7, 0.02), approx_count_distinct(s8, 0.02), approx_count_distinct(s9, 0.02), approx_count_distinct(s10, 0.02) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2024-09-24T06:15:30.000Z,beijing,2,2,",
          "2024-09-24T06:15:31.000Z,beijing,0,0,",
          "2024-09-24T06:15:35.000Z,beijing,2,2,",
          "2024-09-24T06:15:36.000Z,beijing,2,4,",
          "2024-09-24T06:15:40.000Z,beijing,0,4,",
          "2024-09-24T06:15:41.000Z,beijing,2,0,",
          "2024-09-24T06:15:46.000Z,beijing,0,2,",
          "2024-09-24T06:15:50.000Z,beijing,0,2,",
          "2024-09-24T06:15:51.000Z,beijing,2,0,",
          "2024-09-24T06:15:55.000Z,beijing,2,0,",
          "2024-09-24T06:15:30.000Z,shanghai,2,2,",
          "2024-09-24T06:15:31.000Z,shanghai,0,0,",
          "2024-09-24T06:15:35.000Z,shanghai,2,2,",
          "2024-09-24T06:15:36.000Z,shanghai,2,4,",
          "2024-09-24T06:15:40.000Z,shanghai,0,4,",
          "2024-09-24T06:15:41.000Z,shanghai,2,0,",
          "2024-09-24T06:15:46.000Z,shanghai,0,2,",
          "2024-09-24T06:15:50.000Z,shanghai,0,2,",
          "2024-09-24T06:15:51.000Z,shanghai,2,0,",
          "2024-09-24T06:15:55.000Z,shanghai,2,0,",
        };

    tableResultSetEqualTest(
        "select time,province,approx_count_distinct(s6),approx_count_distinct(s7) from table1 group by 1,2 order by 2,1",
        new String[] {"time", "province", "_col2", "_col3"},
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select time,province,approx_count_distinct(s6,0.02),approx_count_distinct(s7,0.02) from table1 group by 1,2 order by 2,1",
        new String[] {"time", "province", "_col2", "_col3"},
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void exceptionTest() {
    tableAssertTestFail(
        "select avg() from table1",
        "701: Aggregate functions [avg] should only have one argument",
        DATABASE_NAME);
    tableAssertTestFail(
        "select sum() from table1",
        "701: Aggregate functions [sum] should only have one argument",
        DATABASE_NAME);
    tableAssertTestFail(
        "select extreme() from table1",
        "701: Aggregate functions [extreme] should only have one argument",
        DATABASE_NAME);
    tableAssertTestFail(
        "select first() from table1",
        "701: Aggregate functions [first] should only have one or two arguments",
        DATABASE_NAME);
    tableAssertTestFail(
        "select first_by() from table1",
        "701: Aggregate functions [first_by] should only have two or three arguments",
        DATABASE_NAME);
    tableAssertTestFail(
        "select last() from table1",
        "701: Aggregate functions [last] should only have one or two arguments",
        DATABASE_NAME);
    tableAssertTestFail(
        "select last_by() from table1",
        "701: Aggregate functions [last_by] should only have two or three arguments",
        DATABASE_NAME);
    tableAssertTestFail(
        "select approx_count_distinct() from table1",
        "701: Aggregate functions [approx_count_distinct] should only have two arguments",
        DATABASE_NAME);
    tableAssertTestFail(
        "select approx_count_distinct(province, 0.3) from table1",
        "750: Max Standard Error must be in [0.0040625, 0.26]: 0.3",
        DATABASE_NAME);
    tableAssertTestFail(
        "select approx_count_distinct(province, 0.3) from table1",
        "750: Max Standard Error must be in [0.0040625, 0.26]: 0.3",
        DATABASE_NAME);
    tableAssertTestFail(
        "select approx_count_distinct(province, 'test') from table1",
        "701: Second argument of Aggregate functions [approx_count_distinct] should be numberic type and do not use expression",
        DATABASE_NAME);
  }

  // ==================================================================
  // ===================== Select Distinct Test =======================
  // ==================================================================

  // Select distinct is a special kind of aggregate query in actual, so we put ITs here to reuse the
  // test data.

  @Test
  public void simpleTest() {
    String[] expectedHeader = new String[] {"s1"};
    String[] retArray = new String[] {"30,", "36,", "40,", "41,", "55,", "null,"};
    tableResultSetEqualTest(
        "select distinct s1 from table1 order by s1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"region", "s1"};
    retArray =
        new String[] {
          "chaoyang,30,",
          "chaoyang,36,",
          "chaoyang,40,",
          "chaoyang,41,",
          "chaoyang,55,",
          "chaoyang,null,",
          "haidian,30,",
          "haidian,36,",
          "haidian,40,",
          "haidian,41,",
          "haidian,55,",
          "haidian,null,",
          "huangpu,30,",
          "huangpu,36,",
          "huangpu,40,",
          "huangpu,41,",
          "huangpu,55,",
          "huangpu,null,",
          "pudong,30,",
          "pudong,36,",
          "pudong,40,",
          "pudong,41,",
          "pudong,55,",
          "pudong,null,"
        };
    tableResultSetEqualTest(
        "select distinct region, s1 from table1 order by region, s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // show all devices
    expectedHeader = new String[] {"province", "city", "region", "device_id"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,",
          "beijing,beijing,chaoyang,d10,",
          "beijing,beijing,chaoyang,d11,",
          "beijing,beijing,chaoyang,d12,",
          "beijing,beijing,haidian,d13,",
          "beijing,beijing,haidian,d14,",
          "beijing,beijing,haidian,d15,",
          "beijing,beijing,haidian,d16,",
          "shanghai,shanghai,huangpu,d01,",
          "shanghai,shanghai,huangpu,d02,",
          "shanghai,shanghai,huangpu,d03,",
          "shanghai,shanghai,huangpu,d04,",
          "shanghai,shanghai,pudong,d05,",
          "shanghai,shanghai,pudong,d06,",
          "shanghai,shanghai,pudong,d07,",
          "shanghai,shanghai,pudong,d08,",
        };
    tableResultSetEqualTest(
        "select distinct province,city,region,device_id from table1 order by province,city,region,device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"false,"};
    tableResultSetEqualTest(
        "select distinct s1 < 0 from table1 where s1 is not null",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void withGroupByTest() {
    String[] expectedHeader = new String[] {"s1"};
    String[] retArray = new String[] {"30,", "36,", "40,", "41,", "55,", "null,"};
    tableResultSetEqualTest(
        "select distinct s1 from table1 group by s1 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select distinct s1 from table1 group by s1,s2 order by s1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"30.0,", "36.0,", "40.0,", "41.0,", "55.0,", "null,"};
    tableResultSetEqualTest(
        "select distinct avg(s1) from table1 group by s1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select distinct avg(s1) from table1 group by s1,s2 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray = new String[] {"4,", "8,", "32,"};
    tableResultSetEqualTest(
        "select distinct count(*) from table1 group by s1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    retArray = new String[] {"4,", "8,"};
    tableResultSetEqualTest(
        "select distinct count(*) from table1 group by s1, s2 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void exceptionTest1() {
    tableAssertTestFail(
        "select distinct s1 from table1 order by s2",
        "701: For SELECT DISTINCT, ORDER BY expressions must appear in select list",
        DATABASE_NAME);
  }

  // ==================================================================
  // ================== Agg-Function Distinct Test ====================
  // ==================================================================
  @Test
  public void countDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11", "_col12", "_col13"
        };
    String[] retArray =
        new String[] {
          "2,2,4,16,5,5,5,5,2,24,32,5,10,1,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select count(distinct province), count(distinct city), count(distinct region), count(distinct device_id), count(distinct s1), count(distinct s2), count(distinct s3), count(distinct s4), count(distinct s5), count(distinct s6), count(distinct s7), count(distinct s8), count(distinct s9), count(distinct s10) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,5,5,5,5,2,6,8,5,10,1,",
          "beijing,beijing,haidian,5,5,5,5,2,6,8,5,10,1,",
          "shanghai,shanghai,huangpu,5,5,5,5,2,6,8,5,10,1,",
          "shanghai,shanghai,pudong,5,5,5,5,2,6,8,5,10,1,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, count(distinct s1), count(distinct s2), count(distinct s3), count(distinct s4), count(distinct s5), count(distinct s6), count(distinct s7), count(distinct s8), count(distinct s9), count(distinct s10) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    retArray =
        new String[] {
          "chaoyang,5,5,5,5,2,6,8,5,10,1,",
          "haidian,5,5,5,5,2,6,8,5,10,1,",
          "huangpu,5,5,5,5,2,6,8,5,10,1,",
          "pudong,5,5,5,5,2,6,8,5,10,1,"
        };
    tableResultSetEqualTest(
        "select region, count(distinct s1), count(distinct s2), count(distinct s3), count(distinct s4), count(distinct s5), count(distinct s6), count(distinct s7), count(distinct s8), count(distinct s9), count(distinct s10) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void countIfDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11", "_col12", "_col13"
        };
    String[] retArray =
        new String[] {
          "1,1,1,1,0,1,1,1,1,1,1,1,1,1,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select count_if(distinct province = 'shanghai'), count_if(distinct city = 'shanghai'), count_if(distinct region= 'huangpu'), count_if(distinct device_id = 'd03'), count_if(distinct s1 < 0), count_if(distinct s2 is not null), count_if(distinct s3 is not null), count_if(distinct s4 is not null), count_if(distinct s5 is not null), count_if(distinct s6 is not null), count_if(distinct s7 is not null), count_if(distinct s8 is not null), count_if(distinct s9 is not null), count_if(distinct s10 is not null) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,0,1,1,1,1,1,1,1,1,1,",
          "beijing,beijing,haidian,0,1,1,1,1,1,1,1,1,1,",
          "shanghai,shanghai,huangpu,0,1,1,1,1,1,1,1,1,1,",
          "shanghai,shanghai,pudong,0,1,1,1,1,1,1,1,1,1,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, count_if(distinct s1 < 0), count_if(distinct s2 is not null), count_if(distinct s3 is not null), count_if(distinct s4 is not null), count_if(distinct s5 is not null), count_if(distinct s6 is not null), count_if(distinct s7 is not null), count_if(distinct s8 is not null), count_if(distinct s9 is not null), count_if(distinct s10 is not null) "
            + "from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    retArray =
        new String[] {
          "chaoyang,0,1,1,1,1,1,1,1,1,1,",
          "haidian,0,1,1,1,1,1,1,1,1,1,",
          "huangpu,0,1,1,1,1,1,1,1,1,1,",
          "pudong,0,1,1,1,1,1,1,1,1,1,"
        };
    tableResultSetEqualTest(
        "select region, count_if(distinct s1 < 0), count_if(distinct s2 is not null), count_if(distinct s3 is not null), count_if(distinct s4 is not null), count_if(distinct s5 is not null), count_if(distinct s6 is not null), count_if(distinct s7 is not null), count_if(distinct s8 is not null), count_if(distinct s9 is not null), count_if(distinct s10 is not null) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void avgDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader = new String[] {"_col0", "_col1", "_col2", "_col3"};
    String[] retArray =
        new String[] {
          "40.4,40400.0,39.4,42.4,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select avg(distinct s1), avg(distinct s2), avg(distinct s3), avg(distinct s4) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "_col3", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,40.4,40400.0,39.4,42.4,",
          "beijing,beijing,haidian,40.4,40400.0,39.4,42.4,",
          "shanghai,shanghai,huangpu,40.4,40400.0,39.4,42.4,",
          "shanghai,shanghai,pudong,40.4,40400.0,39.4,42.4,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, avg(distinct s1), avg(distinct s2), avg(distinct s3), avg(distinct s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader = new String[] {"region", "_col1", "_col2", "_col3", "_col4"};
    retArray =
        new String[] {
          "chaoyang,40.4,40400.0,39.4,42.4,",
          "haidian,40.4,40400.0,39.4,42.4,",
          "huangpu,40.4,40400.0,39.4,42.4,",
          "pudong,40.4,40400.0,39.4,42.4,"
        };
    tableResultSetEqualTest(
        "select region, avg(distinct s1), avg(distinct s2), avg(distinct s3), avg(distinct s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void sumDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader = new String[] {"_col0", "_col1", "_col2", "_col3"};
    String[] retArray =
        new String[] {
          "202.0,202000.0,197.0,212.0,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select sum(distinct s1), sum(distinct s2), sum(distinct s3), sum(distinct s4) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "_col3", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,202.0,202000.0,197.0,212.0,",
          "beijing,beijing,haidian,202.0,202000.0,197.0,212.0,",
          "shanghai,shanghai,huangpu,202.0,202000.0,197.0,212.0,",
          "shanghai,shanghai,pudong,202.0,202000.0,197.0,212.0,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, sum(distinct s1), sum(distinct s2), sum(distinct s3), sum(distinct s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader = new String[] {"region", "_col1", "_col2", "_col3", "_col4"};
    retArray =
        new String[] {
          "chaoyang,202.0,202000.0,197.0,212.0,",
          "haidian,202.0,202000.0,197.0,212.0,",
          "huangpu,202.0,202000.0,197.0,212.0,",
          "pudong,202.0,202000.0,197.0,212.0,"
        };
    tableResultSetEqualTest(
        "select region, sum(distinct s1), sum(distinct s2), sum(distinct s3), sum(distinct s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void minDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11", "_col12", "_col13"
        };
    String[] retArray =
        new String[] {
          "beijing,beijing,chaoyang,d01,30,31000,30.0,35.0,false,beijing_chaoyang_red_A_d09_30,beijing_chaoyang_red_A_d09_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select min(distinct province), min(distinct city), min(distinct region), min(distinct device_id), min(distinct s1), min(distinct s2), min(distinct s3), min(distinct s4), min(distinct s5), min(distinct s6), min(distinct s7), min(distinct s8), min(distinct s9), min(distinct s10) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,30,31000,30.0,35.0,false,beijing_chaoyang_red_A_d09_30,beijing_chaoyang_red_A_d09_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "beijing,beijing,haidian,d13,30,31000,30.0,35.0,false,beijing_haidian_red_A_d13_30,beijing_haidian_red_A_d13_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d01,30,31000,30.0,35.0,false,shanghai_huangpu_red_A_d01_30,shanghai_huangpu_red_A_d01_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d05,30,31000,30.0,35.0,false,shanghai_pudong_red_A_d05_30,shanghai_pudong_red_A_d05_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, min(distinct device_id), min(distinct s1), min(distinct s2), min(distinct s3), min(distinct s4), min(distinct s5), min(distinct s6), min(distinct s7), min(distinct s8), min(distinct s9), min(distinct s10)  from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11"
        };
    retArray =
        new String[] {
          "chaoyang,d09,30,31000,30.0,35.0,false,beijing_chaoyang_red_A_d09_30,beijing_chaoyang_red_A_d09_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "haidian,d13,30,31000,30.0,35.0,false,beijing_haidian_red_A_d13_30,beijing_haidian_red_A_d13_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "huangpu,d01,30,31000,30.0,35.0,false,shanghai_huangpu_red_A_d01_30,shanghai_huangpu_red_A_d01_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,",
          "pudong,d05,30,31000,30.0,35.0,false,shanghai_pudong_red_A_d05_30,shanghai_pudong_red_A_d05_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select region, min(distinct device_id), min(distinct s1), min(distinct s2), min(distinct s3), min(distinct s4), min(distinct s5), min(distinct s6), min(distinct s7), min(distinct s8), min(distinct s9), min(distinct s10)  from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void minByDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7"};
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
        };
    tableResultSetEqualTest(
        "select min_by(distinct time, s1), min_by(distinct time, s2), min_by(distinct time, s3), min_by(distinct time, s4), min_by(distinct time, s5), min_by(distinct time, s6), min_by(distinct time, s9), min_by(distinct time, s10) "
            + "from table1 where device_id='d11'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device_id", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8"
        };
    retArray =
        new String[] {
          "d03,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
          "d11,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,"
        };

    // test GroupByDistinctAccumulator
    tableResultSetEqualTest(
        "select device_id, min_by(distinct time, s1), min_by(distinct time, s2), min_by(distinct time, s3), min_by(distinct time, s4), min_by(distinct time, s5), min_by(distinct time, s6), min_by(distinct time, s9), min_by(distinct time, s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void maxDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11", "_col12", "_col13"
        };
    String[] retArray =
        new String[] {
          "shanghai,shanghai,pudong,d16,55,50000,51.0,55.0,true,shanghai_pudong_yellow_B_d08_55,shanghai_pudong_yellow_B_d08_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select max(distinct province), max(distinct city), max(distinct region), max(distinct device_id), max(distinct s1), max(distinct s2), max(distinct s3), max(distinct s4), max(distinct s5), max(distinct s6), max(distinct s7), max(distinct s8), max(distinct s9), max(distinct s10) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12",
          "_col13"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d12,55,50000,51.0,55.0,true,beijing_chaoyang_yellow_B_d12_55,beijing_chaoyang_yellow_B_d12_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "beijing,beijing,haidian,d16,55,50000,51.0,55.0,true,beijing_haidian_yellow_B_d16_55,beijing_haidian_yellow_B_d16_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,d04,55,50000,51.0,55.0,true,shanghai_huangpu_yellow_B_d04_55,shanghai_huangpu_yellow_B_d04_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "shanghai,shanghai,pudong,d08,55,50000,51.0,55.0,true,shanghai_pudong_yellow_B_d08_55,shanghai_pudong_yellow_B_d08_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region,max(distinct device_id), max(distinct s1), max(distinct s2), max(distinct s3), max(distinct s4), max(distinct s5), max(distinct s6), max(distinct s7), max(distinct s8), max(distinct s9), max(distinct s10)  from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11"
        };
    retArray =
        new String[] {
          "chaoyang,d12,55,50000,51.0,55.0,true,beijing_chaoyang_yellow_B_d12_55,beijing_chaoyang_yellow_B_d12_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "haidian,d16,55,50000,51.0,55.0,true,beijing_haidian_yellow_B_d16_55,beijing_haidian_yellow_B_d16_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "huangpu,d04,55,50000,51.0,55.0,true,shanghai_huangpu_yellow_B_d04_55,shanghai_huangpu_yellow_B_d04_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,",
          "pudong,d08,55,50000,51.0,55.0,true,shanghai_pudong_yellow_B_d08_55,shanghai_pudong_yellow_B_d08_30,0xcafebabe55,2024-09-24T06:15:55.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select region,max(distinct device_id), max(distinct s1), max(distinct s2), max(distinct s3), max(distinct s4), max(distinct s5), max(distinct s6), max(distinct s7), max(distinct s8), max(distinct s9), max(distinct s10) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void maxByDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {"_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7"};
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
        };
    tableResultSetEqualTest(
        "select max_by(distinct time, s1), max_by(distinct time, s2), max_by(distinct time, s3), max_by(distinct time, s4), max_by(distinct time, s5), max_by(distinct time, s6), max_by(distinct time, s9), max_by(distinct time, s10) "
            + "from table1 where device_id='d11'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "device_id", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8"
        };
    retArray =
        new String[] {
          "d03,2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
          "d11,2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,"
        };

    // test GroupByDistinctAccumulator
    tableResultSetEqualTest(
        "select device_id, max_by(distinct time, s1), max_by(distinct time, s2), max_by(distinct time, s3), max_by(distinct time, s4), max_by(distinct time, s5), max_by(distinct time, s6), max_by(distinct time, s9), max_by(distinct time, s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void firstDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9"
        };
    String[] retArray =
        new String[] {
          "36,31000,41.0,36.0,false,beijing_chaoyang_yellow_A_d11_41,beijing_chaoyang_yellow_A_d11_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select first(distinct s1), first(distinct s2), first(distinct s3), first(distinct s4), first(distinct s5), first(distinct s6), first(distinct s7), first(distinct s8), first(distinct s9), first(distinct s10) "
            + "from table1 where device_id='d11'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,36,31000,41.0,36.0,false,beijing_chaoyang_yellow_A_d11_41,beijing_chaoyang_yellow_A_d11_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,36,31000,41.0,36.0,false,shanghai_huangpu_yellow_A_d03_41,shanghai_huangpu_yellow_A_d03_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, first(distinct s1), first(distinct s2), first(distinct s3), first(distinct s4), first(distinct s5), first(distinct s6), first(distinct s7), first(distinct s8), first(distinct s9), first(distinct s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    retArray =
        new String[] {
          "chaoyang,36,31000,41.0,36.0,false,beijing_chaoyang_yellow_A_d11_41,beijing_chaoyang_yellow_A_d11_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,",
          "huangpu,36,31000,41.0,36.0,false,shanghai_huangpu_yellow_A_d03_41,shanghai_huangpu_yellow_A_d03_36,0xcafebabe31,2024-09-24T06:15:31.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select region, first(distinct s1), first(distinct s2), first(distinct s3), first(distinct s4), first(distinct s5), first(distinct s6), first(distinct s7), first(distinct s8), first(distinct s9), first(distinct s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void firstByDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9"
        };
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select first_by(distinct time, s1), first_by(distinct time, s2), first_by(distinct time, s3), first_by(distinct time, s4), first_by(distinct time, s5), first_by(distinct time, s6), first_by(distinct time, s7), first_by(distinct time, s8), first_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 where device_id='d11'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
          "shanghai,shanghai,huangpu,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, first_by(distinct time, s1), first_by(distinct time, s2), first_by(distinct time, s3), first_by(distinct time, s4), first_by(distinct time, s5), first_by(distinct time, s6), first_by(distinct time, s7), first_by(distinct time, s8), first_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    retArray =
        new String[] {
          "chaoyang,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
          "huangpu,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:36.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:31.000Z,2024-09-24T06:15:36.000Z,",
        };
    tableResultSetEqualTest(
        "select region, first_by(distinct time, s1), first_by(distinct time, s2), first_by(distinct time, s3), first_by(distinct time, s4), first_by(distinct time, s5), first_by(distinct time, s6), first_by(distinct time, s7), first_by(distinct time, s8), first_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9"
        };
    String[] retArray =
        new String[] {
          "41,46000,51.0,46.0,false,beijing_chaoyang_yellow_A_d11_51,beijing_chaoyang_yellow_A_d11_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select last(distinct s1), last(distinct s2), last(distinct s3), last(distinct s4), last(distinct s5), last(distinct s6), last(distinct s7), last(distinct s8), last(distinct s9), last(distinct s10) "
            + "from table1 where device_id='d11'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,41,46000,51.0,46.0,false,beijing_chaoyang_yellow_A_d11_51,beijing_chaoyang_yellow_A_d11_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
          "shanghai,shanghai,huangpu,41,46000,51.0,46.0,false,shanghai_huangpu_yellow_A_d03_51,shanghai_huangpu_yellow_A_d03_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, last(distinct s1), last(distinct s2), last(distinct s3), last(distinct s4), last(distinct s5), last(distinct s6), last(distinct s7), last(distinct s8), last(distinct s9), last(distinct s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    retArray =
        new String[] {
          "chaoyang,41,46000,51.0,46.0,false,beijing_chaoyang_yellow_A_d11_51,beijing_chaoyang_yellow_A_d11_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,",
          "huangpu,41,46000,51.0,46.0,false,shanghai_huangpu_yellow_A_d03_51,shanghai_huangpu_yellow_A_d03_46,0xcafebabe41,2024-09-24T06:15:51.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select region, last(distinct s1), last(distinct s2), last(distinct s3), last(distinct s4), last(distinct s5), last(distinct s6), last(distinct s7), last(distinct s8), last(distinct s9), last(distinct s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastByDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader =
        new String[] {
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9"
        };
    String[] retArray =
        new String[] {
          "2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select last_by(distinct time, s1), last_by(distinct time, s2), last_by(distinct time, s3), last_by(distinct time, s4), last_by(distinct time, s5), last_by(distinct time, s6), last_by(distinct time, s7), last_by(distinct time, s8), last_by(distinct time, s9), last_by(distinct time, s10) "
            + "from table1 where device_id='d11'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "province",
          "city",
          "region",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11",
          "_col12"
        };
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
          "shanghai,shanghai,huangpu,2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, last_by(distinct time, s1), last_by(distinct time, s2), last_by(distinct time, s3), last_by(distinct time, s4), last_by(distinct time, s5), last_by(distinct time, s6), last_by(distinct time, s7), last_by(distinct time, s8), last_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader =
        new String[] {
          "region", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10"
        };
    retArray =
        new String[] {
          "chaoyang,2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
          "huangpu,2024-09-24T06:15:41.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:46.000Z,2024-09-24T06:15:41.000Z,2024-09-24T06:15:51.000Z,2024-09-24T06:15:36.000Z,",
        };
    tableResultSetEqualTest(
        "select region, last_by(distinct time, s1), last_by(distinct time, s2), last_by(distinct time, s3), last_by(distinct time, s4), last_by(distinct time, s5), last_by(distinct time, s6), last_by(distinct time, s7), last_by(distinct time, s8), last_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 where device_id='d11' or device_id='d03' group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void extremeDistinctTest() {
    // test MarkDistinct
    String[] expectedHeader = new String[] {"_col0", "_col1", "_col2", "_col3"};
    String[] retArray =
        new String[] {
          "55,50000,51.0,55.0,",
        };
    // global Aggregation
    tableResultSetEqualTest(
        "select extreme(distinct s1), extreme(distinct s2), extreme(distinct s3), extreme(distinct s4) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "_col3", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,55,50000,51.0,55.0,",
          "beijing,beijing,haidian,55,50000,51.0,55.0,",
          "shanghai,shanghai,huangpu,55,50000,51.0,55.0,",
          "shanghai,shanghai,pudong,55,50000,51.0,55.0,"
        };
    // group by Aggregation
    tableResultSetEqualTest(
        "select province,city,region, extreme(distinct s1), extreme(distinct s2), extreme(distinct s3), extreme(distinct s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader = new String[] {"region", "_col1", "_col2", "_col3", "_col4"};
    retArray =
        new String[] {
          "chaoyang,55,50000,51.0,55.0,",
          "haidian,55,50000,51.0,55.0,",
          "huangpu,55,50000,51.0,55.0,",
          "pudong,55,50000,51.0,55.0,"
        };
    tableResultSetEqualTest(
        "select region, extreme(distinct s1), extreme(distinct s2), extreme(distinct s3), extreme(distinct s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void varianceDistinctTest() {
    // The addInput logic of all variance functions are the same, so test any one function is ok.
    String[] expectedHeader = new String[] {"_col0", "_col1", "_col2", "_col3"};
    String[] retArray =
        new String[] {
          "68.2,4.824E7,49.0,54.6,",
        };
    tableResultSetEqualTest(
        "select round(VAR_POP(distinct s1),1), round(VAR_POP(distinct s2),1), round(VAR_POP(distinct s3),1), round(VAR_POP(distinct s4),1) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {"province", "city", "region", "_col3", "_col4", "_col5", "_col6"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,68.2,4.824E7,49.0,54.6,",
          "beijing,beijing,haidian,68.2,4.824E7,49.0,54.6,",
          "shanghai,shanghai,huangpu,68.2,4.824E7,49.0,54.6,",
          "shanghai,shanghai,pudong,68.2,4.824E7,49.0,54.6,"
        };
    tableResultSetEqualTest(
        "select province,city,region, round(VAR_POP(distinct s1),1), round(VAR_POP(distinct s2),1), round(VAR_POP(distinct s3),1), round(VAR_POP(distinct s4),1) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test GroupByDistinctAccumulator
    expectedHeader = new String[] {"region", "_col1", "_col2", "_col3", "_col4"};
    retArray =
        new String[] {
          "chaoyang,68.2,4.824E7,49.0,54.6,",
          "haidian,68.2,4.824E7,49.0,54.6,",
          "huangpu,68.2,4.824E7,49.0,54.6,",
          "pudong,68.2,4.824E7,49.0,54.6,"
        };
    tableResultSetEqualTest(
        "select region, round(VAR_POP(distinct s1),1), round(VAR_POP(distinct s2),1), round(VAR_POP(distinct s3),1), round(VAR_POP(distinct s4),1) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void mixedTest() {
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray =
        new String[] {
          "32,5,",
        };

    tableResultSetEqualTest(
        "select count(s1), count(distinct s1) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "device_id", "_col4", "_col5"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,d09,3,3,",
          "beijing,beijing,chaoyang,d10,2,2,",
          "beijing,beijing,chaoyang,d11,2,2,",
          "beijing,beijing,chaoyang,d12,1,1,",
          "beijing,beijing,haidian,d13,3,3,",
          "beijing,beijing,haidian,d14,2,2,",
          "beijing,beijing,haidian,d15,2,2,",
          "beijing,beijing,haidian,d16,1,1,",
          "shanghai,shanghai,huangpu,d01,3,3,",
          "shanghai,shanghai,huangpu,d02,2,2,",
          "shanghai,shanghai,huangpu,d03,2,2,",
          "shanghai,shanghai,huangpu,d04,1,1,",
          "shanghai,shanghai,pudong,d05,3,3,",
          "shanghai,shanghai,pudong,d06,2,2,",
          "shanghai,shanghai,pudong,d07,2,2,",
          "shanghai,shanghai,pudong,d08,1,1,",
        };
    tableResultSetEqualTest(
        "select province,city,region,device_id,count(s1), count(distinct s1) from table1 group by 1,2,3,4 order by 1,2,3,4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void singleInputDistinctAggregationTest() {
    String[] expectedHeader = new String[] {"_col0", "_col1"};
    String[] retArray =
        new String[] {
          "5,40.4,",
        };

    tableResultSetEqualTest(
        "select count(distinct s1), avg(distinct s1) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"16,"};
    tableResultSetEqualTest(
        "select count(distinct device_id) from table1 group by date_bin(1d,time) order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "region", "_col3", "_col4"};
    retArray =
        new String[] {
          "beijing,beijing,chaoyang,5,40.4,",
          "beijing,beijing,haidian,5,40.4,",
          "shanghai,shanghai,huangpu,5,40.4,",
          "shanghai,shanghai,pudong,5,40.4,"
        };
    tableResultSetEqualTest(
        "select province,city,region,count(distinct s1), avg(distinct s1) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void exceptionTest2() {
    tableAssertTestFail(
        "select count(distinct *) from table1",
        "mismatched input '*'. Expecting: <expression>",
        DATABASE_NAME);

    String errMsg = TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": Unsupported expression: Row";
    tableAssertTestFail("select distinct (s1,s2) from table1", errMsg, DATABASE_NAME);

    tableAssertTestFail("select (s1,s2) from table1", errMsg, DATABASE_NAME);

    tableAssertTestFail("select * from table1 where (s1,s2) is not null", errMsg, DATABASE_NAME);
  }
}
