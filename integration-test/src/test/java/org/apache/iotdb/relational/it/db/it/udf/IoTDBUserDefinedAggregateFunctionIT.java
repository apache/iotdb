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

package org.apache.iotdb.relational.it.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBUserDefinedAggregateFunctionIT {
  private static final String DATABASE_NAME = "test";
  protected static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(province STRING TAG, city STRING TAG, region STRING TAG, device_id STRING TAG, color STRING ATTRIBUTE, type STRING ATTRIBUTE, s1 INT32 FIELD, s2 INT64 FIELD, s3 FLOAT FIELD, s4 DOUBLE FIELD, s5 BOOLEAN FIELD, s6 TEXT FIELD, s7 STRING FIELD, s8 BLOB FIELD, s9 TIMESTAMP FIELD, s10 DATE FIELD)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',30,30.0,'shanghai_huangpu_red_A_d01_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',35000,35.0,35.0,'shanghai_huangpu_red_A_d01_35','shanghai_huangpu_red_A_d01_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',40,40.0,true,'shanghai_huangpu_red_A_d01_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','huangpu','d02','red','BBBBBBBBBBBBBBBB',36,true,'shanghai_huangpu_red_B_d02_36','shanghai_huangpu_red_B_d02_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu','d02','red','BBBBBBBBBBBBBBBB',40,40.0,'shanghai_huangpu_red_B_d02_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','huangpu','d02','red','BBBBBBBBBBBBBBBB',50000,'shanghai_huangpu_red_B_d02_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',36,36.0,'shanghai_huangpu_yellow_A_d03_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',41,41.0,false,'shanghai_huangpu_yellow_A_d03_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',46000,46.0,'shanghai_huangpu_yellow_A_d03_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',51.0,'shanghai_huangpu_yellow_A_d03_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','huangpu','d04','yellow','BBBBBBBBBBBBBBBB',30.0,true,'shanghai_huangpu_yellow_B_d04_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu','d04','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','huangpu','d04','yellow','BBBBBBBBBBBBBBBB',55,55.0,'shanghai_huangpu_yellow_B_d04_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','pudong','d05','red','A',30,30.0,'shanghai_pudong_red_A_d05_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'shanghai','shanghai','pudong','d05','red','A',35000,35.0,35.0,'shanghai_pudong_red_A_d05_35','shanghai_pudong_red_A_d05_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong','d05','red','A',40,40.0,true,'shanghai_pudong_red_A_d05_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','pudong','d05','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','pudong','d05','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','pudong','d06','red','BBBBBBBBBBBBBBBB',36,true,'shanghai_pudong_red_B_d06_36','shanghai_pudong_red_B_d06_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong','d06','red','BBBBBBBBBBBBBBBB',40,40.0,'shanghai_pudong_red_B_d06_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','pudong','d06','red','BBBBBBBBBBBBBBBB',50000,'shanghai_pudong_red_B_d06_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',36,36.0,'shanghai_pudong_yellow_A_d07_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',41,41.0,false,'shanghai_pudong_yellow_A_d07_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',46000,46.0,'shanghai_pudong_yellow_A_d07_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',51.0,'shanghai_pudong_yellow_A_d07_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','pudong','d08','yellow','BBBBBBBBBBBBBBBB',30.0,true,'shanghai_pudong_yellow_B_d08_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong','d08','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','pudong','d08','yellow','BBBBBBBBBBBBBBBB',55,55.0,'shanghai_pudong_yellow_B_d08_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','chaoyang','d09','red','A',30,30.0,'beijing_chaoyang_red_A_d09_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'beijing','beijing','chaoyang','d09','red','A',35000,35.0,35.0,'beijing_chaoyang_red_A_d09_35','beijing_chaoyang_red_A_d09_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang','d09','red','A',40,40.0,true,'beijing_chaoyang_red_A_d09_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','chaoyang','d09','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','chaoyang','d09','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','chaoyang','d10','red','BBBBBBBBBBBBBBBB',36,true,'beijing_chaoyang_red_B_d10_36','beijing_chaoyang_red_B_d10_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang','d10','red','BBBBBBBBBBBBBBBB',40,40.0,'beijing_chaoyang_red_B_d10_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','chaoyang','d10','red','BBBBBBBBBBBBBBBB',50000,'beijing_chaoyang_red_B_d10_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',36,36.0,'beijing_chaoyang_yellow_A_d11_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',41,41.0,false,'beijing_chaoyang_yellow_A_d11_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',46000,46.0,'beijing_chaoyang_yellow_A_d11_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',51.0,'beijing_chaoyang_yellow_A_d11_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','chaoyang','d12','yellow','BBBBBBBBBBBBBBBB',30.0,true,'beijing_chaoyang_yellow_B_d12_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang','d12','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','chaoyang','d12','yellow','BBBBBBBBBBBBBBBB',55,55.0,'beijing_chaoyang_yellow_B_d12_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','haidian','d13','red','A',30,30.0,'beijing_haidian_red_A_d13_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'beijing','beijing','haidian','d13','red','A',35000,35.0,35.0,'beijing_haidian_red_A_d13_35','beijing_haidian_red_A_d13_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian','d13','red','A',40,40.0,true,'beijing_haidian_red_A_d13_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','haidian','d13','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','haidian','d13','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','haidian','d14','red','BBBBBBBBBBBBBBBB',36,true,'beijing_haidian_red_B_d14_36','beijing_haidian_red_B_d14_36',2024-09-24T06:15:36.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian','d14','red','BBBBBBBBBBBBBBBB',40,40.0,'beijing_haidian_red_B_d14_40',2024-09-24T06:15:40.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','haidian','d14','red','BBBBBBBBBBBBBBBB',50000,'beijing_haidian_red_B_d14_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'beijing','beijing','haidian','d15','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','haidian','d15','yellow','A',36,36.0,'beijing_haidian_yellow_A_d15_36',2024-09-24T06:15:36.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'beijing','beijing','haidian','d15','yellow','A',41,41.0,false,'beijing_haidian_yellow_A_d15_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'beijing','beijing','haidian','d15','yellow','A',46000,46.0,'beijing_haidian_yellow_A_d15_46',2024-09-24T06:15:46.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'beijing','beijing','haidian','d15','yellow','A',51.0,'beijing_haidian_yellow_A_d15_51',2024-09-24T06:15:51.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','haidian','d16','yellow','BBBBBBBBBBBBBBBB',30.0,true,'beijing_haidian_yellow_B_d16_30',2024-09-24T06:15:30.000+00:00,'2024-09-24')",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian','d16','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','haidian','d16','yellow','BBBBBBBBBBBBBBBB',55,55.0,'beijing_haidian_yellow_B_d16_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void dropFunction() {
    SQLFunctionUtils.dropAllUDF();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @Test
  public void testMyCount() {
    SQLFunctionUtils.createUDF(
        "my_count", "org.apache.iotdb.db.query.udf.example.relational.MyCount");
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray =
        new String[] {
          "5,",
        };
    tableResultSetEqualTest(
        "select my_count(time) from table1 where device_id = 'd01'",
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
        "select date_bin(5s, time), (date_bin(5s, time) + 5000) as end_time, device_id, my_count(time) from table1 where device_id = 'd01' group by 1,device_id",
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
        "select date_bin(5s, time),province,city,region,device_id, my_count(time) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
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
        "select date_bin(5s, time),province,city,region,device_id, my_count(s1), my_count(s2), my_count(s3), my_count(s4), my_count(s5), my_count(s6), my_count(s7), my_count(s8), my_count(s9), my_count(s10) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
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
        "select province,city,region,device_id, my_count(s1), my_count(s2), my_count(s3), my_count(s4), my_count(s5), my_count(s6), my_count(s7), my_count(s8), my_count(s9), my_count(s10) from table1 group by 1,2,3,4 order by 1,2,3,4",
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
        "select province,city,region,device_id,my_count(time) from table1 group by 1,2,3,4 order by 1,2,3,4",
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
        "select province,city,region,my_count(time) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2"};
    retArray =
        new String[] {
          "beijing,beijing,32,", "shanghai,shanghai,32,",
        };
    tableResultSetEqualTest(
        "select province,city,my_count(time) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,32,", "shanghai,32,",
        };
    tableResultSetEqualTest(
        "select province,my_count(time) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray =
        new String[] {
          "64,",
        };
    tableResultSetEqualTest(
        "select my_count(time) from table1", expectedHeader, retArray, DATABASE_NAME);
    // drop function and invoke
    dropFunction();
    tableAssertTestFail("select my_count(time) from table1", "Unknown function", DATABASE_NAME);
  }

  @Test
  public void testMyAvg() {
    SQLFunctionUtils.createUDF("my_avg", "org.apache.iotdb.db.query.udf.example.relational.MyAvg");
    String[] expectedHeader = new String[] {"device_id", "color", "type", "_col3"};
    String[] retArray =
        new String[] {
          "d01,red,A,45.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, my_avg(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id, color, type",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "color", "type", "_col3"};
    retArray =
        new String[] {
          "d01,red,A,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, my_avg(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id, color, type",
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
        "select date_bin(5s, time), device_id, my_avg(s3) from table1 where device_id = 'd01' group by 1, 2",
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
        "select date_bin(5s, time),province,city,region,device_id, my_avg(s4) from table1 group by 1,2,3,4,5 order by 2,3,4,5,1",
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
        "select province,city,region,device_id, my_avg(s2) from table1 group by 1,2,3,4 order by 1,2,3,4",
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
        "select province,city,region,my_avg(s4) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "city", "_col2"};
    retArray =
        new String[] {
          "beijing,beijing,44.5,", "shanghai,shanghai,44.5,",
        };
    tableResultSetEqualTest(
        "select province,city,my_avg(s4) from table1 group by 1,2 order by 1,2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"province", "_col1"};
    retArray =
        new String[] {
          "beijing,44.5,", "shanghai,44.5,",
        };
    tableResultSetEqualTest(
        "select province,my_avg(s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0"};
    retArray = new String[] {"44.5,"};
    tableResultSetEqualTest(
        "select my_avg(s4) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testIllegalInput() {
    SQLFunctionUtils.createUDF(
        "first_two_sum", "org.apache.iotdb.db.query.udf.example.relational.FirstTwoSum");
    tableAssertTestFail(
        "select first_two_sum(s1,s2) from table1 group by device_id",
        "FirstTwoSum should accept three column as input",
        DATABASE_NAME);
    tableAssertTestFail(
        "select first_two_sum(s1,s2,s3) from table1 group by device_id",
        "FirstTwoSum should accept TIMESTAMP as the third input",
        DATABASE_NAME);
    tableAssertTestFail(
        "select first_two_sum(s1,s10,time) from table1 group by device_id",
        "FirstTwoSum should accept INT32, INT64, FLOAT, DOUBLE as the first two inputs",
        DATABASE_NAME);
  }

  @Test
  public void testFirstTwoSum() {
    SQLFunctionUtils.createUDF(
        "first_two_sum", "org.apache.iotdb.db.query.udf.example.relational.FirstTwoSum");
    String[] expectedHeader = new String[] {"device_id", "sum"};
    String[] retArray =
        new String[] {
          "d01,35030.0,",
          "d02,50036.0,",
          "d03,31036.0,",
          "d04,40055.0,",
          "d05,35030.0,",
          "d06,50036.0,",
          "d07,31036.0,",
          "d08,40055.0,",
          "d09,35030.0,",
          "d10,50036.0,",
          "d11,31036.0,",
          "d12,40055.0,",
          "d13,35030.0,",
          "d14,50036.0,",
          "d15,31036.0,",
          "d16,40055.0,",
        };

    tableResultSetEqualTest(
        "select device_id, first_two_sum(s1, s2, time) as sum from table1 group by device_id order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select device_id, first_two_sum(s1, s2, s9) as sum from table1 group by device_id order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDistinct() {
    SQLFunctionUtils.createUDF(
        "my_count", "org.apache.iotdb.db.query.udf.example.relational.MyCount");
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
        "select my_count(distinct province), my_count(distinct city), my_count(distinct region), my_count(distinct device_id), my_count(distinct s1), my_count(distinct s2), my_count(distinct s3), my_count(distinct s4), my_count(distinct s5), my_count(distinct s6), my_count(distinct s7), my_count(distinct s8), my_count(distinct s9), my_count(distinct s10) from table1",
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
        "select province,city,region, my_count(distinct s1), my_count(distinct s2), my_count(distinct s3), my_count(distinct s4), my_count(distinct s5), my_count(distinct s6), my_count(distinct s7), my_count(distinct s8), my_count(distinct s9), my_count(distinct s10) from table1 group by 1,2,3 order by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
