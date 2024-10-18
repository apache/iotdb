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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableAggregationIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(province STRING ID, city STRING ID, region STRING ID, device_id STRING ID, color STRING ATTRIBUTE, type STRING ATTRIBUTE, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 TEXT MEASUREMENT, s7 STRING MEASUREMENT, s8 BLOB MEASUREMENT, s9 TIMESTAMP MEASUREMENT, s10 DATE MEASUREMENT)",
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
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

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

    expectedHeader = new String[] {"_col0"};
    retArray =
        new String[] {
          "64,",
        };
    tableResultSetEqualTest("select count(*) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void avgTest() {
    String[] expectedHeader = new String[] {"device_id", "color", "type", "_col3"};
    String[] retArray =
        new String[] {
          "d01,red,A,45.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, avg(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id, color, type",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "color", "type", "_col3"};
    retArray =
        new String[] {
          "d01,red,A,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, avg(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id, color, type",
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
    String[] expectedHeader = new String[] {"device_id", "color", "type", "_col3"};
    String[] retArray =
        new String[] {
          "d01,red,A,90.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, sum(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by device_id, color, type",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "color", "type", "_col3"};
    retArray =
        new String[] {
          "d01,red,A,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, sum(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by device_id, color, type",
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
    // TODO Beyyes
    tableResultSetEqualTest("select sum(s3) from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void minTest() {
    String[] expectedHeader =
        new String[] {"device_id", "color", "type", "_col3", "_col4", "_col5", "_col6"};
    String[] retArray =
        new String[] {
          "d01,red,A,2024-09-24T06:15:30.000Z,35.0,2024-09-24T06:15:30.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, min(time),min(s4), min(s9), min(s10) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "color", "type", "_col3", "_col4"};
    retArray =
        new String[] {
          "d01,red,A,2024-09-24T06:15:40.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id, color, type, min(time),min(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by color,type,device_id",
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
  public void maxTest() {
    String[] expectedHeader =
        new String[] {"device_id", "color", "type", "_col3", "_col4", "_col5", "_col6"};
    String[] retArray =
        new String[] {
          "d01,red,A,2024-09-24T06:15:55.000Z,55.0,2024-09-24T06:15:55.000Z,2024-09-24,",
        };
    tableResultSetEqualTest(
        "select device_id,color,type,max(time),max(s4),max(s9),max(s10) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' group by 1,2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id", "color", "type", "_col3", "_col4"};
    retArray =
        new String[] {
          "d01,red,A,2024-09-24T06:15:55.000Z,55.0,",
        };
    tableResultSetEqualTest(
        "select device_id,color,type,max(time),max(s4) from table1 where time >= 2024-09-24T06:15:30.000+00:00 and time <= 2024-09-24T06:15:59.999+00:00 and device_id = 'd01' and s1 >= 40 group by color,type,device_id",
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

  @Ignore
  @Test
  public void groupByAttributeTest() {

    String[] expectedHeader = new String[] {"color", "_col1"};
    String[] retArray =
        new String[] {
          "red,32,", "yellow,32,",
        };
    tableResultSetEqualTest(
        "select color, count(*) from table1 group by color order by color",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"color", "device_id"};
    retArray =
        new String[] {
          "read,d01,",
          "read,d05,",
          "read,d09,",
          "read,d13,",
          "read,d03,",
          "read,d07,",
          "read,d11,",
          "read,d15,",
        };
    tableResultSetEqualTest(
        "SELECT color, device_id FROM (SELECT date_bin(5s, time), color, device_id, avg(s4) as avg_s4 FROM table1 WHERE type='A' AND (time >= 2024-09-24T06:15:30.000+00:00 AND time <= 2024-09-24T06:15:59.999+00:00) GROUP BY 1,2,3) WHERE avg_s4 > 1.0 GROUP BY color, device_id HAVING count(*) >= 2 ORDER BY color, device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "city", "type", "_col3"};
    retArray = new String[] {};

    tableResultSetEqualTest(
        "SELECT date_bin(10s, five_seconds), city, type, sum(five_seconds_count) / 2 FROM (SELECT date_bin(5s, time) AS five_seconds, city, type, count(*) AS five_seconds_count FROM table1 WHERE (time >= 2024-09-24T06:15:30.000+00:00 AND time <= 2024-09-24T06:15:59.999+00:00) AND device_id IS NOT NULL GROUP BY 1, city, type, device_id HAVING avg(s1) > 1) GROUP BY 1, city, type order by 2,3,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Ignore
  @Test
  public void subQueryTest() {

    String[] expectedHeader = new String[] {"ts", "type", "color", "device_id", "current_s7"};
    String[] retArray = new String[] {};

    tableResultSetEqualTest(
        "SELECT ts, type, color, device_id, current_s7 FROM (SELECT type, color, device_id, last(time) as ts, last_by(s7,time) as current_s7 FROM table1 WHERE city='shanghai' GROUP BY type, color, device_id) WHERE strpos(current_s7, color) != 0 order by type, color, device_id, ts",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"color", "device_id"};
    retArray =
        new String[] {
          "read,d01,",
          "read,d05,",
          "read,d09,",
          "read,d13,",
          "read,d03,",
          "read,d07,",
          "read,d11,",
          "read,d15,",
        };
    tableResultSetEqualTest(
        "SELECT color, device_id FROM (SELECT date_bin(5s, time), color, device_id, avg(s4) as avg_s4 FROM table1 WHERE type='A' AND (time >= 2024-09-24T06:15:30.000+00:00 AND time <= 2024-09-24T06:15:59.999+00:00) GROUP BY 1,2,3) WHERE avg_s4 > 1.0 GROUP BY color, device_id HAVING count(*) >= 2 ORDER BY color, device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"_col0", "city", "type", "_col3"};
    retArray = new String[] {};

    tableResultSetEqualTest(
        "SELECT date_bin(10s, five_seconds), city, type, sum(five_seconds_count) / 2 FROM (SELECT date_bin(5s, time) AS five_seconds, city, type, count(*) AS five_seconds_count FROM table1 WHERE (time >= 2024-09-24T06:15:30.000+00:00 AND time <= 2024-09-24T06:15:59.999+00:00) AND device_id IS NOT NULL GROUP BY 1, city, type, device_id HAVING avg(s1) > 1) GROUP BY 1, city, type order by 2,3,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Ignore
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
  }
}
