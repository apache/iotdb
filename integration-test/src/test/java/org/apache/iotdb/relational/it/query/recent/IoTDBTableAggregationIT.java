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
        "CREATE TABLE table1(province STRING ID, city STRING ID, region STRING ID, device_id STRING ID, color STRING ATTRIBUTE, type STRING ATTRIBUTE, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 TEXT MEASUREMENT, s7 STRING MEASUREMENT, s8 BLOB MEASUREMENT, s9 TIMESTAMP MEASUREMENT, s10 DATE MEASUREMENT",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',30,30.0,'shanghai_huangpu_red_A_d01_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',35000,35.0,35.0,'shanghai_huangpu_red_A_d01_35','shanghai_huangpu_red_A_d01_35',2024-09-24T06:15:35.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',40,40.0,true,'shanghai_huangpu_red_A_d01_40',2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','huangpu','d01','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','huangpu','d02','red','BBBBBBBBBBBBBBBB',36,true,'shanghai_huangpu_red_B_d02_36','shanghai_huangpu_red_B_d02_36',2024-09-24T06:15:36.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu','d02','red','BBBBBBBBBBBBBBBB',40,40.0,'shanghai_huangpu_red_B_d02_40',2024-09-24T06:15:40.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','huangpu','d02','red','BBBBBBBBBBBBBBBB',50000,'shanghai_huangpu_red_B_d02_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',36,36.0,'shanghai_huangpu_yellow_A_d03_36',2024-09-24T06:15:36.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',41,41.0,false,'shanghai_huangpu_yellow_A_d03_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',46000,46.0,'shanghai_huangpu_yellow_A_d03_46',2024-09-24T06:15:46.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'shanghai','shanghai','huangpu','d03','yellow','A',51.0,'shanghai_huangpu_yellow_A_d03_51',2024-09-24T06:15:51.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','huangpu','d04','yellow','BBBBBBBBBBBBBBBB',30.0,true,'shanghai_huangpu_yellow_B_d04_30',2024-09-24T06:15:30.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','huangpu','d04','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','huangpu','d04','yellow','BBBBBBBBBBBBBBBB',55,55.0,'shanghai_huangpu_yellow_B_d04_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','pudong','d05','red','A',30,30.0,'shanghai_pudong_red_A_d05_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'shanghai','shanghai','pudong','d05','red','A',35000,35.0,35.0,'shanghai_pudong_red_A_d05_35','shanghai_pudong_red_A_d05_35',2024-09-24T06:15:35.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong','d05','red','A',40,40.0,true,'shanghai_pudong_red_A_d05_40',2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','pudong','d05','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','pudong','d05','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','pudong','d06','red','BBBBBBBBBBBBBBBB',36,true,'shanghai_pudong_red_B_d06_36','shanghai_pudong_red_B_d06_36',2024-09-24T06:15:36.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong','d06','red','BBBBBBBBBBBBBBBB',40,40.0,'shanghai_pudong_red_B_d06_40',2024-09-24T06:15:40.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'shanghai','shanghai','pudong','d06','red','BBBBBBBBBBBBBBBB',50000,'shanghai_pudong_red_B_d06_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',36,36.0,'shanghai_pudong_yellow_A_d07_36',2024-09-24T06:15:36.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',41,41.0,false,'shanghai_pudong_yellow_A_d07_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',46000,46.0,'shanghai_pudong_yellow_A_d07_46',2024-09-24T06:15:46.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'shanghai','shanghai','pudong','d07','yellow','A',51.0,'shanghai_pudong_yellow_A_d07_51',2024-09-24T06:15:51.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'shanghai','shanghai','pudong','d08','yellow','BBBBBBBBBBBBBBBB',30.0,true,'shanghai_pudong_yellow_B_d08_30',2024-09-24T06:15:30.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'shanghai','shanghai','pudong','d08','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'shanghai','shanghai','pudong','d08','yellow','BBBBBBBBBBBBBBBB',55,55.0,'shanghai_pudong_yellow_B_d08_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','chaoyang','d09','red','A',30,30.0,'beijing_chaoyang_red_A_d09_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'beijing','beijing','chaoyang','d09','red','A',35000,35.0,35.0,'beijing_chaoyang_red_A_d09_35','beijing_chaoyang_red_A_d09_35',2024-09-24T06:15:35.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang','d09','red','A',40,40.0,true,'beijing_chaoyang_red_A_d09_40',2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','chaoyang','d09','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','chaoyang','d09','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','chaoyang','d10','red','BBBBBBBBBBBBBBBB',36,true,'beijing_chaoyang_red_B_d10_36','beijing_chaoyang_red_B_d10_36',2024-09-24T06:15:36.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang','d10','red','BBBBBBBBBBBBBBBB',40,40.0,'beijing_chaoyang_red_B_d10_40',2024-09-24T06:15:40.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','chaoyang','d10','red','BBBBBBBBBBBBBBBB',50000,'beijing_chaoyang_red_B_d10_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',36,36.0,'beijing_chaoyang_yellow_A_d11_36',2024-09-24T06:15:36.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',41,41.0,false,'beijing_chaoyang_yellow_A_d11_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',46000,46.0,'beijing_chaoyang_yellow_A_d11_46',2024-09-24T06:15:46.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'beijing','beijing','chaoyang','d11','yellow','A',51.0,'beijing_chaoyang_yellow_A_d11_51',2024-09-24T06:15:51.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','chaoyang','d12','yellow','BBBBBBBBBBBBBBBB',30.0,true,'beijing_chaoyang_yellow_B_d12_30',2024-09-24T06:15:30.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','chaoyang','d12','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','chaoyang','d12','yellow','BBBBBBBBBBBBBBBB',55,55.0,'beijing_chaoyang_yellow_B_d12_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s6,s8,s9) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','haidian','d13','red','A',30,30.0,'beijing_haidian_red_A_d13_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s3,s4,s6,s7,s9,s10) values (2024-09-24T06:15:35.000+00:00,'beijing','beijing','haidian','d13','red','A',35000,35.0,35.0,'beijing_haidian_red_A_d13_35','beijing_haidian_red_A_d13_35',2024-09-24T06:15:35.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s7,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian','d13','red','A',40,40.0,true,'beijing_haidian_red_A_d13_40',2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s5,s9,s10) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','haidian','d13','red','A',50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','haidian','d13','red','A',55,55.0,X'cafebabe55',2024-09-24T06:15:55.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s5,s6,s7,s9) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','haidian','d14','red','BBBBBBBBBBBBBBBB',36,true,'beijing_haidian_red_B_d14_36','beijing_haidian_red_B_d14_36',2024-09-24T06:15:36.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian','d14','red','BBBBBBBBBBBBBBBB',40,40.0,'beijing_haidian_red_B_d14_40',2024-09-24T06:15:40.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s7,s8,s9) values (2024-09-24T06:15:50.000+00:00,'beijing','beijing','haidian','d14','red','BBBBBBBBBBBBBBBB',50000,'beijing_haidian_red_B_d14_50',X'cafebabe50',2024-09-24T06:15:50.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s8,s9) values (2024-09-24T06:15:31.000+00:00,'beijing','beijing','haidian','d15','yellow','A',31000,X'cafebabe31',2024-09-24T06:15:31.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s7,s9,s10) values (2024-09-24T06:15:36.000+00:00,'beijing','beijing','haidian','d15','yellow','A',36,36.0,'beijing_haidian_yellow_A_d15_36',2024-09-24T06:15:36.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s3,s5,s6,s8,s9) values (2024-09-24T06:15:41.000+00:00,'beijing','beijing','haidian','d15','yellow','A',41,41.0,false,'beijing_haidian_yellow_A_d15_41',X'cafebabe41',2024-09-24T06:15:41.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s4,s7,s9) values (2024-09-24T06:15:46.000+00:00,'beijing','beijing','haidian','d15','yellow','A',46000,46.0,'beijing_haidian_yellow_A_d15_46',2024-09-24T06:15:46.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s6,s9) values (2024-09-24T06:15:51.000+00:00,'beijing','beijing','haidian','d15','yellow','A',51.0,'beijing_haidian_yellow_A_d15_51',2024-09-24T06:15:51.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s3,s5,s7,s9,s10) values (2024-09-24T06:15:30.000+00:00,'beijing','beijing','haidian','d16','yellow','BBBBBBBBBBBBBBBB',30.0,true,'beijing_haidian_yellow_B_d16_30',2024-09-24T06:15:30.000+00:00,'2024-09-24'",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s2,s9) values (2024-09-24T06:15:40.000+00:00,'beijing','beijing','haidian','d16','yellow','BBBBBBBBBBBBBBBB',40000,2024-09-24T06:15:40.000+00:00",
        "INSERT INTO table1(time,province,city,region,device_id,color,type,s1,s4,s6,s8,s9) values (2024-09-24T06:15:55.000+00:00,'beijing','beijing','haidian','d16','yellow','BBBBBBBBBBBBBBBB',55,55.0,'beijing_haidian_yellow_B_d16_55',X'cafebabe55',2024-09-24T06:15:55.000+00:00",
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

  @Ignore
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
  }
}
