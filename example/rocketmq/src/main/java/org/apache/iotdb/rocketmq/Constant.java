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
package org.apache.iotdb.rocketmq;

public class Constant {

  public static final String TOPIC = "RocketMQ-Test";
  public static final String PRODUCER_GROUP = "IoTDBConsumer";
  public static final String CONSUMER_GROUP = "IoTDBProducer";
  public static final String SERVER_ADDRESS = "localhost:9876";
  public static final String IOTDB_CONNECTION_HOST = "localhost";
  public static final int IOTDB_CONNECTION_PORT = 6667;
  public static final String IOTDB_CONNECTION_USER = "root";
  public static final String IOTDB_CONNECTION_PASSWORD = "root";
  public static final String[] STORAGE_GROUP = {"root.vehicle", "root.test"};
  public static final String ADDITIONAL_STORAGE_GROUP = "root.addition";
  public static final String INT32 = "INT32";
  public static final String PLAIN = "PLAIN";
  public static final String SNAPPY = "SNAPPY";
  public static final String[][] CREATE_TIMESERIES = {
    {"root.vehicle.d0.s0", INT32, PLAIN, SNAPPY},
    {"root.vehicle.d0.s1", "TEXT", PLAIN, SNAPPY},
    {"root.vehicle.d1.s2", "FLOAT", PLAIN, SNAPPY},
    {"root.vehicle.d1.s3", "BOOLEAN", PLAIN, SNAPPY},
    {"root.test.d0.s0", INT32, PLAIN, SNAPPY},
    {"root.test.d0.s1", "TEXT", PLAIN, SNAPPY},
    {"root.test.d1.s0", INT32, PLAIN, SNAPPY},
  };
  public static final String[] ALL_DATA = {
    "root.vehicle.d0,10,s0,INT32,100",
    "root.vehicle.d0,12,s0:s1,INT32:TEXT,101:'employeeId102'",
    "root.vehicle.d0,19,s1,TEXT,'employeeId103'",
    "root.vehicle.d1,11,s2,FLOAT,104.0",
    "root.vehicle.d1,15,s2:s3,FLOAT:BOOLEAN,105.0:true",
    "root.vehicle.d1,17,s3,BOOLEAN,false",
    "root.vehicle.d0,20,s0,INT32,1000",
    "root.vehicle.d0,22,s0:s1,INT32:TEXT,1001:'employeeId1002'",
    "root.vehicle.d0,29,s1,TEXT,'employeeId1003'",
    "root.vehicle.d1,21,s2,FLOAT,1004.0",
    "root.vehicle.d1,25,s2:s3,FLOAT:BOOLEAN,1005.0:true",
    "root.vehicle.d1,27,s3,BOOLEAN,true",
    "root.test.d0,10,s0,INT32,106",
    "root.test.d0,14,s0:s1,INT32:TEXT,107:'employeeId108'",
    "root.test.d0,16,s1,TEXT,'employeeId109'",
    "root.test.d1,1,s0,INT32,110",
    "root.test.d0,30,s0,INT32,1006",
    "root.test.d0,34,s0:s1,INT32:TEXT,1007:'employeeId1008'",
    "root.test.d0,36,s1,TEXT,'employeeId1090'",
    "root.test.d1,10,s0,INT32,1100",
  };
}
