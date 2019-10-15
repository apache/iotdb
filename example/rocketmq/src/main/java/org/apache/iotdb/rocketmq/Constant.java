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

import org.apache.iotdb.jdbc.Config;

public class Constant {

  public static final String TOPIC = "RocketMQ-Test";
  public static final String PRODUCER_GROUP = "IoTDBConsumer";
  public static final String CONSUMER_GROUP = "IoTDBProducer";
  public static final String SERVER_ADDRESS = "localhost:9876";
  public static final String IOTDB_CONNECTION_URL = Config.IOTDB_URL_PREFIX + "localhost:6667/";
  public static final String IOTDB_CONNECTION_USER = "root";
  public static final String IOTDB_CONNECTION_PASSWORD = "root";
  public static final String[] STORAGE_GROUP = {"root.vehicle", "root.test"};
  public static final String[] CREATE_TIMESERIES = {
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d1.s0 WITH DATATYPE=INT32, ENCODING=PLAIN"};
  public static final String[] ALL_DATA = {
      "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
      "insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
      "insert into root.vehicle.d1(timestamp,s2) values(11,104.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(15,105.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(17,false)",
      "insert into root.vehicle.d0(timestamp,s0) values(20,1000)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(22,1001,'1002')",
      "insert into root.vehicle.d0(timestamp,s1) values(29,'1003')",
      "insert into root.vehicle.d1(timestamp,s2) values(21,1004.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(25,1005.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(27,true)",
      "insert into root.test.d0(timestamp,s0) values(10,106)",
      "insert into root.test.d0(timestamp,s0,s1) values(14,107,'108')",
      "insert into root.test.d0(timestamp,s1) values(16,'109')",
      "insert into root.test.d1(timestamp,s0) values(1,110)",
      "insert into root.test.d0(timestamp,s0) values(30,1006)",
      "insert into root.test.d0(timestamp,s0,s1) values(34,1007,'1008')",
      "insert into root.test.d0(timestamp,s1) values(36,'1090')",
      "insert into root.test.d1(timestamp,s0) values(10,1100)"
  };
}
