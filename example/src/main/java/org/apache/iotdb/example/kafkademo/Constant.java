/**
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
package org.apache.iotdb.example.kafkademo;

import org.apache.iotdb.jdbc.Config;

/**
 * Created by litianan on 2019-02-24.
 */
public class Constant {

  public static final String TOPIC = "Kafka-Test";
  public static final int CONSUMER_THREAD_NUM = 5;
  public static final String IOTDB_CONNECTION_URL = Config.IOTDB_URL_PREFIX + "localhost:6667/";
  public static final String IOTDB_CONNECTION_USER = "root";
  public static final String IOTDB_CONNECTION_PASSWORD = "root";
  public static final String STORAGE_GROUP = "root.vehicle";
  public static final String[] ALL_TIMESERIES = {"root.vehicle.device.sensor1",
      "root.vehicle.device.sensor2", "root.vehicle.device.sensor3", "root.vehicle.device.sensor4"};
  public static final String[] ALL_DATA = {
      "sensor1,2017/10/24 19:30:00,60 61 62",
      "sensor2,2017/10/24 19:30:00,160 161 162",
      "sensor3,2017/10/24 19:30:00,260 261 262",
      "sensor4,2017/10/24 19:30:00,360 361 362",
      "sensor1,2017/10/24 19:31:00,81 81 82",
      "sensor2,2017/10/24 19:31:00,180 181 182",
      "sensor3,2017/10/24 19:31:00,280 281 282",
      "sensor4,2017/10/24 19:31:00,380 381 382",
      "sensor1,2017/10/24 19:32:00,50 51 52",
      "sensor2,2017/10/24 19:32:00,150 151 152",
      "sensor3,2017/10/24 19:32:00,250 251 252",
      "sensor4,2017/10/24 19:32:00,350 351 352",
      "sensor1,2017/10/24 19:33:00,40 41 42",
      "sensor2,2017/10/24 19:33:00,140 141 142",
      "sensor3,2017/10/24 19:33:00,240 241 242",
      "sensor4,2017/10/24 19:33:00,340 341 342",
      "sensor1,2017/10/24 19:34:00,10 11 12",
      "sensor2,2017/10/24 19:34:00,110 111 112",
      "sensor3,2017/10/24 19:34:00,210 211 212",
      "sensor4,2017/10/24 19:34:00,310 311 312",
  };
}
