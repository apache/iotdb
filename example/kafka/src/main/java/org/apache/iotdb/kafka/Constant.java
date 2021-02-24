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
package org.apache.iotdb.kafka;

import org.apache.iotdb.jdbc.Config;

public class Constant {
  private Constant() {}

  public static final String TOPIC = "Kafka-Test";
  public static final int CONSUMER_THREAD_NUM = 5;
  public static final String IOTDB_CONNECTION_URL = Config.IOTDB_URL_PREFIX + "localhost:6667/";
  public static final String IOTDB_CONNECTION_USER = "root";
  public static final String IOTDB_CONNECTION_PASSWORD = "root";
  public static final String STORAGE_GROUP = "root.vehicle";

  /** If you write level3 as device, timeseries will not be created because device is the keyword */
  public static final String[] ALL_TIMESERIES = {
    "root.vehicle.deviceid.sensor1",
    "root.vehicle.deviceid.sensor2",
    "root.vehicle.deviceid.sensor3",
    "root.vehicle.deviceid.sensor4"
  };

  public static final String[] ALL_DATA = {
    "sensor1,2017/10/24 19:30:00,606162908",
    "sensor2,2017/10/24 19:30:00,160161162",
    "sensor3,2017/10/24 19:30:00,260261262",
    "sensor4,2017/10/24 19:30:00,360361362",
    "sensor1,2017/10/24 19:31:00,818182346",
    "sensor2,2017/10/24 19:31:00,180181182",
    "sensor3,2017/10/24 19:31:00,280281282",
    "sensor4,2017/10/24 19:31:00,380381382",
    "sensor1,2017/10/24 19:32:00,505152421",
    "sensor2,2017/10/24 19:32:00,150151152",
    "sensor3,2017/10/24 19:32:00,250251252",
    "sensor4,2017/10/24 19:32:00,350351352",
    "sensor1,2017/10/24 19:33:00,404142234",
    "sensor2,2017/10/24 19:33:00,140141142",
    "sensor3,2017/10/24 19:33:00,240241242",
    "sensor4,2017/10/24 19:33:00,340341342",
    "sensor1,2017/10/24 19:34:00,101112567",
    "sensor2,2017/10/24 19:34:00,110111112",
    "sensor3,2017/10/24 19:34:00,210211212",
    "sensor4,2017/10/24 19:34:00,310311312",
  };
}
