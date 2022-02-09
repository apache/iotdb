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
package org.apache.iotdb.pulsar;

@SuppressWarnings("squid:S2068")
public class Constant {
  private Constant() {}

  public static final String TOPIC_NAME = "persistent://public/default/regions-partitioned";
  public static final String IOTDB_CONNECTION_URL = "jdbc:iotdb://localhost:6667/";
  public static final String IOTDB_CONNECTION_USER = "root";
  public static final String IOTDB_CONNECTION_PASSWORD = "root";
  public static final String STORAGE_GROUP = "root.vehicle";
}
