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

package org.apache.iotdb.db.engine.trigger.sink.mqtt;

import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;
import org.apache.iotdb.db.metadata.PartialPath;

public class MQTTConfiguration implements Configuration {

  private final String host;
  private final int port;

  private final String username;
  private final String password;

  private final PartialPath device;
  private final String[] measurements;

  public MQTTConfiguration(
      String host,
      int port,
      String username,
      String password,
      PartialPath device,
      String[] measurements) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.device = device;
    this.measurements = measurements;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public PartialPath getDevice() {
    return device;
  }
}
