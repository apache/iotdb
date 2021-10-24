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
package org.apache.iotdb.influxdb.protocol.dto;

public class SessionPoint {
  private final String host;
  private final int rpcPort;
  private final String username;
  private final String password;

  public SessionPoint(String host, int rpcPort, String username, String password) {
    this.host = host;
    this.rpcPort = rpcPort;
    this.username = username;
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public boolean equals(SessionPoint sessionPoint) {
    if (host != null && host.equals(sessionPoint.getHost())) {
      if (rpcPort == sessionPoint.getRpcPort()) {
        if (username != null && username.equals(sessionPoint.getUsername())) {
          if (password != null && password.equals(sessionPoint.getPassword())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "SessionPoint{"
        + "host='"
        + host
        + '\''
        + ", rpcPort="
        + rpcPort
        + ", username='"
        + username
        + '\''
        + ", password='"
        + password
        + '\''
        + '}';
  }
}
