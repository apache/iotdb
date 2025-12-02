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

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class ConnectionInfo {
  private static final int dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private final long userId;
  private final String userName;
  private final long sessionId;
  private final long lastActiveTime;
  private final String clientAddress;

  public ConnectionInfo(
      long userId, String userName, long sessionId, long lastActiveTime, String clientAddress) {
    this.userId = userId;
    this.userName = userName;
    this.sessionId = sessionId;
    this.lastActiveTime = lastActiveTime;
    this.clientAddress = clientAddress;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public long getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }

  public long getSessionId() {
    return sessionId;
  }

  public long getLastActiveTime() {
    return lastActiveTime;
  }

  public String getClientAddress() {
    return clientAddress;
  }
}
