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

package org.apache.iotdb.commons.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import javax.annotation.Nullable;

/**
 * Describes one attempt to transfer user data between physically separated parts of IoTDB. Payload
 * contents and exception messages must not be included in this event.
 */
public final class UserDataTransferAuditEvent {

  private final long timestamp;
  private final TEndPoint initiator;
  private final TEndPoint source;
  private final TEndPoint target;
  private final UserDataTransferProtectionMethod protectionMethod;
  private final boolean success;
  private final String error;

  public UserDataTransferAuditEvent(
      TEndPoint initiator,
      TEndPoint source,
      TEndPoint target,
      UserDataTransferProtectionMethod protectionMethod,
      boolean success,
      @Nullable String error) {
    this.timestamp = System.currentTimeMillis();
    this.initiator = initiator;
    this.source = source;
    this.target = target;
    this.protectionMethod = protectionMethod;
    this.success = success;
    this.error = error;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public TEndPoint getInitiator() {
    return initiator;
  }

  public TEndPoint getSource() {
    return source;
  }

  public TEndPoint getTarget() {
    return target;
  }

  public UserDataTransferProtectionMethod getProtectionMethod() {
    return protectionMethod;
  }

  public boolean isSuccess() {
    return success;
  }

  @Nullable
  public String getError() {
    return error;
  }
}
