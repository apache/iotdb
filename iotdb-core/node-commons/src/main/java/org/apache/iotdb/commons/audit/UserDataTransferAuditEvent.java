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
  private final UserDataTransferType transferType;
  private final TEndPoint initiator;
  private final TEndPoint source;
  private final TEndPoint target;
  private final UserDataTransferProtectionMethod protectionMethod;
  private final String protectionProtocol;
  private final String context;
  private final int attempt;
  private final boolean success;
  private final String errorCode;
  private final String errorType;

  public UserDataTransferAuditEvent(
      UserDataTransferType transferType,
      TEndPoint initiator,
      TEndPoint source,
      TEndPoint target,
      UserDataTransferProtectionMethod protectionMethod,
      @Nullable String protectionProtocol,
      @Nullable String context,
      int attempt,
      boolean success,
      @Nullable String errorCode,
      @Nullable Throwable error) {
    this.timestamp = System.currentTimeMillis();
    this.transferType = transferType;
    this.initiator = initiator;
    this.source = source;
    this.target = target;
    this.protectionMethod = protectionMethod;
    this.protectionProtocol = protectionProtocol;
    this.context = context;
    this.attempt = attempt;
    this.success = success;
    this.errorCode = errorCode;
    this.errorType = error == null ? null : error.getClass().getName();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public UserDataTransferType getTransferType() {
    return transferType;
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

  @Nullable
  public String getProtectionProtocol() {
    return protectionProtocol;
  }

  @Nullable
  public String getContext() {
    return context;
  }

  public int getAttempt() {
    return attempt;
  }

  public boolean isSuccess() {
    return success;
  }

  @Nullable
  public String getErrorCode() {
    return errorCode;
  }

  @Nullable
  public String getErrorType() {
    return errorType;
  }
}
