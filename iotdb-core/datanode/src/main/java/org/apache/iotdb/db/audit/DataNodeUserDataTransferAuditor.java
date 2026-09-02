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

package org.apache.iotdb.db.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserDataTransferAuditEvent;
import org.apache.iotdb.commons.audit.UserDataTransferProtectionMethod;
import org.apache.iotdb.commons.audit.UserDataTransferType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import javax.annotation.Nullable;

public final class DataNodeUserDataTransferAuditor {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private DataNodeUserDataTransferAuditor() {}

  public static boolean isEnabled() {
    return COMMON_CONFIG.isEnableAuditLog();
  }

  public static void record(
      UserDataTransferType transferType,
      TEndPoint initiator,
      TEndPoint source,
      TEndPoint target,
      @Nullable String context,
      int attempt,
      boolean success,
      @Nullable String errorCode,
      @Nullable Throwable error) {
    if (!isEnabled()) {
      return;
    }
    DNAuditLogger.getInstance()
        .recordUserDataTransferAuditLog(
            new UserDataTransferAuditEvent(
                transferType,
                initiator,
                source,
                target,
                UserDataTransferProtectionMethod.fromTlsEnabled(
                    COMMON_CONFIG.isEnableInternalSSL()),
                COMMON_CONFIG.isEnableInternalSSL() ? COMMON_CONFIG.getSslProtocol() : null,
                context,
                attempt,
                success,
                errorCode,
                error));
  }
}
