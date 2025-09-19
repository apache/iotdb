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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import java.util.List;

public class AbstractAuditLogger {
  private static final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private static final List<AuditLogOperation> auditLogOperationList =
      config.getAuditableOperationType();

  private static final PrivilegeLevel auditablePrivilegeLevel = config.getAuditableOperationLevel();

  private static final String auditableOperationResult = config.getAuditableOperationResult();

  void log(AuditLogFields auditLogFields, String log) {
    // do nothing
  }

  public boolean checkBeforeLog(AuditLogFields auditLogFields) {
    String username = auditLogFields.getUsername();
    String address = auditLogFields.getCliHostname();
    AuditEventType type = auditLogFields.getAuditType();
    AuditLogOperation operation = auditLogFields.getOperationType();
    PrivilegeType privilegeType = auditLogFields.getPrivilegeType();
    PrivilegeLevel privilegeLevel = judgePrivilegeLevel(privilegeType);
    boolean result = auditLogFields.isResult();

    // to do: check whether this event should be logged.
    // if whitelist or blacklist is used, only ip on the whitelist or blacklist can be logged

    if (auditLogOperationList == null || !auditLogOperationList.contains(operation)) {
      return false;
    }
    if (auditablePrivilegeLevel == PrivilegeLevel.OBJECT
        && privilegeLevel == PrivilegeLevel.GLOBAL) {
      return false;
    }
    if (result && !auditableOperationResult.contains("SUCCESS")) {
      return false;
    }
    if (!result && !auditableOperationResult.contains("FAIL")) {
      return false;
    }
    return true;
  }

  public static PrivilegeLevel judgePrivilegeLevel(PrivilegeType type) {
    switch (type) {
      case READ_DATA:
      case DROP:
      case ALTER:
      case CREATE:
      case DELETE:
      case INSERT:
      case SELECT:
      case WRITE_DATA:
      case READ_SCHEMA:
      case WRITE_SCHEMA:
        return PrivilegeLevel.OBJECT;
      case USE_CQ:
      case USE_UDF:
      case USE_PIPE:
      case USE_MODEL:
      case MAINTAIN:
      case MANAGE_ROLE:
      case MANAGE_USER:
      case USE_TRIGGER:
      case MANAGE_DATABASE:
      case EXTEND_TEMPLATE:
      default:
        return PrivilegeLevel.GLOBAL;
    }
  }

  public static Boolean isLoginEvent(AuditEventType type) {
    switch (type) {
      case LOGIN:
      case LOGIN_FINAL:
      case MODIFY_PASSWD:
      case LOGIN_EXCEED_LIMIT:
      case LOGIN_FAILED_TRIES:
      case LOGIN_REJECT_IP:
      case LOGIN_FAIL_MAX_TIMES:
      case LOGIN_RESOURCE_RESTRICT:
        return true;
      default:
        return false;
    }
  }
}
