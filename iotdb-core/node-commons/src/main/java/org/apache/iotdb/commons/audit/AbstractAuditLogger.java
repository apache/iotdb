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
import java.util.function.Supplier;

public abstract class AbstractAuditLogger {

  public static final String AUDIT_LOG_NODE_ID = "node_id";
  public static final String AUDIT_LOG_USER_ID = "user_id";
  public static final String AUDIT_LOG_USERNAME = "username";
  public static final String AUDIT_LOG_CLI_HOSTNAME = "cli_hostname";
  public static final String AUDIT_LOG_AUDIT_EVENT_TYPE = "audit_event_type";
  public static final String AUDIT_LOG_OPERATION_TYPE = "operation_type";
  public static final String AUDIT_LOG_PRIVILEGE_TYPE = "privilege_type";
  public static final String AUDIT_LOG_PRIVILEGE_LEVEL = "privilege_level";
  public static final String AUDIT_LOG_RESULT = "result";
  public static final String AUDIT_LOG_DATABASE = "database";
  public static final String AUDIT_LOG_SQL_STRING = "sql_string";
  public static final String AUDIT_LOG_LOG = "log";

  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();
  protected static final boolean IS_AUDIT_LOG_ENABLED = CONFIG.isEnableAuditLog();
  private static final List<AuditLogOperation> AUDITABLE_OPERATION_TYPE =
      CONFIG.getAuditableOperationType();
  private static final PrivilegeLevel AUDITABLE_OPERATION_LEVEL =
      CONFIG.getAuditableOperationLevel();
  private static final String AUDITABLE_OPERATION_RESULT = CONFIG.getAuditableOperationResult();

  public abstract void log(IAuditEntity auditLogFields, Supplier<String> log);

  public boolean noNeedInsertAuditLog(IAuditEntity auditLogFields) {
    AuditLogOperation operation = auditLogFields.getAuditLogOperation();
    boolean result = auditLogFields.getResult();

    // to do: check whether this event should be logged.
    // if whitelist or blacklist is used, only ip on the whitelist or blacklist can be logged

    if (AUDITABLE_OPERATION_TYPE == null || !AUDITABLE_OPERATION_TYPE.contains(operation)) {
      return true;
    }
    if (auditLogFields.getPrivilegeTypes() != null) {
      for (PrivilegeType privilegeType : auditLogFields.getPrivilegeTypes()) {
        PrivilegeLevel privilegeLevel = judgePrivilegeLevel(privilegeType);
        if (AUDITABLE_OPERATION_LEVEL == PrivilegeLevel.OBJECT
            && privilegeLevel == PrivilegeLevel.GLOBAL) {
          return true;
        }
      }
    }
    if (result && !AUDITABLE_OPERATION_RESULT.contains("SUCCESS")) {
      return true;
    }
    return !result && !AUDITABLE_OPERATION_RESULT.contains("FAIL");
  }

  public static PrivilegeLevel judgePrivilegeLevel(PrivilegeType type) {
    if (type == null) {
      return PrivilegeLevel.GLOBAL;
    }
    switch (type) {
      case READ_DATA:
      case DROP:
      case ALTER:
      case CREATE:
      case DELETE:
      case INSERT:
      case SELECT:
      case MANAGE_DATABASE:
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
