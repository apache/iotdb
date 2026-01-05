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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import java.util.function.Supplier;

public abstract class AbstractAuditLogger {
  public static final String OBJECT_AUTHENTICATION_AUDIT_STR =
      "User %s (ID=%d) requests authority on object %s with result %s";
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

  public abstract void log(IAuditEntity auditLogFields, Supplier<String> log);

  public boolean noNeedInsertAuditLog(IAuditEntity auditLogFields) {
    return true;
  }

  public void recordObjectAuthenticationAuditLog(
      final IAuditEntity auditEntity, final Supplier<String> auditObject) {
    log(
        auditEntity.setAuditEventType(AuditEventType.OBJECT_AUTHENTICATION),
        () ->
            String.format(
                OBJECT_AUTHENTICATION_AUDIT_STR,
                auditEntity.getUsername(),
                auditEntity.getUserId(),
                auditObject.get(),
                auditEntity.getResult()));
  }
}
