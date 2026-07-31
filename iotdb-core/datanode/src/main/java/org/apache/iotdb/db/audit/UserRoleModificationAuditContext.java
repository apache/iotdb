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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.function.Supplier;

/** Tracks one user-role membership statement and logs its successful security changes. */
public class UserRoleModificationAuditContext {

  private final DNAuditLogger auditLogger;
  private final String sqlString;

  private Long userId;
  private String username;
  private String clientAddress;
  private String database;
  private String targetUsername;
  private String targetRoleName;

  public UserRoleModificationAuditContext(String sqlString) {
    this(DNAuditLogger.getInstance(), sqlString);
  }

  UserRoleModificationAuditContext(DNAuditLogger auditLogger, String sqlString) {
    this.auditLogger = auditLogger;
    this.sqlString = sqlString;
  }

  public void setSessionInfo(SessionInfo sessionInfo) {
    if (sessionInfo == null) {
      return;
    }
    setActor(
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getCliHostname(),
        sessionInfo.getDatabaseName().orElse(null));
  }

  private void setActor(long userId, String username, String clientAddress, String database) {
    this.userId = userId;
    this.username = username;
    this.clientAddress = clientAddress;
    this.database = database;
  }

  public void track(AuthorStatement statement) {
    if (statement.getAuthorType() == AuthorType.GRANT_USER_ROLE
        || statement.getAuthorType() == AuthorType.REVOKE_USER_ROLE) {
      targetUsername = statement.getUserName();
      targetRoleName = statement.getRoleName();
    }
  }

  public void track(RelationalAuthorStatement statement) {
    if (statement.getAuthorType() == AuthorRType.GRANT_USER_ROLE
        || statement.getAuthorType() == AuthorRType.REVOKE_USER_ROLE) {
      targetUsername = statement.getUserName();
      targetRoleName = statement.getRoleName();
    }
  }

  public void log(TSStatus status) {
    if (targetUsername == null || userId == null || !isSuccessful(status)) {
      return;
    }
    logEvent(
        AuditEventType.MODIFY_SECURITY_ATTRIBUTE,
        () ->
            String.format(
                DataNodeMiscMessages
                    .LOG_SECURITY_ATTRIBUTE_USER_ROLE_MEMBERSHIP_USER_ARG_ROLE_ARG_D6DC8233,
                targetUsername,
                targetRoleName));
    logEvent(
        AuditEventType.MODIFY_ROLE_MEMBERSHIP,
        () ->
            String.format(
                DataNodeMiscMessages.LOG_USER_ARG_ROLE_ARG_422D48D3,
                targetUsername,
                targetRoleName));
  }

  private void logEvent(AuditEventType eventType, Supplier<String> log) {
    auditLogger.log(
        new AuditLogFields(
            userId,
            username,
            clientAddress,
            eventType,
            AuditLogOperation.CONTROL,
            PrivilegeType.SECURITY,
            true,
            database,
            sqlString),
        log);
  }

  private static boolean isSuccessful(TSStatus status) {
    return status != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }
}
