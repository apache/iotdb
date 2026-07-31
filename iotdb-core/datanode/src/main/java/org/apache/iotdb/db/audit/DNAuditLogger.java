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
import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import jakarta.validation.constraints.NotNull;

import javax.annotation.Nullable;

import java.util.function.Supplier;

public class DNAuditLogger extends AbstractAuditLogger {

  private Coordinator coordinator;

  private DNAuditLogger() {
    // Empty constructor
  }

  public static DNAuditLogger getInstance() {
    return DNAuditLoggerHolder.INSTANCE;
  }

  public void setCoordinator(Coordinator coordinator) {
    DNAuditLoggerHolder.INSTANCE.coordinator = coordinator;
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      IAuditEntity auditLogFields, String log, PartialPath logDevice) {
    return null;
  }

  public void createViewIfNecessary() {}

  @Override
  public synchronized void log(IAuditEntity auditLogFields, Supplier<String> log) {}

  public void logFromCN(AuditLogFields auditLogFields, String log, int nodeId)
      throws IllegalPathException {}

  public void logUserRoleModificationAuthorizationFailure(
      Statement statement, IAuditEntity auditEntity, @Nullable TSStatus status) {
    if (isSuccessful(status) || isRedirected(status)) {
      return;
    }
    logUserRoleModification(getUserRoleTarget(statement), auditEntity, status);
  }

  public void logUserRoleModification(
      Statement statement,
      SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable TSStatus status) {
    logUserRoleModification(getUserRoleTarget(statement), sessionInfo, sql, status);
  }

  public void logUserRoleModification(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable TSStatus status) {
    logUserRoleModification(getUserRoleTarget(statement), sessionInfo, sql, status);
  }

  private void logUserRoleModification(
      @Nullable UserRoleTarget target,
      @Nullable SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable TSStatus status) {
    if (target == null || sessionInfo == null || isRedirected(status)) {
      return;
    }
    logUserRoleModification(
        target,
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getCliHostname(),
        sessionInfo.getDatabaseName().orElse(null),
        sql,
        status);
  }

  private void logUserRoleModification(
      @Nullable UserRoleTarget target, IAuditEntity auditEntity, @Nullable TSStatus status) {
    if (target == null || isRedirected(status)) {
      return;
    }
    logUserRoleModification(
        target,
        auditEntity.getUserId(),
        auditEntity.getUsername(),
        auditEntity.getCliHostname(),
        auditEntity.getDatabase(),
        auditEntity.getSqlString(),
        status);
  }

  private void logUserRoleModification(
      UserRoleTarget target,
      long userId,
      String username,
      String clientAddress,
      @Nullable String database,
      @Nullable String sql,
      @Nullable TSStatus status) {
    log(
        new AuditLogFields(
            userId,
            username,
            clientAddress,
            AuditEventType.MODIFY_USER_ROLE,
            AuditLogOperation.CONTROL,
            PrivilegeType.SECURITY,
            isSuccessful(status),
            database,
            sql),
        () ->
            String.format(
                DataNodeMiscMessages.LOG_USER_ARG_ROLE_ARG_422D48D3,
                target.username,
                target.roleName));
  }

  @Nullable
  private static UserRoleTarget getUserRoleTarget(Statement statement) {
    if (!(statement instanceof AuthorStatement)) {
      return null;
    }
    AuthorStatement authorStatement = (AuthorStatement) statement;
    if (authorStatement.getAuthorType() != AuthorType.GRANT_USER_ROLE
        && authorStatement.getAuthorType() != AuthorType.REVOKE_USER_ROLE) {
      return null;
    }
    return new UserRoleTarget(authorStatement.getUserName(), authorStatement.getRoleName());
  }

  @Nullable
  private static UserRoleTarget getUserRoleTarget(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement) {
    if (!(statement instanceof RelationalAuthorStatement)) {
      return null;
    }
    RelationalAuthorStatement authorStatement = (RelationalAuthorStatement) statement;
    if (authorStatement.getAuthorType() != AuthorRType.GRANT_USER_ROLE
        && authorStatement.getAuthorType() != AuthorRType.REVOKE_USER_ROLE) {
      return null;
    }
    return new UserRoleTarget(authorStatement.getUserName(), authorStatement.getRoleName());
  }

  private static boolean isSuccessful(@Nullable TSStatus status) {
    return status != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static boolean isRedirected(@Nullable TSStatus status) {
    return status != null && status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode();
  }

  private static class UserRoleTarget {

    private final String username;
    private final String roleName;

    private UserRoleTarget(String username, String roleName) {
      this.username = username;
      this.roleName = roleName;
    }
  }

  private static class DNAuditLoggerHolder {

    private static final DNAuditLogger INSTANCE = new DNAuditLogger();

    private DNAuditLoggerHolder() {}
  }
}
