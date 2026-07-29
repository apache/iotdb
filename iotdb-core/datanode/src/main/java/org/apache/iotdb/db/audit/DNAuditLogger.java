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

  public void logRevokeFailure(
      Statement statement, IAuditEntity auditEntity, @Nullable TSStatus status) {
    logRevokeFailure(
        getTargetName(statement),
        auditEntity.getUserId(),
        auditEntity.getUsername(),
        auditEntity.getCliHostname(),
        auditEntity.getDatabase(),
        auditEntity.getSqlString(),
        status);
  }

  public void logRevokeFailure(
      Statement statement,
      SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable TSStatus status) {
    logRevokeFailure(getTargetName(statement), sessionInfo, sql, status);
  }

  public void logRevokeFailure(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable TSStatus status) {
    logRevokeFailure(getTargetName(statement), sessionInfo, sql, status);
  }

  private void logRevokeFailure(
      @Nullable String targetName,
      SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable TSStatus status) {
    logRevokeFailure(
        targetName,
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getCliHostname(),
        sessionInfo.getDatabaseName().orElse(null),
        sql,
        status);
  }

  private void logRevokeFailure(
      @Nullable String targetName,
      long userId,
      String username,
      String clientAddress,
      @Nullable String database,
      @Nullable String sql,
      @Nullable TSStatus status) {
    if (targetName == null || isSuccessful(status)) {
      return;
    }
    log(
        new AuditLogFields(
            userId,
            username,
            clientAddress,
            AuditEventType.REVOKE_FAILED,
            AuditLogOperation.CONTROL,
            PrivilegeType.SECURITY,
            false,
            database,
            sql),
        () -> targetName);
  }

  @Nullable
  private static String getTargetName(Statement statement) {
    if (!(statement instanceof AuthorStatement)) {
      return null;
    }
    AuthorStatement authorStatement = (AuthorStatement) statement;
    if (authorStatement.getAuthorType() == AuthorType.REVOKE_USER
        || authorStatement.getAuthorType() == AuthorType.REVOKE_USER_ROLE) {
      return authorStatement.getUserName();
    }
    if (authorStatement.getAuthorType() == AuthorType.REVOKE_ROLE) {
      return authorStatement.getRoleName();
    }
    return null;
  }

  @Nullable
  private static String getTargetName(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement) {
    if (!(statement instanceof RelationalAuthorStatement)) {
      return null;
    }
    RelationalAuthorStatement authorStatement = (RelationalAuthorStatement) statement;
    AuthorRType type = authorStatement.getAuthorType();
    if (type == AuthorRType.REVOKE_USER_ANY
        || type == AuthorRType.REVOKE_USER_ALL
        || type == AuthorRType.REVOKE_USER_DB
        || type == AuthorRType.REVOKE_USER_TB
        || type == AuthorRType.REVOKE_USER_SYS
        || type == AuthorRType.REVOKE_USER_ROLE) {
      return authorStatement.getUserName();
    }
    if (type == AuthorRType.REVOKE_ROLE_ANY
        || type == AuthorRType.REVOKE_ROLE_ALL
        || type == AuthorRType.REVOKE_ROLE_DB
        || type == AuthorRType.REVOKE_ROLE_TB
        || type == AuthorRType.REVOKE_ROLE_SYS) {
      return authorStatement.getRoleName();
    }
    return null;
  }

  private static boolean isSuccessful(@Nullable TSStatus status) {
    return status != null
        && (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
  }

  private static class DNAuditLoggerHolder {

    private static final DNAuditLogger INSTANCE = new DNAuditLogger();

    private DNAuditLoggerHolder() {}
  }
}
