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
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Records changes to access permissions and security roles required by FMT_MSA_EXT.1. */
public final class SecurityAttributeModificationAuditContext {

  private static final SecurityAttributeModificationAuditContext EMPTY =
      new SecurityAttributeModificationAuditContext(null, -1, null, null, null, null, null);

  private final AuditLogWriter auditLogWriter;
  private final long userId;
  private final String username;
  private final String clientHostname;
  private final String database;
  private final String sql;
  private final SecurityAttributeTarget target;
  private final AtomicBoolean recorded = new AtomicBoolean(false);

  private SecurityAttributeModificationAuditContext(
      @Nullable AuditLogWriter auditLogWriter,
      long userId,
      @Nullable String username,
      @Nullable String clientHostname,
      @Nullable String database,
      @Nullable String sql,
      @Nullable SecurityAttributeTarget target) {
    this.auditLogWriter = auditLogWriter;
    this.userId = userId;
    this.username = username;
    this.clientHostname = clientHostname;
    this.database = database;
    this.sql = sql;
    this.target = target;
  }

  public static SecurityAttributeModificationAuditContext forTreeStatement(
      AuthorStatement statement, @Nullable SessionInfo sessionInfo, @Nullable String sql) {
    return forTreeStatement(statement, sessionInfo, sql, DNAuditLogger.getInstance()::log);
  }

  static SecurityAttributeModificationAuditContext forTreeStatement(
      AuthorStatement statement,
      @Nullable SessionInfo sessionInfo,
      @Nullable String sql,
      AuditLogWriter auditLogWriter) {
    return fromSessionInfo(
        sessionInfo, sql, getTreeSecurityAttributeTarget(statement), auditLogWriter);
  }

  public static SecurityAttributeModificationAuditContext forTableStatement(
      RelationalAuthorStatement statement,
      @Nullable SessionInfo sessionInfo,
      @Nullable String sql) {
    return forTableStatement(statement, sessionInfo, sql, DNAuditLogger.getInstance()::log);
  }

  static SecurityAttributeModificationAuditContext forTableStatement(
      RelationalAuthorStatement statement,
      @Nullable SessionInfo sessionInfo,
      @Nullable String sql,
      AuditLogWriter auditLogWriter) {
    return fromSessionInfo(
        sessionInfo, sql, getTableSecurityAttributeTarget(statement), auditLogWriter);
  }

  public static SecurityAttributeModificationAuditContext forTreeAuthorization(
      Statement statement, IAuditEntity auditEntity) {
    return forTreeAuthorization(statement, auditEntity, DNAuditLogger.getInstance()::log);
  }

  static SecurityAttributeModificationAuditContext forTreeAuthorization(
      Statement statement, IAuditEntity auditEntity, AuditLogWriter auditLogWriter) {
    return fromAuditEntity(auditEntity, getTreeSecurityAttributeTarget(statement), auditLogWriter);
  }

  public static SecurityAttributeModificationAuditContext forTableAuthorization(
      RelationalAuthorStatement statement, IAuditEntity auditEntity) {
    return forTableAuthorization(statement, auditEntity, DNAuditLogger.getInstance()::log);
  }

  static SecurityAttributeModificationAuditContext forTableAuthorization(
      RelationalAuthorStatement statement,
      IAuditEntity auditEntity,
      AuditLogWriter auditLogWriter) {
    return fromAuditEntity(auditEntity, getTableSecurityAttributeTarget(statement), auditLogWriter);
  }

  private static SecurityAttributeModificationAuditContext fromSessionInfo(
      @Nullable SessionInfo sessionInfo,
      @Nullable String sql,
      @Nullable SecurityAttributeTarget target,
      AuditLogWriter auditLogWriter) {
    if (sessionInfo == null || target == null) {
      return EMPTY;
    }
    return new SecurityAttributeModificationAuditContext(
        auditLogWriter,
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getCliHostname(),
        sessionInfo.getDatabaseName().orElse(null),
        sql,
        target);
  }

  private static SecurityAttributeModificationAuditContext fromAuditEntity(
      IAuditEntity auditEntity,
      @Nullable SecurityAttributeTarget target,
      AuditLogWriter auditLogWriter) {
    if (target == null) {
      return EMPTY;
    }
    return new SecurityAttributeModificationAuditContext(
        auditLogWriter,
        auditEntity.getUserId(),
        auditEntity.getUsername(),
        auditEntity.getCliHostname(),
        auditEntity.getDatabase(),
        auditEntity.getSqlString(),
        target);
  }

  public ListenableFuture<ConfigTaskResult> track(
      ListenableFuture<ConfigTaskResult> executionFuture) {
    if (target == null) {
      return executionFuture;
    }
    Futures.addCallback(
        executionFuture,
        new FutureCallback<ConfigTaskResult>() {
          @Override
          public void onSuccess(@Nullable ConfigTaskResult result) {
            record(toStatus(result));
          }

          @Override
          public void onFailure(Throwable throwable) {
            record(null);
          }
        },
        MoreExecutors.directExecutor());
    return executionFuture;
  }

  public void recordAuthorizationFailure(@Nullable TSStatus status) {
    if (!isSuccessful(status)) {
      record(status);
    }
  }

  public void record(@Nullable TSStatus status) {
    if (target == null || isRedirection(status) || !recorded.compareAndSet(false, true)) {
      return;
    }
    auditLogWriter.log(
        new AuditLogFields(
            userId,
            username,
            clientHostname,
            AuditEventType.MODIFY_SECURITY_ATTRIBUTE,
            AuditLogOperation.CONTROL,
            PrivilegeType.SECURITY,
            isSuccessful(status),
            database,
            sql),
        target::format);
  }

  @Nullable
  private static SecurityAttributeTarget getTreeSecurityAttributeTarget(Statement statement) {
    if (!(statement instanceof AuthorStatement)) {
      return null;
    }
    AuthorStatement authorStatement = (AuthorStatement) statement;
    switch (authorStatement.getAuthorType()) {
      case CREATE_ROLE:
      case DROP_ROLE:
      case GRANT_ROLE:
      case REVOKE_ROLE:
        return new SecurityAttributeTarget(null, authorStatement.getRoleName());
      case GRANT_USER:
      case REVOKE_USER:
        return new SecurityAttributeTarget(authorStatement.getUserName(), null);
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        return new SecurityAttributeTarget(
            authorStatement.getUserName(), authorStatement.getRoleName());
      default:
        return null;
    }
  }

  @Nullable
  private static SecurityAttributeTarget getTableSecurityAttributeTarget(
      RelationalAuthorStatement statement) {
    switch (statement.getAuthorType()) {
      case CREATE_ROLE:
      case DROP_ROLE:
      case GRANT_ROLE_ANY:
      case GRANT_ROLE_ALL:
      case GRANT_ROLE_DB:
      case GRANT_ROLE_TB:
      case GRANT_ROLE_SYS:
      case REVOKE_ROLE_ANY:
      case REVOKE_ROLE_ALL:
      case REVOKE_ROLE_DB:
      case REVOKE_ROLE_TB:
      case REVOKE_ROLE_SYS:
        return new SecurityAttributeTarget(null, statement.getRoleName());
      case GRANT_USER_ANY:
      case GRANT_USER_ALL:
      case GRANT_USER_DB:
      case GRANT_USER_TB:
      case GRANT_USER_SYS:
      case REVOKE_USER_ANY:
      case REVOKE_USER_ALL:
      case REVOKE_USER_DB:
      case REVOKE_USER_TB:
      case REVOKE_USER_SYS:
        return new SecurityAttributeTarget(statement.getUserName(), null);
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        return new SecurityAttributeTarget(statement.getUserName(), statement.getRoleName());
      default:
        return null;
    }
  }

  @Nullable
  private static TSStatus toStatus(@Nullable ConfigTaskResult result) {
    if (result == null) {
      return null;
    }
    if (result.getStatus() != null) {
      return result.getStatus();
    }
    return result.getStatusCode() == null ? null : RpcUtils.getStatus(result.getStatusCode());
  }

  private static boolean isSuccessful(@Nullable TSStatus status) {
    return status != null
        && (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
  }

  private static boolean isRedirection(@Nullable TSStatus status) {
    return status != null && status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode();
  }

  private static class SecurityAttributeTarget {

    private final String username;
    private final String roleName;

    private SecurityAttributeTarget(@Nullable String username, @Nullable String roleName) {
      this.username = username;
      this.roleName = roleName;
    }

    private String format() {
      if (username != null && roleName != null) {
        return String.format(
            DataNodeMiscMessages.LOG_USER_ARG_ROLE_ARG_422D48D3, username, roleName);
      }
      return username != null ? username : roleName;
    }
  }

  @FunctionalInterface
  interface AuditLogWriter {

    void log(IAuditEntity auditEntity, Supplier<String> logMessage);
  }
}
