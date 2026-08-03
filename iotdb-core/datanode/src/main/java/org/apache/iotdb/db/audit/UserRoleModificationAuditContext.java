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
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/** Ensures one user-role modification attempt produces at most one execution audit record. */
public final class UserRoleModificationAuditContext {

  private static final UserRoleModificationAuditContext EMPTY =
      new UserRoleModificationAuditContext(null);

  private final AuditLogWriter auditLogWriter;
  private final AtomicBoolean logged = new AtomicBoolean(false);

  private UserRoleModificationAuditContext(@Nullable AuditLogWriter auditLogWriter) {
    this.auditLogWriter = auditLogWriter;
  }

  public static UserRoleModificationAuditContext forTreeStatement(
      Statement statement, @Nullable SessionInfo sessionInfo, @Nullable String sql) {
    return forTreeStatement(
        statement,
        sessionInfo,
        status ->
            DNAuditLogger.getInstance()
                .logUserRoleModification(statement, sessionInfo, sql, status));
  }

  static UserRoleModificationAuditContext forTreeStatement(
      Statement statement, @Nullable SessionInfo sessionInfo, AuditLogWriter auditLogWriter) {
    return isUserRoleModification(statement) && sessionInfo != null
        ? new UserRoleModificationAuditContext(auditLogWriter)
        : EMPTY;
  }

  public static UserRoleModificationAuditContext forTableStatement(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      @Nullable SessionInfo sessionInfo,
      @Nullable String sql) {
    return forTableStatement(
        statement,
        sessionInfo,
        status ->
            DNAuditLogger.getInstance()
                .logUserRoleModification(statement, sessionInfo, sql, status));
  }

  static UserRoleModificationAuditContext forTableStatement(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      @Nullable SessionInfo sessionInfo,
      AuditLogWriter auditLogWriter) {
    return isUserRoleModification(statement) && sessionInfo != null
        ? new UserRoleModificationAuditContext(auditLogWriter)
        : EMPTY;
  }

  public void log(@Nullable TSStatus status) {
    if (auditLogWriter == null || isRedirected(status) || !logged.compareAndSet(false, true)) {
      return;
    }
    auditLogWriter.log(status);
  }

  boolean isEnabled() {
    return auditLogWriter != null;
  }

  private static boolean isUserRoleModification(Statement statement) {
    if (!(statement instanceof AuthorStatement)) {
      return false;
    }
    AuthorType type = ((AuthorStatement) statement).getAuthorType();
    return type == AuthorType.GRANT_USER_ROLE || type == AuthorType.REVOKE_USER_ROLE;
  }

  private static boolean isUserRoleModification(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement) {
    if (!(statement instanceof RelationalAuthorStatement)) {
      return false;
    }
    AuthorRType type = ((RelationalAuthorStatement) statement).getAuthorType();
    return type == AuthorRType.GRANT_USER_ROLE || type == AuthorRType.REVOKE_USER_ROLE;
  }

  private static boolean isRedirected(@Nullable TSStatus status) {
    return status != null && status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode();
  }

  @FunctionalInterface
  interface AuditLogWriter {

    void log(@Nullable TSStatus status);
  }
}
