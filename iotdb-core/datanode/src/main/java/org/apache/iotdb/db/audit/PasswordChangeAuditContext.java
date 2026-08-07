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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Holds the security-safe audit fields for one password-change statement. */
public final class PasswordChangeAuditContext {

  private static final PasswordChangeAuditContext EMPTY =
      new PasswordChangeAuditContext(null, -1, null, null, null, null);

  private final AuditLogWriter auditLogWriter;
  private final long userId;
  private final String username;
  private final String clientHostname;
  private final String database;
  private final String targetUsername;
  private final AtomicBoolean logged = new AtomicBoolean(false);

  private PasswordChangeAuditContext(
      AuditLogWriter auditLogWriter,
      long userId,
      String username,
      String clientHostname,
      String database,
      String targetUsername) {
    this.auditLogWriter = auditLogWriter;
    this.userId = userId;
    this.username = username;
    this.clientHostname = clientHostname;
    this.database = database;
    this.targetUsername = targetUsername;
  }

  public static PasswordChangeAuditContext forTreeStatement(
      Statement statement, SessionInfo sessionInfo) {
    String targetUsername = getTreeTargetUsername(statement);
    return targetUsername == null
        ? EMPTY
        : fromSessionInfo(sessionInfo, targetUsername, DNAuditLogger.getInstance()::log);
  }

  static PasswordChangeAuditContext forTreeStatement(
      Statement statement, SessionInfo sessionInfo, AuditLogWriter auditLogWriter) {
    String targetUsername = getTreeTargetUsername(statement);
    return targetUsername == null
        ? EMPTY
        : fromSessionInfo(sessionInfo, targetUsername, auditLogWriter);
  }

  public static PasswordChangeAuditContext forTableStatement(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      SessionInfo sessionInfo) {
    String targetUsername = getTableTargetUsername(statement);
    return targetUsername == null
        ? EMPTY
        : fromSessionInfo(sessionInfo, targetUsername, DNAuditLogger.getInstance()::log);
  }

  static PasswordChangeAuditContext forTableStatement(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      SessionInfo sessionInfo,
      AuditLogWriter auditLogWriter) {
    String targetUsername = getTableTargetUsername(statement);
    return targetUsername == null
        ? EMPTY
        : fromSessionInfo(sessionInfo, targetUsername, auditLogWriter);
  }

  public static PasswordChangeAuditContext forTreeAuthorization(
      Statement statement, IAuditEntity auditEntity) {
    String targetUsername = getTreeTargetUsername(statement);
    return targetUsername == null
        ? EMPTY
        : fromAuditEntity(auditEntity, targetUsername, DNAuditLogger.getInstance()::log);
  }

  static PasswordChangeAuditContext forTreeAuthorization(
      Statement statement, IAuditEntity auditEntity, AuditLogWriter auditLogWriter) {
    String targetUsername = getTreeTargetUsername(statement);
    return targetUsername == null
        ? EMPTY
        : fromAuditEntity(auditEntity, targetUsername, auditLogWriter);
  }

  private static String getTreeTargetUsername(Statement statement) {
    return statement instanceof AuthorStatement authorStatement
            && authorStatement.getAuthorType() == AuthorType.UPDATE_USER
        ? authorStatement.getUserName()
        : null;
  }

  private static String getTableTargetUsername(
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement) {
    return statement instanceof RelationalAuthorStatement authorStatement
            && authorStatement.getAuthorType() == AuthorRType.UPDATE_USER
        ? authorStatement.getUserName()
        : null;
  }

  private static PasswordChangeAuditContext fromAuditEntity(
      IAuditEntity auditEntity, String targetUsername, AuditLogWriter auditLogWriter) {
    return new PasswordChangeAuditContext(
        auditLogWriter,
        auditEntity.getUserId(),
        auditEntity.getUsername(),
        auditEntity.getCliHostname(),
        auditEntity.getDatabase(),
        targetUsername);
  }

  private static PasswordChangeAuditContext fromSessionInfo(
      SessionInfo sessionInfo, String targetUsername, AuditLogWriter auditLogWriter) {
    return new PasswordChangeAuditContext(
        auditLogWriter,
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getCliHostname(),
        sessionInfo.getDatabaseName().orElse(null),
        targetUsername);
  }

  public void log(TSStatus status) {
    if (targetUsername == null || !logged.compareAndSet(false, true)) {
      return;
    }
    boolean result =
        status != null
            && (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    auditLogWriter.log(
        new AuditLogFields(
            userId,
            username,
            clientHostname,
            AuditEventType.MODIFY_PASSWD,
            AuditLogOperation.CONTROL,
            PrivilegeType.SECURITY,
            result,
            database,
            null),
        () -> targetUsername);
  }

  boolean isEnabled() {
    return targetUsername != null;
  }

  @FunctionalInterface
  interface AuditLogWriter {

    void log(IAuditEntity auditEntity, Supplier<String> logMessage);
  }
}
