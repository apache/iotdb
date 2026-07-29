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
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

/** Tracks one role-grant statement and records an audit event only when it fails. */
public class GrantRoleFailureAuditContext {

  private final DNAuditLogger auditLogger;
  private final String sqlString;

  private IClientSession clientSession;
  private String targetUsername;

  public GrantRoleFailureAuditContext(String sqlString) {
    this(DNAuditLogger.getInstance(), sqlString);
  }

  GrantRoleFailureAuditContext(DNAuditLogger auditLogger, String sqlString) {
    this.auditLogger = auditLogger;
    this.sqlString = sqlString;
  }

  public void setClientSession(IClientSession clientSession) {
    this.clientSession = clientSession;
  }

  public void track(AuthorStatement statement) {
    if (statement.getAuthorType() == AuthorType.GRANT_USER_ROLE) {
      targetUsername = statement.getUserName();
    }
  }

  public void track(RelationalAuthorStatement statement) {
    if (statement.getAuthorType() == AuthorRType.GRANT_USER_ROLE) {
      targetUsername = statement.getUserName();
    }
  }

  public void log(TSStatus status) {
    if (targetUsername == null || clientSession == null || isSuccessfulOrRedirected(status)) {
      return;
    }
    auditLogger.log(
        new AuditLogFields(
            clientSession.getUserId(),
            clientSession.getUsername(),
            clientSession.getClientAddress(),
            AuditEventType.GRANT_ROLE_FAILED,
            AuditLogOperation.CONTROL,
            PrivilegeType.SECURITY,
            false,
            clientSession.getDatabaseName(),
            sqlString),
        () -> targetUsername);
  }

  private static boolean isSuccessfulOrRedirected(TSStatus status) {
    return status != null
        && (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
  }
}
