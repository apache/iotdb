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

public class AuditLogFields {
  private final String username;

  private final int userId;
  private final String cliHostname;
  private final AuditEventType auditType;
  private final AuditLogOperation operationType;
  private final PrivilegeType privilegeType;
  private final boolean result;
  private final String database;
  private final String sqlString;

  public AuditLogFields(
      String username,
      int userId,
      String cliHostname,
      AuditEventType auditType,
      AuditLogOperation operationType,
      PrivilegeType privilegeType,
      boolean result,
      String database,
      String sqlString) {
    this.username = username;
    this.userId = userId;
    this.cliHostname = cliHostname;
    this.auditType = auditType;
    this.operationType = operationType;
    this.privilegeType = privilegeType;
    this.result = result;
    this.database = database;
    this.sqlString = sqlString;
  }

  public String getUsername() {
    return username;
  }

  public int getUserId() {
    return userId;
  }

  public String getCliHostname() {
    return cliHostname;
  }

  public AuditEventType getAuditType() {
    return auditType;
  }

  public AuditLogOperation getOperationType() {
    return operationType;
  }

  public PrivilegeType getPrivilegeType() {
    return privilegeType;
  }

  public boolean isResult() {
    return result;
  }

  public String getDatabase() {
    return database;
  }

  public String getSqlString() {
    return sqlString;
  }
}
