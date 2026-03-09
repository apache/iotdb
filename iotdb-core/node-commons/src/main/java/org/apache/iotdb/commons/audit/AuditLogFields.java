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

import java.util.Collections;
import java.util.List;

public class AuditLogFields implements IAuditEntity {

  private final long userId;
  private final String username;
  private final String cliHostname;
  private final AuditEventType auditType;
  private final AuditLogOperation operationType;
  private final List<PrivilegeType> privilegeTypes;
  private final boolean result;
  private final String database;
  private final String sqlString;

  public AuditLogFields(
      long userId,
      String username,
      String cliHostname,
      AuditEventType auditType,
      AuditLogOperation operationType,
      List<PrivilegeType> privilegeTypes,
      boolean result,
      String database,
      String sqlString) {
    this.username = username;
    this.userId = userId;
    this.cliHostname = cliHostname;
    this.auditType = auditType;
    this.operationType = operationType;
    this.privilegeTypes = privilegeTypes;
    this.result = result;
    this.database = database;
    this.sqlString = sqlString;
  }

  public AuditLogFields(
      long userId,
      String username,
      String cliHostname,
      AuditEventType auditType,
      AuditLogOperation operationType,
      PrivilegeType privilegeType,
      boolean result,
      String database,
      String sqlString) {
    this(
        userId,
        username,
        cliHostname,
        auditType,
        operationType,
        Collections.singletonList(privilegeType),
        result,
        database,
        sqlString);
  }

  public AuditLogFields(
      long userId,
      String username,
      String cliHostname,
      AuditEventType auditEventType,
      AuditLogOperation operationType,
      boolean result) {
    this(
        userId,
        username,
        cliHostname,
        auditEventType,
        operationType,
        (List<PrivilegeType>) null,
        result,
        "",
        "");
  }

  public String getUsername() {
    return username;
  }

  public long getUserId() {
    return userId;
  }

  public String getCliHostname() {
    return cliHostname;
  }

  @Override
  public AuditEventType getAuditEventType() {
    return auditType;
  }

  @Override
  public AuditLogOperation getAuditLogOperation() {
    return operationType;
  }

  @Override
  public List<PrivilegeType> getPrivilegeTypes() {
    return privilegeTypes;
  }

  @Override
  public String getPrivilegeTypeString() {
    return privilegeTypes.toString();
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public String getSqlString() {
    return sqlString;
  }

  @Override
  public boolean getResult() {
    return result;
  }

  @Override
  public IAuditEntity setAuditEventType(AuditEventType auditEventType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IAuditEntity setAuditLogOperation(AuditLogOperation auditLogOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IAuditEntity setPrivilegeType(PrivilegeType privilegeType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IAuditEntity setPrivilegeTypes(List<PrivilegeType> privilegeTypes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IAuditEntity setResult(boolean result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IAuditEntity setDatabase(String database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IAuditEntity setSqlString(String sqlString) {
    throw new UnsupportedOperationException();
  }
}
