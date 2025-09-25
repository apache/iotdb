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

package org.apache.iotdb.db.queryengine.plan.relational.security;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;

public class TreeAccessCheckContext implements IAuditEntity {

  private final UserEntity userEntity;

  public TreeAccessCheckContext(UserEntity userEntity) {
    this.userEntity = userEntity;
  }

  @Override
  public long getUserId() {
    return userEntity.getUserId();
  }

  @Override
  public String getUsername() {
    return userEntity.getUsername();
  }

  @Override
  public String getCliHostname() {
    return userEntity.getCliHostname();
  }

  private AuditEventType auditEventType;
  private AuditLogOperation auditLogOperation;
  private PrivilegeType privilegeType;
  private boolean result;
  private String database;
  private String sqlString;

  @Override
  public AuditEventType getAuditEventType() {
    return auditEventType;
  }

  @Override
  public IAuditEntity setAuditEventType(AuditEventType auditEventType) {
    this.auditEventType = auditEventType;
    return this;
  }

  @Override
  public AuditLogOperation getAuditLogOperation() {
    return auditLogOperation;
  }

  @Override
  public IAuditEntity setAuditLogOperation(AuditLogOperation auditLogOperation) {
    this.auditLogOperation = auditLogOperation;
    return this;
  }

  @Override
  public PrivilegeType getPrivilegeType() {
    return privilegeType;
  }

  @Override
  public IAuditEntity setPrivilegeType(PrivilegeType privilegeType) {
    this.privilegeType = privilegeType;
    return this;
  }

  @Override
  public boolean getResult() {
    return result;
  }

  @Override
  public IAuditEntity setResult(boolean result) {
    this.result = result;
    return this;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public IAuditEntity setDatabase(String database) {
    this.database = database;
    return this;
  }

  @Override
  public String getSqlString() {
    return sqlString;
  }

  @Override
  public IAuditEntity setSqlString(String sqlString) {
    this.sqlString = sqlString;
    return this;
  }

  public UserEntity getUserEntity() {
    return userEntity;
  }
}
