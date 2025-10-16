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
import java.util.Objects;

/** This class defines the fields of a user entity to be audited. */
public class UserEntity implements IAuditEntity {

  private final long userId;

  private final String username;

  private final String cliHostname;

  public UserEntity(long userId, String username, String cliHostname) {
    this.userId = userId;
    this.username = username;
    this.cliHostname = cliHostname;
  }

  public long getUserId() {
    return userId;
  }

  public String getUsername() {
    return username;
  }

  public String getCliHostname() {
    return cliHostname;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserEntity that = (UserEntity) o;
    return userId == that.userId
        && Objects.equals(username, that.username)
        && Objects.equals(cliHostname, that.cliHostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userId, username, cliHostname);
  }

  private AuditEventType auditEventType;
  private AuditLogOperation auditLogOperation;
  private List<PrivilegeType> privilegeTypeList;
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
  public List<PrivilegeType> getPrivilegeTypes() {
    return privilegeTypeList;
  }

  @Override
  public String getPrivilegeTypeString() {
    return privilegeTypeList.toString();
  }

  @Override
  public IAuditEntity setPrivilegeType(PrivilegeType privilegeType) {
    this.privilegeTypeList = Collections.singletonList(privilegeType);
    return this;
  }

  @Override
  public IAuditEntity setPrivilegeTypes(List<PrivilegeType> privilegeTypes) {
    this.privilegeTypeList = privilegeTypes;
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
}
