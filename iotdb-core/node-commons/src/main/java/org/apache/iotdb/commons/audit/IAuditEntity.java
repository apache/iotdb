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

import java.util.List;

public interface IAuditEntity {

  long getUserId();

  String getUsername();

  String getCliHostname();

  AuditEventType getAuditEventType();

  IAuditEntity setAuditEventType(AuditEventType auditEventType);

  AuditLogOperation getAuditLogOperation();

  IAuditEntity setAuditLogOperation(AuditLogOperation auditLogOperation);

  List<PrivilegeType> getPrivilegeTypes();

  String getPrivilegeTypeString();

  IAuditEntity setPrivilegeType(PrivilegeType privilegeType);

  IAuditEntity setPrivilegeTypes(List<PrivilegeType> privilegeTypes);

  boolean getResult();

  IAuditEntity setResult(boolean result);

  String getDatabase();

  IAuditEntity setDatabase(String database);

  String getSqlString();

  IAuditEntity setSqlString(String sqlString);
}
