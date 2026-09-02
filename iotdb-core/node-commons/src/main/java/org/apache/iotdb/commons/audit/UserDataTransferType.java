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

public enum UserDataTransferType {
  MPP_TS_BLOCK(AuditLogOperation.QUERY, PrivilegeType.READ_DATA),
  INSERT_PLAN_NODE(AuditLogOperation.DML, PrivilegeType.WRITE_DATA),
  LOAD_TSFILE_PIECE(AuditLogOperation.DML, PrivilegeType.WRITE_DATA),
  IOT_CONSENSUS_LOG(AuditLogOperation.DML, PrivilegeType.WRITE_DATA),
  IOT_CONSENSUS_SNAPSHOT(AuditLogOperation.DML, PrivilegeType.WRITE_DATA),
  IOT_CONSENSUS_V2_TABLET(AuditLogOperation.DML, PrivilegeType.WRITE_DATA),
  IOT_CONSENSUS_V2_TSFILE(AuditLogOperation.DML, PrivilegeType.WRITE_DATA);

  private final AuditLogOperation operation;
  private final PrivilegeType privilegeType;

  UserDataTransferType(AuditLogOperation operation, PrivilegeType privilegeType) {
    this.operation = operation;
    this.privilegeType = privilegeType;
  }

  public AuditLogOperation getOperation() {
    return operation;
  }

  public PrivilegeType getPrivilegeType() {
    return privilegeType;
  }
}
