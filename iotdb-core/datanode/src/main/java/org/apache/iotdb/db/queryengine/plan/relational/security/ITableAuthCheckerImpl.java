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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.rpc.TSStatusCode;

public class ITableAuthCheckerImpl implements ITableAuthChecker {

  @Override
  public void checkDatabaseVisibility(String userName, String databaseName) {
    if (!AuthorityChecker.checkDBVisible(userName, databaseName)) {
      throw new RuntimeException(
          new IoTDBException("NO PERMISSION", TSStatusCode.NO_PERMISSION.getStatusCode()));
    }
  }

  @Override
  public void checkDatabasePrivilege(
      String userName, String databaseName, TableModelPrivilege privilege) {
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkDBPermission(
                userName, databaseName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            databaseName);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(result.getMessage(), result.getCode()));
    }
  }

  @Override
  public void checkTablePrivilege(
      String userName, QualifiedObjectName tableName, TableModelPrivilege privilege) {
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkTablePermission(
                userName,
                tableName.getDatabaseName(),
                tableName.getObjectName(),
                privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            tableName.getDatabaseName(),
            tableName.getObjectName());
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(result.getMessage(), result.getCode()));
    }
  }

  @Override
  public void checkTableVisibility(String userName, QualifiedObjectName tableName) {
    if (!AuthorityChecker.checkTableVisible(
        userName, tableName.getDatabaseName(), tableName.getObjectName())) {
      throw new RuntimeException(
          new IoTDBException("NO PERMISSION", TSStatusCode.NO_PERMISSION.getStatusCode()));
    }
  }

  @Override
  public void checkGlobalPrivilege(String userName, TableModelPrivilege privilege) {
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(result.getMessage(), result.getCode()));
    }
  }
}