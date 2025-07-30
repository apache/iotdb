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

package org.apache.iotdb.db.queryengine.plan.statement;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.LbacPermissionChecker;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.List;

/**
 * This class is a superclass of all statements.
 *
 * <p>A Statement containing all semantic information of an SQL. It is obtained by traversing the
 * AST via {@link ASTVisitor}.
 */
public abstract class Statement extends StatementNode {
  protected StatementType statementType = StatementType.NULL;

  protected boolean isDebug;

  protected Statement() {}

  public void setType(final StatementType statementType) {
    this.statementType = statementType;
  }

  public StatementType getType() {
    return statementType;
  }

  public boolean isDebug() {
    return isDebug;
  }

  public void setDebug(final boolean debug) {
    isDebug = debug;
  }

  public boolean isQuery() {
    return false;
  }

  public abstract List<? extends PartialPath> getPaths();

  /**
   * Enhanced three-step permission check: Root → RBAC → LBAC This is the main permission check
   * method that follows the security principle
   *
   * @param userName The username requesting access
   * @return TSStatus indicating success or failure
   */
  public TSStatus checkPermissionBeforeProcess(final String userName) {
    // Step 1: Check if root user (highest priority)
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    // Step 2: RBAC check
    TSStatus rbacStatus = checkRbacPermission(userName);
    if (rbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return rbacStatus;
    }

    // Step 3: LBAC check (only if RBAC passes)
    return checkLbacPermission(userName);
  }

  /**
   * RBAC permission check - to be overridden by subclasses that need specific RBAC checks Default
   * implementation allows access for show operations
   *
   * @param userName The username requesting access
   * @return TSStatus indicating RBAC check result
   */
  public TSStatus checkRbacPermission(String userName) {
    // Default implementation for show operations - allow access
    // Subclasses should override this for specific RBAC checks
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * LBAC permission check using LbacPermissionChecker This method is consistent across all
   * statement types
   *
   * @param userName The username requesting access
   * @return TSStatus indicating LBAC check result
   */
  public TSStatus checkLbacPermission(String userName) {
    try {
      return LbacPermissionChecker.checkLbacPermissionForStatement(this, userName);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage("LBAC check failed: " + e.getMessage());
    }
  }

  /**
   * Determine the privilege type needed for this statement. Subclasses should override this method
   * to provide specific privilege types.
   *
   * @return The privilege type needed for this statement
   */
  public PrivilegeType determinePrivilegeType() {
    // Default implementation - subclasses should override
    return null;
  }

  public org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement toRelationalStatement(
      final MPPQueryContext context) {
    throw new UnsupportedOperationException("Method not implemented yet");
  }
}
