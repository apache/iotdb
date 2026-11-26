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

package org.apache.iotdb.db.lbac;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.lbac.LBACAccessDeniedException;
import org.apache.iotdb.commons.lbac.RequiredLabels;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;

import java.util.Map;

/**
 * LBAC (Label-Based Access Control) entry point, at the same level as RBAC's {@code AccessControl}.
 * Performs label-level access checks on protected columns.
 */
@SuppressWarnings("java:S100")
public interface ILBACAccessControl {

  /**
   * Performs a full LBAC access check for the given DDL statement.
   *
   * <p>Internally extracts the required column labels from the statement, loads user/role grants
   * and LBAC metadata from cache, then compares subject labels against required object labels.
   *
   * @param statement the DDL statement to check
   * @param metadata metadata handle for table schema lookups
   * @param clientSession the client session for database resolution
   * @param auditEntity the audit entity carrying the user identity
   * @throws LBACAccessDeniedException if any required label comparison fails
   */
  void checkCanAccess(
      Statement statement,
      Metadata metadata,
      IClientSession clientSession,
      IAuditEntity auditEntity)
      throws LBACAccessDeniedException;

  /**
   * Performs a full LBAC access check for explicitly-declared column label requirements, without
   * going through the relational StatementAnalyzer (e.g. the tree-model Load TsFile path).
   *
   * @param requiredLabelsByPolicy required object labels grouped by policy name
   * @param auditEntity the audit entity carrying the user identity
   * @throws LBACAccessDeniedException
   */
  void checkCanAccess(Map<String, RequiredLabels> requiredLabelsByPolicy, IAuditEntity auditEntity)
      throws LBACAccessDeniedException;

  /**
   * Clears any LBAC-local caches. Invoked when the metadata lease is fenced and the DataNode drops
   * its caches, so a recovery forces a fresh re-fetch from the ConfigNode. Defaults to a no-op for
   * implementations without a local cache.
   */
  default void clearCache() {}
}
