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
import org.apache.iotdb.commons.lbac.RequiredLabels;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;

import java.util.Map;

/**
 * Default implementation of {@link ILBACAccessControl} that allows all access. A DataNode may
 * replace it with a real LBAC implementation during startup.
 */
@SuppressWarnings("java:S100")
public class AllowAllLBACAccessControl implements ILBACAccessControl {

  @Override
  public void checkCanAccess(
      final Statement statement,
      final Metadata metadata,
      final IClientSession clientSession,
      final IAuditEntity auditEntity) {}

  @Override
  public void checkCanAccess(
      final Map<String, RequiredLabels> requiredLabelsByPolicy, final IAuditEntity auditEntity) {}
}
