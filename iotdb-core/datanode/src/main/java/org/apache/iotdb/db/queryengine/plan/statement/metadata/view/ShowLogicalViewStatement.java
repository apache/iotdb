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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.view;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;

import java.util.Collections;
import java.util.List;

public class ShowLogicalViewStatement extends ShowStatement {

  private final PartialPath pathPattern;

  private SchemaFilter schemaFilter;

  public ShowLogicalViewStatement(PartialPath pathPattern) {
    super();
    this.pathPattern = pathPattern;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  // Add LBAC check for logical view show operation
  @Override
  public org.apache.iotdb.common.rpc.thrift.TSStatus checkPermissionBeforeProcess(String userName) {
    // Check RBAC permissions first
    org.apache.iotdb.common.rpc.thrift.TSStatus rbacStatus =
        super.checkPermissionBeforeProcess(userName);
    // Check RBAC permission result
    if (rbacStatus.getCode() != org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return rbacStatus;
    }
    // Add LBAC check for read operation
    java.util.List<org.apache.iotdb.commons.path.PartialPath> devicePaths = getPaths();
    org.apache.iotdb.common.rpc.thrift.TSStatus lbacStatus =
        org.apache.iotdb.db.auth.LbacIntegration.checkLbacAfterRbac(this, userName, devicePaths);
    if (lbacStatus.getCode() != org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return lbacStatus;
    }
    return new org.apache.iotdb.common.rpc.thrift.TSStatus(
        org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowLogicalView(this, context);
  }
}
