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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.ArrayList;
import java.util.List;

public class ShowTTLStatement extends ShowStatement implements IConfigStatement {
  private final List<PartialPath> pathPatterns = new ArrayList<>();

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTTL(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    return pathPatterns;
  }

  // Add LBAC check for TTL show operation
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

  public void addPathPatterns(PartialPath pathPattern) {
    pathPatterns.add(pathPattern);
  }
}
