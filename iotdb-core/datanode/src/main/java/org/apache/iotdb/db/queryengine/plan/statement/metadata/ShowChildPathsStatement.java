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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.LbacIntegration;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

public class ShowChildPathsStatement extends ShowStatement {
  private final PartialPath partialPath;

  public ShowChildPathsStatement(PartialPath partialPath) {
    super();
    this.partialPath = partialPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(partialPath);
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    // Check RBAC permissions first
    TSStatus rbacStatus = super.checkPermissionBeforeProcess(userName);

    // Check RBAC permission result
    if (rbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return rbacStatus;
    }

    // Add LBAC check for read operation
    List<PartialPath> devicePaths = getPaths();
    TSStatus lbacStatus = LbacIntegration.checkLbacAfterRbac(this, userName, devicePaths);
    if (lbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return lbacStatus;
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowChildPaths(this, context);
  }
}
