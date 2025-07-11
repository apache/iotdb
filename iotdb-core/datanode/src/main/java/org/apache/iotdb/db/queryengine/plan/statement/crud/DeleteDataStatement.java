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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.LbacIntegration;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.read.common.TimeRange;

import java.util.ArrayList;
import java.util.List;

public class DeleteDataStatement extends Statement {

  private List<MeasurementPath> pathList;
  private long deleteStartTime;
  private long deleteEndTime;

  public DeleteDataStatement() {
    super();
    statementType = StatementType.DELETE;
  }

  @Override
  public List<MeasurementPath> getPaths() {
    return getPathList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    // First check if user is super user
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    // Perform traditional RBAC permission check
    List<MeasurementPath> checkedPaths = getPaths();
    TSStatus rbacStatus =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternListPermission(
                userName, checkedPaths, PrivilegeType.WRITE_DATA),
            checkedPaths,
            PrivilegeType.WRITE_DATA);

    // If RBAC check fails, return immediately
    if (rbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return rbacStatus;
    }

    // Perform LBAC permission check
    try {
      // Extract device paths from measurement paths for LBAC write policy check
      List<PartialPath> devicePaths = new ArrayList<>();
      for (MeasurementPath measurementPath : checkedPaths) {
        PartialPath devicePath = measurementPath.getDevicePath();
        if (!devicePaths.contains(devicePath)) {
          devicePaths.add(devicePath);
        }
      }
      TSStatus lbacStatus = LbacIntegration.checkLbacAfterRbac(this, userName, devicePaths);
      if (lbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return lbacStatus;
      }
    } catch (Exception e) {
      // Reject access when LBAC check fails with exception
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage("LBAC permission check failed: " + e.getMessage());
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public List<MeasurementPath> getPathList() {
    return pathList;
  }

  public void setPathList(List<MeasurementPath> pathList) {
    this.pathList = pathList;
  }

  public long getDeleteStartTime() {
    return deleteStartTime;
  }

  public void setDeleteStartTime(long deleteStartTime) {
    this.deleteStartTime = deleteStartTime;
  }

  public long getDeleteEndTime() {
    return deleteEndTime;
  }

  public void setDeleteEndTime(long deleteEndTime) {
    this.deleteEndTime = deleteEndTime;
  }

  public void setTimeRange(TimeRange timeRange) {
    this.deleteStartTime = timeRange.getMin();
    this.deleteEndTime = timeRange.getMax();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitDeleteData(this, context);
  }
}
