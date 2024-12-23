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

package org.apache.iotdb.db.queryengine.plan.statement.internal;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InternalCreateMultiTimeSeriesStatement extends Statement {

  private final Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap;

  public InternalCreateMultiTimeSeriesStatement(
      Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap) {
    super();
    statementType = StatementType.INTERNAL_CREATE_MULTI_TIMESERIES;
    this.deviceMap = deviceMap;
  }

  public Map<PartialPath, Pair<Boolean, MeasurementGroup>> getDeviceMap() {
    return deviceMap;
  }

  @Override
  public List<PartialPath> getPaths() {
    return deviceMap.entrySet().stream()
        .flatMap(
            entry ->
                entry.getValue().right.getMeasurements().stream()
                    .map(entry.getKey()::concatAsMeasurementPath))
        .collect(Collectors.toList());
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathListPermission(
            userName, checkedPaths, PrivilegeType.WRITE_SCHEMA.ordinal()),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInternalCreateMultiTimeSeries(this, context);
  }
}
