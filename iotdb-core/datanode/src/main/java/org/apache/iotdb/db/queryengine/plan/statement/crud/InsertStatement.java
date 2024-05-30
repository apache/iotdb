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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;

/** this class extends {@code Statement} and process insert statement. */
public class InsertStatement extends Statement {

  private PartialPath device;

  private long[] times;
  private String[] measurementList;

  private List<Object[]> valuesList;

  private boolean isAligned;

  public InsertStatement() {
    statementType = StatementType.INSERT;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurementList) {
      PartialPath fullPath = device.concatNode(m);
      ret.add(fullPath);
    }
    return ret;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathListPermission(
            userName, checkedPaths, PrivilegeType.WRITE_DATA.ordinal()),
        checkedPaths,
        PrivilegeType.WRITE_DATA);
  }

  public PartialPath getDevice() {
    return device;
  }

  public void setDevice(PartialPath device) {
    this.device = device;
  }

  public String[] getMeasurementList() {
    return measurementList;
  }

  public void setMeasurementList(String[] measurementList) {
    this.measurementList = measurementList;
  }

  public List<Object[]> getValuesList() {
    return valuesList;
  }

  public void setValuesList(List<Object[]> valuesList) {
    this.valuesList = valuesList;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsert(this, context);
  }
}
