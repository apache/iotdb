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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;
import java.util.stream.Collectors;

// This is only used for auto creation while inserting data
public class CreateTimeSeriesByDeviceStatement extends Statement {

  private PartialPath devicePath;
  private List<String> measurements;
  private List<TSDataType> tsDataTypes;

  public CreateTimeSeriesByDeviceStatement(
      PartialPath devicePath, List<String> measurements, List<TSDataType> tsDataTypes) {
    super();
    setType(StatementType.CREATE_TIMESERIES_BY_DEVICE);
    this.devicePath = devicePath;
    this.measurements = measurements;
    this.tsDataTypes = tsDataTypes;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public List<TSDataType> getTsDataTypes() {
    return tsDataTypes;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return measurements.stream().map(o -> devicePath.concatNode(o)).collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTimeseriesByDevice(this, context);
  }
}
