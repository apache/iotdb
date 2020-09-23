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

package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

abstract public class InsertPlan extends PhysicalPlan {

  protected PartialPath deviceId;
  protected String[] measurements;
  protected TSDataType[] dataTypes;
  protected MeasurementMNode[] measurementMNodes;

  // record the failed measurements
  protected List<String> failedMeasurements;

  public InsertPlan(Operator.OperatorType operatorType) {
    super(false, operatorType);
    super.canBeSplit = false;
  }

  public PartialPath getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(PartialPath deviceId) {
    this.deviceId = deviceId;
  }

  public String[] getMeasurements() {
    return this.measurements;
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  public MeasurementMNode[] getMeasurementMNodes() {
    return measurementMNodes;
  }

  public void setMeasurementMNodes(MeasurementMNode[] mNodes) {
    this.measurementMNodes = mNodes;
  }

  public List<String> getFailedMeasurements() {
    return failedMeasurements;
  }

  public int getFailedMeasurementNumber() {
    return failedMeasurements == null ? 0 : failedMeasurements.size();
  }

  /**
   * @param index failed measurement index
   */
  public void markFailedMeasurementInsertion(int index) {
    if (failedMeasurements == null) {
      failedMeasurements = new ArrayList<>();
    }
    failedMeasurements.add(measurements[index]);
    measurements[index] = null;
    dataTypes[index] = null;
  }

}
