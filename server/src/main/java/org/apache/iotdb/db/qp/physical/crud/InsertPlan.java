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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class InsertPlan extends PhysicalPlan {

  protected PartialPath prefixPath;
  protected PartialPath originalPrefixPath;
  protected boolean isAligned;
  protected String[] measurements;
  // get from client
  protected TSDataType[] dataTypes;
  // get from MManager
  protected IMeasurementMNode[] measurementMNodes;

  // record the failed measurements, their reasons, and positions in "measurements"
  List<String> failedMeasurements;
  private List<Exception> failedExceptions;
  List<Integer> failedIndices;

  public InsertPlan(Operator.OperatorType operatorType) {
    super(false, operatorType);
    super.canBeSplit = false;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(PartialPath prefixPath) {
    this.prefixPath = prefixPath;
  }

  /*
  the original prefixPath needs to be recorded and recovered by recoverFromFailure because cluster may try to execute this plan twice
   */
  public void setPrefixPathForAlignTimeSeries(PartialPath prefixPath) {
    this.originalPrefixPath = this.prefixPath;
    this.prefixPath = prefixPath;
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

  public IMeasurementMNode[] getMeasurementMNodes() {
    return measurementMNodes;
  }

  public void setMeasurementMNodes(IMeasurementMNode[] mNodes) {
    this.measurementMNodes = mNodes;
  }

  public List<String> getFailedMeasurements() {
    return failedMeasurements;
  }

  public List<Exception> getFailedExceptions() {
    return failedExceptions;
  }

  public int getFailedMeasurementNumber() {
    return failedMeasurements == null ? 0 : failedMeasurements.size();
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public abstract long getMinTime();

  /** @param index failed measurement index */
  public void markFailedMeasurementInsertion(int index, Exception e) {
    if (measurements[index] == null) {
      return;
    }
    if (failedMeasurements == null) {
      failedMeasurements = new ArrayList<>();
      failedExceptions = new ArrayList<>();
      failedIndices = new ArrayList<>();
    }
    failedMeasurements.add(measurements[index]);
    failedExceptions.add(e);
    failedIndices.add(index);
    measurements[index] = null;
  }

  public void markFailedMeasurementAlignedInsertion(Exception e) {
    if (failedMeasurements == null) {
      failedMeasurements = new ArrayList<>();
      failedExceptions = new ArrayList<>();
      failedIndices = new ArrayList<>();
    }

    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] == null) {
        continue;
      }
      failedMeasurements.add(measurements[i]);
      failedExceptions.add(e);
      failedIndices.add(i);
      measurements[i] = null;
    }
  }

  /**
   * Reconstruct this plan with the failed measurements.
   *
   * @return the plan itself, with measurements replaced with the previously failed ones.
   */
  public InsertPlan getPlanFromFailed() {
    if (isAligned && originalPrefixPath != null) {
      prefixPath = originalPrefixPath;
    }
    if (failedMeasurements == null) {
      return null;
    }
    measurements = failedMeasurements.toArray(new String[0]);
    failedMeasurements = null;
    if (dataTypes != null) {
      TSDataType[] temp = dataTypes.clone();
      dataTypes = new TSDataType[failedIndices.size()];
      for (int i = 0; i < failedIndices.size(); i++) {
        dataTypes[i] = temp[failedIndices.get(i)];
      }
    }
    if (measurementMNodes != null) {
      IMeasurementMNode[] temp = measurementMNodes.clone();
      measurementMNodes = new IMeasurementMNode[failedIndices.size()];
      for (int i = 0; i < failedIndices.size(); i++) {
        measurementMNodes[i] = temp[failedIndices.get(i)];
      }
    }

    failedIndices = null;
    failedExceptions = null;
    return this;
  }

  /** Reset measurements from failed measurements (if any), as if no failure had ever happened. */
  public void recoverFromFailure() {
    if (isAligned && originalPrefixPath != null) {
      prefixPath = originalPrefixPath;
    }
    if (failedMeasurements == null) {
      return;
    }

    for (int i = 0; i < failedMeasurements.size(); i++) {
      int index = failedIndices.get(i);
      measurements[index] = failedMeasurements.get(i);
    }
    failedIndices = null;
    failedExceptions = null;
    failedMeasurements = null;
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    if (prefixPath == null) {
      throw new QueryProcessException("DeviceId is null");
    }
    if (measurements == null) {
      throw new QueryProcessException("Measurements are null");
    }
    for (String measurement : measurements) {
      if (measurement == null || measurement.isEmpty()) {
        throw new QueryProcessException(
            "Measurement contains null or empty string: " + Arrays.toString(measurements));
      }
    }
  }
}
