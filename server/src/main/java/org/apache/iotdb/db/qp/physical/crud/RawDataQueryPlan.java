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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RawDataQueryPlan extends QueryPlan {

  private List<PartialPath> deduplicatedPaths = new ArrayList<>();
  private List<TSDataType> deduplicatedDataTypes = new ArrayList<>();
  private IExpression expression = null;
  private Map<String, Set<String>> deviceToMeasurements = new HashMap<>();

  private List<PartialPath> deduplicatedVectorPaths = new ArrayList<>();
  private List<TSDataType> deduplicatedVectorDataTypes = new ArrayList<>();

  public RawDataQueryPlan() {
    super();
  }

  public RawDataQueryPlan(boolean isQuery, Operator.OperatorType operatorType) {
    super(isQuery, operatorType);
  }

  public IExpression getExpression() {
    return expression;
  }

  public void setExpression(IExpression expression) throws QueryProcessException {
    this.expression = expression;
  }

  public List<PartialPath> getDeduplicatedPaths() {
    return deduplicatedPaths;
  }

  public void addDeduplicatedPaths(PartialPath path) {
    deviceToMeasurements
        .computeIfAbsent(path.getDevice(), key -> new HashSet<>())
        .add(path.getMeasurement());
    this.deduplicatedPaths.add(path);
  }

  /**
   * used for AlignByDevice Query, the query is executed by each device, So we only maintain
   * measurements of current device.
   */
  public void setDeduplicatedPathsAndUpdate(List<PartialPath> deduplicatedPaths) {
    deviceToMeasurements.clear();
    deduplicatedPaths.forEach(
        path -> {
          Set<String> set =
              deviceToMeasurements.computeIfAbsent(path.getDevice(), key -> new HashSet<>());
          set.add(path.getMeasurement());
          if (path instanceof VectorPartialPath) {
            ((VectorPartialPath) path)
                .getSubSensorsPathList()
                .forEach(subSensor -> set.add(subSensor.getMeasurement()));
          }
        });
    this.deduplicatedPaths = deduplicatedPaths;
  }

  public void setDeduplicatedPaths(List<PartialPath> deduplicatedPaths) {
    this.deduplicatedPaths = deduplicatedPaths;
  }

  public List<TSDataType> getDeduplicatedDataTypes() {
    return deduplicatedDataTypes;
  }

  public void addDeduplicatedDataTypes(TSDataType dataType) {
    this.deduplicatedDataTypes.add(dataType);
  }

  public void setDeduplicatedDataTypes(List<TSDataType> deduplicatedDataTypes) {
    this.deduplicatedDataTypes = deduplicatedDataTypes;
  }

  public Set<String> getAllMeasurementsInDevice(String device) {
    return deviceToMeasurements.getOrDefault(device, Collections.emptySet());
  }

  public void addFilterPathInDeviceToMeasurements(Path path) {
    deviceToMeasurements
        .computeIfAbsent(path.getDevice(), key -> new HashSet<>())
        .add(path.getMeasurement());
  }

  public Map<String, Set<String>> getDeviceToMeasurements() {
    return deviceToMeasurements;
  }

  public void transformPaths(MManager mManager) throws MetadataException {
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      PartialPath path = mManager.transformPath(deduplicatedPaths.get(i));
      if (path instanceof VectorPartialPath) {
        deduplicatedPaths.set(i, path);
      }
    }
  }

  public List<PartialPath> getDeduplicatedVectorPaths() {
    return deduplicatedVectorPaths;
  }

  public void setDeduplicatedVectorPaths(List<PartialPath> deduplicatedVectorPaths) {
    this.deduplicatedVectorPaths = deduplicatedVectorPaths;
  }

  public List<TSDataType> getDeduplicatedVectorDataTypes() {
    return deduplicatedVectorDataTypes;
  }

  public void setDeduplicatedVectorDataTypes(List<TSDataType> deduplicatedVectorDataTypes) {
    this.deduplicatedVectorDataTypes = deduplicatedVectorDataTypes;
  }

  public void transformToVector() {
    if (!this.deduplicatedVectorPaths.isEmpty()) {
      this.deduplicatedPaths = this.deduplicatedVectorPaths;
      this.deduplicatedDataTypes = this.deduplicatedVectorDataTypes;
      setPathToIndex(getVectorPathToIndex());
    }
  }

  public boolean isRawQuery() {
    return true;
  }
}
