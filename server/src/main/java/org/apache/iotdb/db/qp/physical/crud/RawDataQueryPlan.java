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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
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

  // TODO: remove this when all types of query supporting vector
  /** used to group all the sub sensors of one vector into VectorPartialPath */
  private List<PartialPath> deduplicatedVectorPaths = new ArrayList<>();

  private List<TSDataType> deduplicatedVectorDataTypes = new ArrayList<>();

  public RawDataQueryPlan() {
    super();
  }

  public RawDataQueryPlan(boolean isQuery, Operator.OperatorType operatorType) {
    super(isQuery, operatorType);
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException {
    // sort paths by device, to accelerate the metadata read process
    List<Pair<PartialPath, Integer>> indexedPaths = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      indexedPaths.add(new Pair<>(paths.get(i), i));
    }
    indexedPaths.sort(Comparator.comparing(pair -> pair.left));

    Map<String, Integer> pathNameToReaderIndex = new HashMap<>();
    Set<String> columnForReaderSet = new HashSet<>();
    Set<String> columnForDisplaySet = new HashSet<>();

    for (Pair<PartialPath, Integer> indexedPath : indexedPaths) {
      PartialPath originalPath = indexedPath.left;
      Integer originalIndex = indexedPath.right;

      String columnForReader = getColumnForReaderFromPath(originalPath, originalIndex);
      if (!columnForReaderSet.contains(columnForReader)) {
        addDeduplicatedPaths(originalPath);
        addDeduplicatedDataTypes(dataTypes.get(originalIndex));
        pathNameToReaderIndex.put(columnForReader, pathNameToReaderIndex.size());
        if (this instanceof AggregationPlan) {
          ((AggregationPlan) this)
              .addDeduplicatedAggregations(getAggregations().get(originalIndex));
        }
        columnForReaderSet.add(columnForReader);
      }

      String columnForDisplay = getColumnForDisplay(columnForReader, originalIndex);
      if (!columnForDisplaySet.contains(columnForDisplay)) {
        setColumnNameToDatasetOutputIndex(columnForDisplay, getPathToIndex().size());
        columnForDisplaySet.add(columnForDisplay);
      }
    }

    // if it is a RawQueryWithoutValueFilter, we also need to group all the subSensors of one
    // vector into one VectorPartialPath
    groupVectorPaths(physicalGenerator);
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
            set.addAll(((VectorPartialPath) path).getSubSensorsList());
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
    return deviceToMeasurements.getOrDefault(device, new HashSet<>());
  }

  public void addFilterPathInDeviceToMeasurements(Path path) {
    deviceToMeasurements
        .computeIfAbsent(path.getDevice(), key -> new HashSet<>())
        .add(path.getMeasurement());
  }

  public Map<String, Set<String>> getDeviceToMeasurements() {
    return deviceToMeasurements;
  }

  /**
   * Group all the subSensors of one vector into one VectorPartialPath save the grouped
   * VectorPartialPath in deduplicatedVectorPaths and deduplicatedVectorDataTypes instead of putting
   * them directly into deduplicatedPaths and deduplicatedDataTypes, because we don't know whether
   * the raw query has value filter here.
   */
  public void groupVectorPaths(PhysicalGenerator physicalGenerator) throws MetadataException {
    List<PartialPath> vectorizedDeduplicatedPaths =
        physicalGenerator.groupVectorPaths(getDeduplicatedPaths());
    List<TSDataType> vectorizedDeduplicatedDataTypes =
        new ArrayList<>(physicalGenerator.getSeriesTypes(vectorizedDeduplicatedPaths));
    setDeduplicatedVectorPaths(vectorizedDeduplicatedPaths);
    setDeduplicatedVectorDataTypes(vectorizedDeduplicatedDataTypes);
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

  /**
   * RawQueryWithoutValueFilter should call this method to use grouped vector partial path to
   * replace the previous deduplicatedPaths and deduplicatedDataTypes
   */
  public void transformToVector() {
    if (!this.deduplicatedVectorPaths.isEmpty()) {
      this.deduplicatedPaths = this.deduplicatedVectorPaths;
      this.deduplicatedDataTypes = this.deduplicatedVectorDataTypes;
    }
  }
}
