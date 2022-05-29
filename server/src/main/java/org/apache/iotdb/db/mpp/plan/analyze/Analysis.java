/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSetInfo;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FilterNullParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Analysis used for planning a query. TODO: This class may need to store more info for a query. */
public class Analysis {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Common Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // Statement
  private Statement statement;

  // indicate whether this statement is `WRITE` or `READ`
  private QueryType queryType;

  private DataPartition dataPartition;

  private SchemaPartition schemaPartition;

  private SchemaTree schemaTree;

  // map from output column name (for every node) to its datatype
  private TypeProvider typeProvider;

  private boolean finishQueryAfterAnalyze;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Analysis (used in ALIGN BY TIME)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // map from device name to series/aggregation under this device
  private Set<Expression> sourceExpressions;

  // input expressions of aggregations to be calculated
  private Set<Expression> aggregationTransformExpressions;

  // all aggregations that need to be calculated
  private Set<Expression> aggregationExpressions;

  // expression of output column to be calculated
  private Set<Expression> transformExpressions;

  private Expression queryFilter;

  // map from grouped path name to list of input aggregation in `GROUP BY LEVEL` clause
  private Map<Expression, Set<Expression>> groupByLevelExpressions;

  // map from raw path to grouped path in `GROUP BY LEVEL` clause
  private Map<Expression, Expression> rawPathToGroupedPathMap;

  private boolean isRawDataSource;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Analysis (used in ALIGN BY DEVICE)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // map from device name to series/aggregation under this device
  private Map<String, Set<Expression>> deviceToSourceExpressions;

  // input expressions of aggregations to be calculated
  private Map<String, Set<Expression>> deviceToAggregationTransformExpressions;

  // all aggregations that need to be calculated
  private Map<String, Set<Expression>> deviceToAggregationExpressions;

  // expression of output column to be calculated
  private Map<String, Set<Expression>> deviceToTransformExpressions;

  // map from device name to query filter under this device
  private Map<String, Expression> deviceToQueryFilter;

  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  private Map<String, List<Integer>> deviceToMeasurementIndexesMap;

  private Map<String, Boolean> deviceToIsRawDataSource;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Common Analysis (above DeviceView)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // indicate is there a value filter
  private boolean hasValueFilter = false;

  // true if nested expressions and UDFs exist in aggregation function
  private boolean isHasRawDataInputAggregation;

  // a global time filter used in `initQueryDataSource` and filter push down
  private Filter globalTimeFilter;

  // parameter of `WITHOUT NULL` clause
  private FilterNullParameter filterNullParameter;

  // parameter of `FILL` clause
  private FillDescriptor fillDescriptor;

  // parameter of `GROUP BY TIME` clause
  private GroupByTimeParameter groupByTimeParameter;

  // header of result dataset
  private DatasetHeader respDatasetHeader;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Schema Query Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // extra mesaage from config node, used for node management
  private Set<String> matchedNodes;

  // extracted from partition, used for delete data, invalidate cache and delete timeseries
  private List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList;

  public Analysis() {
    this.finishQueryAfterAnalyze = false;
  }

  public List<TRegionReplicaSet> getPartitionInfo(PartialPath seriesPath, Filter timefilter) {
    // TODO: (xingtanzjr) implement the calculation of timePartitionIdList
    return dataPartition.getDataRegionReplicaSet(seriesPath.getDevice(), null);
  }

  public List<TRegionReplicaSet> getPartitionInfo(String deviceName, Filter globalTimeFilter) {
    return dataPartition.getDataRegionReplicaSet(deviceName, null);
  }

  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement statement) {
    this.statement = statement;
  }

  public DataPartition getDataPartitionInfo() {
    return dataPartition;
  }

  public void setDataPartitionInfo(DataPartition dataPartition) {
    this.dataPartition = dataPartition;
  }

  public SchemaPartition getSchemaPartitionInfo() {
    return schemaPartition;
  }

  public void setSchemaPartitionInfo(SchemaPartition schemaPartition) {
    this.schemaPartition = schemaPartition;
  }

  public SchemaTree getSchemaTree() {
    return schemaTree;
  }

  public void setSchemaTree(SchemaTree schemaTree) {
    this.schemaTree = schemaTree;
  }

  public Filter getGlobalTimeFilter() {
    return globalTimeFilter;
  }

  public void setGlobalTimeFilter(Filter timeFilter) {
    this.globalTimeFilter = timeFilter;
  }

  public DatasetHeader getRespDatasetHeader() {
    return respDatasetHeader;
  }

  public void setRespDatasetHeader(DatasetHeader respDatasetHeader) {
    this.respDatasetHeader = respDatasetHeader;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public void setTypeProvider(TypeProvider typeProvider) {
    this.typeProvider = typeProvider;
  }

  public boolean hasDataSource() {
    return (dataPartition != null && !dataPartition.isEmpty())
        || (schemaPartition != null && !schemaPartition.isEmpty());
  }

  public boolean isHasRawDataInputAggregation() {
    return isHasRawDataInputAggregation;
  }

  public void setHasRawDataInputAggregation(boolean hasRawDataInputAggregation) {
    isHasRawDataInputAggregation = hasRawDataInputAggregation;
  }

  public Map<Expression, Set<Expression>> getGroupByLevelExpressions() {
    return groupByLevelExpressions;
  }

  public void setGroupByLevelExpressions(Map<Expression, Set<Expression>> groupByLevelExpressions) {
    this.groupByLevelExpressions = groupByLevelExpressions;
  }

  public void setRawPathToGroupedPathMap(Map<Expression, Expression> rawPathToGroupedPathMap) {
    this.rawPathToGroupedPathMap = rawPathToGroupedPathMap;
  }

  public Expression getGroupedExpressionByLevel(Expression expression) {
    if (rawPathToGroupedPathMap.containsKey(expression)) {
      return rawPathToGroupedPathMap.get(expression);
    }
    if (rawPathToGroupedPathMap.containsValue(expression)) {
      return expression;
    }
    throw new IllegalArgumentException(
        String.format("GROUP BY LEVEL: Unknown input expression '%s'", expression));
  }

  public FilterNullParameter getFilterNullParameter() {
    return filterNullParameter;
  }

  public void setFilterNullParameter(FilterNullParameter filterNullParameter) {
    this.filterNullParameter = filterNullParameter;
  }

  public FillDescriptor getFillDescriptor() {
    return fillDescriptor;
  }

  public void setFillDescriptor(FillDescriptor fillDescriptor) {
    this.fillDescriptor = fillDescriptor;
  }

  public boolean hasValueFilter() {
    return hasValueFilter;
  }

  public void setHasValueFilter(boolean hasValueFilter) {
    this.hasValueFilter = hasValueFilter;
  }

  public Expression getQueryFilter() {
    return queryFilter;
  }

  public void setQueryFilter(Expression queryFilter) {
    this.queryFilter = queryFilter;
  }

  public Map<String, Expression> getDeviceToQueryFilter() {
    return deviceToQueryFilter;
  }

  public void setDeviceToQueryFilter(Map<String, Expression> deviceToQueryFilter) {
    this.deviceToQueryFilter = deviceToQueryFilter;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public void setGroupByTimeParameter(GroupByTimeParameter groupByTimeParameter) {
    this.groupByTimeParameter = groupByTimeParameter;
  }

  public boolean isFinishQueryAfterAnalyze() {
    return finishQueryAfterAnalyze;
  }

  public void setFinishQueryAfterAnalyze(boolean finishQueryAfterAnalyze) {
    this.finishQueryAfterAnalyze = finishQueryAfterAnalyze;
  }

  public void setDeviceToMeasurementIndexesMap(
      Map<String, List<Integer>> deviceToMeasurementIndexesMap) {
    this.deviceToMeasurementIndexesMap = deviceToMeasurementIndexesMap;
  }

  public Map<String, List<Integer>> getDeviceToMeasurementIndexesMap() {
    return deviceToMeasurementIndexesMap;
  }

  public Set<Expression> getSourceExpressions() {
    return sourceExpressions;
  }

  public void setSourceExpressions(Set<Expression> sourceExpressions) {
    this.sourceExpressions = sourceExpressions;
  }

  public Set<Expression> getAggregationTransformExpressions() {
    return aggregationTransformExpressions;
  }

  public void setAggregationTransformExpressions(Set<Expression> aggregationTransformExpressions) {
    this.aggregationTransformExpressions = aggregationTransformExpressions;
  }

  public Set<Expression> getAggregationExpressions() {
    return aggregationExpressions;
  }

  public void setAggregationExpressions(Set<Expression> aggregationExpressions) {
    this.aggregationExpressions = aggregationExpressions;
  }

  public Set<Expression> getTransformExpressions() {
    return transformExpressions;
  }

  public void setTransformExpressions(Set<Expression> transformExpressions) {
    this.transformExpressions = transformExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToSourceExpressions() {
    return deviceToSourceExpressions;
  }

  public void setDeviceToSourceExpressions(Map<String, Set<Expression>> deviceToSourceExpressions) {
    this.deviceToSourceExpressions = deviceToSourceExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToAggregationTransformExpressions() {
    return deviceToAggregationTransformExpressions;
  }

  public void setDeviceToAggregationTransformExpressions(
      Map<String, Set<Expression>> deviceToAggregationTransformExpressions) {
    this.deviceToAggregationTransformExpressions = deviceToAggregationTransformExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToAggregationExpressions() {
    return deviceToAggregationExpressions;
  }

  public void setDeviceToAggregationExpressions(
      Map<String, Set<Expression>> deviceToAggregationExpressions) {
    this.deviceToAggregationExpressions = deviceToAggregationExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToTransformExpressions() {
    return deviceToTransformExpressions;
  }

  public void setDeviceToTransformExpressions(
      Map<String, Set<Expression>> deviceToTransformExpressions) {
    this.deviceToTransformExpressions = deviceToTransformExpressions;
  }

  public boolean isRawDataSource() {
    return isRawDataSource;
  }

  public void setRawDataSource(boolean rawDataSource) {
    isRawDataSource = rawDataSource;
  }

  public Map<String, Boolean> getDeviceToIsRawDataSource() {
    return deviceToIsRawDataSource;
  }

  public void setDeviceToIsRawDataSource(Map<String, Boolean> deviceToIsRawDataSource) {
    this.deviceToIsRawDataSource = deviceToIsRawDataSource;
  }

  public Set<String> getMatchedNodes() {
    return matchedNodes;
  }

  public void setMatchedNodes(Set<String> matchedNodes) {
    this.matchedNodes = matchedNodes;
  }

  public List<Pair<RegionReplicaSetInfo, List<PartialPath>>> getRegionRequestList() {
    return regionRequestList;
  }

  public void setRegionRequestList(
      List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList) {
    this.regionRequestList = regionRequestList;
  }
}
