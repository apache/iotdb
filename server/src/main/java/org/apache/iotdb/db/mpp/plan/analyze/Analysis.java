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
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FilterNullParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Analysis used for planning a query. TODO: This class may need to store more info for a query. */
public class Analysis {

  // Statement
  private Statement statement;

  // indicate whether this statement is `WRITE` or `READ`
  private QueryType queryType;

  private DataPartition dataPartition;

  private SchemaPartition schemaPartition;

  private SchemaTree schemaTree;

  // map from output column name (for every node) to its datatype
  private TypeProvider typeProvider;

  // map from device name to series/aggregation under this device
  private Map<String, Set<Expression>> sourceExpressions;

  //
  private Set<Expression> selectExpressions;

  // all aggregations that need to be calculated
  private Map<String, Set<Expression>> AggregationExpressions;

  // map from grouped path name to list of input aggregation in `GROUP BY LEVEL` clause
  private Map<Expression, Set<Expression>> groupByLevelExpressions;

  // parameter of `WITHOUT NULL` clause
  private FilterNullParameter filterNullParameter;

  // parameter of `FILL` clause
  private List<FillDescriptor> fillDescriptorList;

  private Expression queryFilter;

  private Map<String, Expression> deviceToQueryFilter;

  // indicate is there a value filter
  private boolean hasValueFilter = false;

  // a global time filter used in `initQueryDataSource`
  private Filter globalTimeFilter;

  // header of result dataset
  private DatasetHeader respDatasetHeader;

  public Analysis() {}

  public List<TRegionReplicaSet> getPartitionInfo(PartialPath seriesPath, Filter timefilter) {
    // TODO: (xingtanzjr) implement the calculation of timePartitionIdList
    return dataPartition.getDataRegionReplicaSet(seriesPath.getDeviceIdString(), null);
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

  public Map<String, Set<Expression>> getSourceExpressions() {
    return sourceExpressions;
  }

  public void setSourceExpressions(Map<String, Set<Expression>> sourceExpressions) {
    this.sourceExpressions = sourceExpressions;
  }

  public Set<Expression> getSelectExpressions() {
    return selectExpressions;
  }

  public void setSelectExpressions(Set<Expression> selectExpressions) {
    this.selectExpressions = selectExpressions;
  }

  public Map<String, Set<Expression>> getAggregationExpressions() {
    return AggregationExpressions;
  }

  public void setAggregationExpressions(Map<String, Set<Expression>> aggregationExpressions) {
    AggregationExpressions = aggregationExpressions;
  }

  public Map<Expression, Set<Expression>> getGroupByLevelExpressions() {
    return groupByLevelExpressions;
  }

  public void setGroupByLevelExpressions(Map<Expression, Set<Expression>> groupByLevelExpressions) {
    this.groupByLevelExpressions = groupByLevelExpressions;
  }

  public FilterNullParameter getFilterNullParameter() {
    return filterNullParameter;
  }

  public void setFilterNullParameter(FilterNullParameter filterNullParameter) {
    this.filterNullParameter = filterNullParameter;
  }

  public List<FillDescriptor> getFillDescriptorList() {
    return fillDescriptorList;
  }

  public void setFillDescriptorList(List<FillDescriptor> fillDescriptorList) {
    this.fillDescriptorList = fillDescriptorList;
  }

  public boolean isHasValueFilter() {
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
}
