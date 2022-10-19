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
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

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

  private ISchemaTree schemaTree;

  // map from output column name (for every node) to its datatype
  private final Map<NodeRef<Expression>, TSDataType> expressionTypes = new LinkedHashMap<>();

  private boolean finishQueryAfterAnalyze;

  // potential fail message when finishQueryAfterAnalyze is true. If failMessage is NULL, means no
  // fail.
  private String failMessage;

  // As every result column will be analyzed with path concatenation and wildcard removing to
  // generate one or more output expressions. We can maintain a map from output expression to
  // original result column for later use.
  // Note that if multiple expressions come from one single ResultColumn, the right object's
  // REFERENCE should be equal, aka outputExpressions.get(i).right ==
  // outputExpressions.get(j).right, NOT VALUE equal.
  private List<Pair<Expression, ResultColumn>> outputExpressions = new ArrayList<>();

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Analysis (used in ALIGN BY TIME)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // map from device name to series/aggregation under this device
  private Set<Expression> sourceExpressions;

  // input expressions of aggregations to be calculated
  private Set<Expression> sourceTransformExpressions;

  private Expression whereExpression;

  // all aggregations that need to be calculated
  private Set<Expression> aggregationExpressions;

  // An ordered map from cross-timeseries aggregation to list of inner-timeseries aggregations. The
  // keys' order is the output one.
  private LinkedHashMap<Expression, Set<Expression>> crossGroupByExpressions;

  // tag keys specified in `GROUP BY TAG` clause
  private List<String> tagKeys;

  // {tag values -> {grouped expression -> source timeseries expressions}}
  // For different combination of tag keys, the grouped expression may be different. Let's say there
  // are 3 timeseries root.sg.d1.temperature, root.sg.d1.status, root.sg.d2.temperature, and their
  // tags are [k1=v1], [k1=v1] and [k1=v2] respectively. For query "SELECT last_value(**) FROM root
  // GROUP BY k1", timeseries are grouped by their tags into 2 buckets. Bucket [v1] has
  // [root.sg.d1.temperature, root.sg.d1.status], while bucket [v2] has [root.sg.d2.temperature].
  // Thus, the aggregation results of bucket [v1] and [v2] are different. Bucket [v1] has 2
  // aggregation results last_value(temperature) and last_value(status), whereas bucket [v2] only
  // has [last_value(temperature)].
  private Map<List<String>, LinkedHashMap<Expression, Set<Expression>>>
      tagValuesToGroupedTimeseriesOperands;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Analysis (used in ALIGN BY DEVICE)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // map from device name to series/aggregation under this device
  private Map<String, Set<Expression>> deviceToSourceExpressions;

  // input expressions of aggregations to be calculated
  private Map<String, Set<Expression>> deviceToSourceTransformExpressions;

  // map from device name to query filter under this device
  private Map<String, Expression> deviceToWhereExpression;

  // all aggregations that need to be calculated
  private Map<String, Set<Expression>> deviceToAggregationExpressions;

  // expression of output column to be calculated
  private Map<String, Set<Expression>> deviceToSelectExpressions;

  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  private Map<String, List<Integer>> deviceViewInputIndexesMap;

  private Set<Expression> deviceViewOutputExpressions;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Common Analysis (above DeviceView)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // indicate is there a value filter
  private boolean hasValueFilter = false;

  // a global time filter used in `initQueryDataSource` and filter push down
  private Filter globalTimeFilter;

  // expression of output column to be calculated
  private Set<Expression> selectExpressions;

  private Expression havingExpression;

  // parameter of `FILL` clause
  private FillDescriptor fillDescriptor;

  // parameter of `GROUP BY TIME` clause
  private GroupByTimeParameter groupByTimeParameter;

  private OrderByParameter mergeOrderParameter;

  // header of result dataset
  private DatasetHeader respDatasetHeader;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // SELECT INTO Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // used in ALIGN BY DEVICE
  private DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor;

  // used in ALIGN BY TIME
  private IntoPathDescriptor intoPathDescriptor;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Schema Query Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // extra mesaage from config node, used for node management
  private Set<TSchemaNode> matchedNodes;

  // template and paths set template
  private Pair<Template, List<PartialPath>> templateSetInfo;

  // potential template used in timeseries query or fetch
  private Map<Integer, Template> relatedTemplateInfo;

  // generated by combine the input path pattern and template set path
  private List<PartialPath> specifiedTemplateRelatedPathPatternList;

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

  public ISchemaTree getSchemaTree() {
    return schemaTree;
  }

  public void setSchemaTree(ISchemaTree schemaTree) {
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

  public TSDataType getType(Expression expression) {
    TSDataType type = expressionTypes.get(NodeRef.of(expression));
    checkArgument(type != null, "Expression not analyzed: %s", expression);
    return type;
  }

  public boolean hasDataSource() {
    return (dataPartition != null && !dataPartition.isEmpty())
        || (schemaPartition != null && !schemaPartition.isEmpty());
  }

  public LinkedHashMap<Expression, Set<Expression>> getCrossGroupByExpressions() {
    return crossGroupByExpressions;
  }

  public void setCrossGroupByExpressions(
      LinkedHashMap<Expression, Set<Expression>> crossGroupByExpressions) {
    this.crossGroupByExpressions = crossGroupByExpressions;
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

  public Expression getWhereExpression() {
    return whereExpression;
  }

  public void setWhereExpression(Expression whereExpression) {
    this.whereExpression = whereExpression;
  }

  public Map<String, Expression> getDeviceToWhereExpression() {
    return deviceToWhereExpression;
  }

  public void setDeviceToWhereExpression(Map<String, Expression> deviceToWhereExpression) {
    this.deviceToWhereExpression = deviceToWhereExpression;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public Expression getHavingExpression() {
    return havingExpression;
  }

  public void setHavingExpression(Expression havingExpression) {
    this.havingExpression = havingExpression;
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

  public boolean isFailed() {
    return failMessage != null;
  }

  public String getFailMessage() {
    return failMessage;
  }

  public void setFailMessage(String failMessage) {
    this.failMessage = failMessage;
  }

  public void setDeviceViewInputIndexesMap(Map<String, List<Integer>> deviceViewInputIndexesMap) {
    this.deviceViewInputIndexesMap = deviceViewInputIndexesMap;
  }

  public Map<String, List<Integer>> getDeviceViewInputIndexesMap() {
    return deviceViewInputIndexesMap;
  }

  public Set<Expression> getSourceExpressions() {
    return sourceExpressions;
  }

  public void setSourceExpressions(Set<Expression> sourceExpressions) {
    this.sourceExpressions = sourceExpressions;
  }

  public Set<Expression> getSourceTransformExpressions() {
    return sourceTransformExpressions;
  }

  public void setSourceTransformExpressions(Set<Expression> sourceTransformExpressions) {
    this.sourceTransformExpressions = sourceTransformExpressions;
  }

  public Set<Expression> getAggregationExpressions() {
    return aggregationExpressions;
  }

  public void setAggregationExpressions(Set<Expression> aggregationExpressions) {
    this.aggregationExpressions = aggregationExpressions;
  }

  public Set<Expression> getSelectExpressions() {
    return selectExpressions;
  }

  public void setSelectExpressions(Set<Expression> selectExpressions) {
    this.selectExpressions = selectExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToSourceExpressions() {
    return deviceToSourceExpressions;
  }

  public void setDeviceToSourceExpressions(Map<String, Set<Expression>> deviceToSourceExpressions) {
    this.deviceToSourceExpressions = deviceToSourceExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToSourceTransformExpressions() {
    return deviceToSourceTransformExpressions;
  }

  public void setDeviceToSourceTransformExpressions(
      Map<String, Set<Expression>> deviceToSourceTransformExpressions) {
    this.deviceToSourceTransformExpressions = deviceToSourceTransformExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToAggregationExpressions() {
    return deviceToAggregationExpressions;
  }

  public void setDeviceToAggregationExpressions(
      Map<String, Set<Expression>> deviceToAggregationExpressions) {
    this.deviceToAggregationExpressions = deviceToAggregationExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToSelectExpressions() {
    return deviceToSelectExpressions;
  }

  public void setDeviceToSelectExpressions(Map<String, Set<Expression>> deviceToSelectExpressions) {
    this.deviceToSelectExpressions = deviceToSelectExpressions;
  }

  public Set<TSchemaNode> getMatchedNodes() {
    return matchedNodes;
  }

  public void setMatchedNodes(Set<TSchemaNode> matchedNodes) {
    this.matchedNodes = matchedNodes;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  public void setMergeOrderParameter(OrderByParameter mergeOrderParameter) {
    this.mergeOrderParameter = mergeOrderParameter;
  }

  public Pair<Template, List<PartialPath>> getTemplateSetInfo() {
    return templateSetInfo;
  }

  public void setTemplateSetInfo(Pair<Template, List<PartialPath>> templateSetInfo) {
    this.templateSetInfo = templateSetInfo;
  }

  public Map<Integer, Template> getRelatedTemplateInfo() {
    return relatedTemplateInfo;
  }

  public void setRelatedTemplateInfo(Map<Integer, Template> relatedTemplateInfo) {
    this.relatedTemplateInfo = relatedTemplateInfo;
  }

  public List<PartialPath> getSpecifiedTemplateRelatedPathPatternList() {
    return specifiedTemplateRelatedPathPatternList;
  }

  public void setSpecifiedTemplateRelatedPathPatternList(
      List<PartialPath> specifiedTemplateRelatedPathPatternList) {
    this.specifiedTemplateRelatedPathPatternList = specifiedTemplateRelatedPathPatternList;
  }

  public void addTypes(Map<NodeRef<Expression>, TSDataType> types) {
    this.expressionTypes.putAll(types);
  }

  public Set<Expression> getDeviceViewOutputExpressions() {
    return deviceViewOutputExpressions;
  }

  public void setDeviceViewOutputExpressions(Set<Expression> deviceViewOutputExpressions) {
    this.deviceViewOutputExpressions = deviceViewOutputExpressions;
  }

  public DeviceViewIntoPathDescriptor getDeviceViewIntoPathDescriptor() {
    return deviceViewIntoPathDescriptor;
  }

  public void setDeviceViewIntoPathDescriptor(
      DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor) {
    this.deviceViewIntoPathDescriptor = deviceViewIntoPathDescriptor;
  }

  public IntoPathDescriptor getIntoPathDescriptor() {
    return intoPathDescriptor;
  }

  public void setIntoPathDescriptor(IntoPathDescriptor intoPathDescriptor) {
    this.intoPathDescriptor = intoPathDescriptor;
  }

  public List<String> getTagKeys() {
    return tagKeys;
  }

  public void setTagKeys(List<String> tagKeys) {
    this.tagKeys = tagKeys;
  }

  public Map<List<String>, LinkedHashMap<Expression, Set<Expression>>>
      getTagValuesToGroupedTimeseriesOperands() {
    return tagValuesToGroupedTimeseriesOperands;
  }

  public void setTagValuesToGroupedTimeseriesOperands(
      Map<List<String>, LinkedHashMap<Expression, Set<Expression>>>
          tagValuesToGroupedTimeseriesOperands) {
    this.tagValuesToGroupedTimeseriesOperands = tagValuesToGroupedTimeseriesOperands;
  }

  public List<Pair<Expression, ResultColumn>> getOutputExpressions() {
    return outputExpressions;
  }

  public void setOutputExpressions(List<Pair<Expression, ResultColumn>> outputExpressions) {
    this.outputExpressions = outputExpressions;
  }
}
