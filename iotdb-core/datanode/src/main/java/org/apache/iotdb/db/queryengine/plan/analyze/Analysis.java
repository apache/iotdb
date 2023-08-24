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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

  private DataPartition dataPartition;

  private SchemaPartition schemaPartition;

  private ISchemaTree schemaTree;

  private List<TEndPoint> redirectNodeList;

  // map from output column name (for every node) to its datatype
  private final Map<NodeRef<Expression>, TSDataType> expressionTypes = new LinkedHashMap<>();

  private boolean finishQueryAfterAnalyze;

  // potential fail status when finishQueryAfterAnalyze is true. If failStatus is NULL, means no
  // fail.

  private TSStatus failStatus;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Analysis (used in ALIGN BY TIME)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // map from device name to series/aggregation under this device
  private Set<Expression> sourceExpressions;

  // input expressions of aggregations to be calculated
  private Set<Expression> sourceTransformExpressions = new HashSet<>();

  private Expression whereExpression;

  private Expression groupByExpression;

  // all aggregations that need to be calculated
  private Set<Expression> aggregationExpressions;

  // An ordered map from cross-timeseries aggregation to list of inner-timeseries aggregations. The
  // keys' order is the output one.
  private LinkedHashMap<Expression, Set<Expression>> crossGroupByExpressions;

  // tag keys specified in `GROUP BY TAG` clause
  private List<String> tagKeys;

  /*
  tag values -> (grouped expression -> output expressions)
  For different combination of tag keys, the grouped expression may be different. Let's say there
   are 3 timeseries root.sg.d1.temperature, root.sg.d1.status, root.sg.d2.temperature, and their
   tags are [k1=v1], [k1=v1] and [k1=v2] respectively. For query "SELECT last_value(**) FROM root
   GROUP BY k1", timeseries are grouped by their tags into 2 buckets. Bucket [v1] has
   [root.sg.d1.temperature, root.sg.d1.status], while bucket [v2] has [root.sg.d2.temperature].
   Thus, the aggregation results of bucket [v1] and [v2] are different. Bucket [v1] has 2
   aggregation results last_value(temperature) and last_value(status), whereas bucket [v2] only
  has [last_value(temperature)].
   */
  private Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
      tagValuesToGroupedTimeseriesOperands;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Query Analysis (used in ALIGN BY DEVICE)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // the list of device names
  private List<PartialPath> deviceList;

  // map from output device name to queried devices
  private Map<String, List<String>> outputDeviceToQueriedDevicesMap;

  // map from device name to series/aggregation under this device
  private Map<String, Set<Expression>> deviceToSourceExpressions;

  // input expressions of aggregations to be calculated
  private Map<String, Set<Expression>> deviceToSourceTransformExpressions = new HashMap<>();

  // map from device name to query filter under this device
  private Map<String, Expression> deviceToWhereExpression;

  // all aggregations that need to be calculated
  private Map<String, Set<Expression>> deviceToAggregationExpressions = new HashMap<>();

  // expression of output column to be calculated
  private Map<String, Set<Expression>> deviceToSelectExpressions;

  // expression of group by that need to be calculated
  private Map<String, Expression> deviceToGroupByExpression;

  // expression of order by that need to be calculated
  private Map<String, Set<Expression>> deviceToOrderByExpressions;

  // the sortItems used in order by push down of align by device
  private Map<String, List<SortItem>> deviceToSortItems;

  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  private Map<String, List<Integer>> deviceViewInputIndexesMap;

  private Set<Expression> deviceViewOutputExpressions;

  private final Map<String, Set<Expression>> deviceToOutputExpressions = new HashMap<>();

  // indicates whether DeviceView need special process when rewriteSource in DistributionPlan,
  // you can see SourceRewriter#visitDeviceView to get more information
  private boolean deviceViewSpecialProcess;

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

  // The expressions in order by clause
  // In align by device orderByExpression is the deviceView of expression which doesn't have
  // device-prefix
  // for example, in device root.sg1.d1, [root.sg1.d1.s1] is expression and [s1] is the device-view
  // one.
  private Set<Expression> orderByExpressions;

  private boolean hasSort = false;

  // parameter of `FILL` clause
  private FillDescriptor fillDescriptor;

  // parameter of `GROUP BY TIME` clause
  private GroupByTimeParameter groupByTimeParameter;

  // parameter of `GROUP BY VARIATION` clause
  private GroupByParameter groupByParameter;

  private OrderByParameter mergeOrderParameter;

  // This field will be set and used when the order by in last query only indicates the ordering of
  // timeseries, otherwise it will be null
  private Ordering timeseriesOrderingForLastQuery = null;

  // header of result dataset
  private DatasetHeader respDatasetHeader;

  // indicate whether the Nodes produce source data are VirtualSourceNodes
  private boolean isVirtualSource = false;

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

  // extra message from config node, used for node management
  private Set<TSchemaNode> matchedNodes;

  // template and paths set template
  private Pair<Template, List<PartialPath>> templateSetInfo;

  // devicePath -> <template, paths set template>
  private Map<PartialPath, Pair<Template, PartialPath>> deviceTemplateSetInfoMap;

  // potential template used in timeseries query or fetch
  private Map<Integer, Template> relatedTemplateInfo;

  // generated by combine the input path pattern and template set path
  private List<PartialPath> specifiedTemplateRelatedPathPatternList;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Logical View Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private boolean useLogicalView = false;

  private List<Pair<Expression, String>> outputExpressions = null;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Show Queries Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // extra message from config node, queries wll be sent to these Running DataNodes
  private List<TDataNodeLocation> runningDataNodeLocations;

  // used for limit and offset push down optimizer, if we select all columns from aligned device, we
  // can use statistics to skip
  private boolean lastLevelUseWildcard = false;

  public Analysis() {
    this.finishQueryAfterAnalyze = false;
  }

  public List<TRegionReplicaSet> getPartitionInfo(PartialPath seriesPath, Filter timefilter) {
    // TODO: (xingtanzjr) implement the calculation of timePartitionIdList
    return dataPartition.getDataRegionReplicaSet(seriesPath.getDevice(), null);
  }

  public List<TRegionReplicaSet> getPartitionInfo(String deviceName, Filter globalTimeFilter) {
    // TODO: (xingtanzjr) implement the calculation of timePartitionIdList
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

  public List<TEndPoint> getRedirectNodeList() {
    return redirectNodeList;
  }

  public void setRedirectNodeList(List<TEndPoint> redirectNodeList) {
    this.redirectNodeList = redirectNodeList;
  }

  public void addEndPointToRedirectNodeList(TEndPoint endPoint) {
    if (redirectNodeList == null) {
      redirectNodeList = new ArrayList<>();
    }
    redirectNodeList.add(endPoint);
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
    // NULL_Operand needn't check
    if (expression.getExpressionType() == ExpressionType.NULL) {
      return null;
    }
    TSDataType type = expressionTypes.get(NodeRef.of(expression));
    checkArgument(type != null, "Expression not analyzed: %s", expression);
    return type;
  }

  public boolean hasDataSource() {
    return (dataPartition != null && !dataPartition.isEmpty())
        || (schemaPartition != null && !schemaPartition.isEmpty())
        || statement instanceof ShowQueriesStatement
        || (statement instanceof QueryStatement
            && ((QueryStatement) statement).isAggregationQuery());
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

  public void setGroupByParameter(GroupByParameter groupByParameter) {
    this.groupByParameter = groupByParameter;
  }

  public GroupByParameter getGroupByParameter() {
    return groupByParameter;
  }

  public boolean hasGroupByParameter() {
    return groupByParameter != null;
  }

  public boolean isFinishQueryAfterAnalyze() {
    return finishQueryAfterAnalyze;
  }

  public void setFinishQueryAfterAnalyze(boolean finishQueryAfterAnalyze) {
    this.finishQueryAfterAnalyze = finishQueryAfterAnalyze;
  }

  public boolean isFailed() {
    return failStatus != null;
  }

  public TSStatus getFailStatus() {
    return this.failStatus;
  }

  public void setFailStatus(TSStatus status) {
    this.failStatus = status;
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

  public Expression getGroupByExpression() {
    return groupByExpression;
  }

  public void setGroupByExpression(Expression groupByExpression) {
    this.groupByExpression = groupByExpression;
  }

  public Map<String, Expression> getDeviceToGroupByExpression() {
    return deviceToGroupByExpression;
  }

  public void setDeviceToGroupByExpression(Map<String, Expression> deviceToGroupByExpression) {
    this.deviceToGroupByExpression = deviceToGroupByExpression;
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

  public Map<PartialPath, Pair<Template, PartialPath>> getDeviceTemplateSetInfoMap() {
    return deviceTemplateSetInfoMap;
  }

  public void setDeviceTemplateSetInfoMap(
      Map<PartialPath, Pair<Template, PartialPath>> deviceTemplateSetInfoMap) {
    this.deviceTemplateSetInfoMap = deviceTemplateSetInfoMap;
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

  public void setExpressionType(Expression expression, TSDataType type) {
    this.expressionTypes.put(NodeRef.of(expression), type);
  }

  public Set<Expression> getDeviceViewOutputExpressions() {
    return deviceViewOutputExpressions;
  }

  public void setDeviceViewOutputExpressions(Set<Expression> deviceViewOutputExpressions) {
    this.deviceViewOutputExpressions = deviceViewOutputExpressions;
  }

  public boolean isDeviceViewSpecialProcess() {
    return deviceViewSpecialProcess;
  }

  public void setDeviceViewSpecialProcess(boolean deviceViewSpecialProcess) {
    this.deviceViewSpecialProcess = deviceViewSpecialProcess;
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

  public Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
      getTagValuesToGroupedTimeseriesOperands() {
    return tagValuesToGroupedTimeseriesOperands;
  }

  public void setTagValuesToGroupedTimeseriesOperands(
      Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
          tagValuesToGroupedTimeseriesOperands) {
    this.tagValuesToGroupedTimeseriesOperands = tagValuesToGroupedTimeseriesOperands;
  }

  public List<TDataNodeLocation> getRunningDataNodeLocations() {
    return runningDataNodeLocations;
  }

  public void setRunningDataNodeLocations(List<TDataNodeLocation> runningDataNodeLocations) {
    this.runningDataNodeLocations = runningDataNodeLocations;
  }

  public boolean isVirtualSource() {
    return isVirtualSource;
  }

  public void setVirtualSource(boolean virtualSource) {
    isVirtualSource = virtualSource;
  }

  public Map<NodeRef<Expression>, TSDataType> getExpressionTypes() {
    return expressionTypes;
  }

  public void setOrderByExpressions(Set<Expression> orderByExpressions) {
    this.orderByExpressions = orderByExpressions;
  }

  public Set<Expression> getOrderByExpressions() {
    return orderByExpressions;
  }

  public Map<String, Set<Expression>> getDeviceToOrderByExpressions() {
    return deviceToOrderByExpressions;
  }

  public void setDeviceToOrderByExpressions(
      Map<String, Set<Expression>> deviceToOrderByExpressions) {
    this.deviceToOrderByExpressions = deviceToOrderByExpressions;
  }

  public void setHasSort(boolean hasSort) {
    this.hasSort = hasSort;
  }

  public boolean isHasSort() {
    return hasSort;
  }

  public Map<String, List<SortItem>> getDeviceToSortItems() {
    return deviceToSortItems;
  }

  public void setDeviceToSortItems(Map<String, List<SortItem>> deviceToSortItems) {
    this.deviceToSortItems = deviceToSortItems;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Logical View Analysis
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public void setUseLogicalView(boolean useLogicalView) {
    this.useLogicalView = useLogicalView;
  }

  public boolean useLogicalView() {
    return this.useLogicalView;
  }

  public void setOutputExpressions(List<Pair<Expression, String>> outputExpressions) {
    this.outputExpressions = outputExpressions;
  }

  public List<Pair<Expression, String>> getOutputExpressions() {
    return this.outputExpressions;
  }

  public Ordering getTimeseriesOrderingForLastQuery() {
    return timeseriesOrderingForLastQuery;
  }

  public void setTimeseriesOrderingForLastQuery(Ordering timeseriesOrderingForLastQuery) {
    this.timeseriesOrderingForLastQuery = timeseriesOrderingForLastQuery;
  }

  public Map<String, List<String>> getOutputDeviceToQueriedDevicesMap() {
    return outputDeviceToQueriedDevicesMap;
  }

  public void setOutputDeviceToQueriedDevicesMap(
      Map<String, List<String>> outputDeviceToQueriedDevicesMap) {
    this.outputDeviceToQueriedDevicesMap = outputDeviceToQueriedDevicesMap;
  }

  public Map<String, Set<Expression>> getDeviceToOutputExpressions() {
    return deviceToOutputExpressions;
  }

  public boolean isLastLevelUseWildcard() {
    return lastLevelUseWildcard;
  }

  public void setLastLevelUseWildcard(boolean lastLevelUseWildcard) {
    this.lastLevelUseWildcard = lastLevelUseWildcard;
  }

  public void setDeviceList(List<PartialPath> deviceList) {
    this.deviceList = deviceList;
  }

  public List<PartialPath> getDeviceList() {
    return deviceList;
  }
}
