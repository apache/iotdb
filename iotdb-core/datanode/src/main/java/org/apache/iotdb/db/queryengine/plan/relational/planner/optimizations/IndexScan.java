/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;

/** Extract IDeviceID */
public class IndexScan implements RelationalPlanOptimizer {

  static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      IPartitionFetcher partitionFetcher,
      SessionInfo sessionInfo,
      MPPQueryContext queryContext) {

    return planNode.accept(
        new Rewriter(),
        new RewriterContext(null, metadata, sessionInfo, analysis, partitionFetcher, queryContext));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      context.setPredicate(node.getPredicate());
      context.setFilterNode(node);
      node.getChild().accept(this, context);
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {

      // only when exist diff predicate in FilterNode, context.predicate will not equal null
      if (context.predicate == null) {
        context.predicate = node.getPushDownPredicate();
      }

      List<Expression> metadataExpressions =
          context.queryContext.getTableModelPredicateExpressions() == null
                  || context.queryContext.getTableModelPredicateExpressions().get(0).isEmpty()
              ? Collections.emptyList()
              : context.queryContext.getTableModelPredicateExpressions().get(0);
      String dbName = context.getSessionInfo().getDatabaseName().get();
      List<String> attributeColumns =
          node.getOutputSymbols().stream()
              .filter(
                  symbol -> ATTRIBUTE.equals(node.getAssignments().get(symbol).getColumnCategory()))
              .map(Symbol::getName)
              .collect(Collectors.toList());
      List<DeviceEntry> deviceEntries =
          context
              .getMetadata()
              .indexScan(
                  new QualifiedObjectName(dbName, node.getQualifiedTableName()),
                  metadataExpressions,
                  attributeColumns);
      node.setDeviceEntries(deviceEntries);

      String treeModelDatabase = "root." + dbName;
      Set<String> deviceSet = new HashSet<>();
      for (DeviceEntry deviceEntry : deviceEntries) {
        StringArrayDeviceID arrayDeviceID = (StringArrayDeviceID) deviceEntry.getDeviceID();
        String device = arrayDeviceID.toString();
        deviceSet.add("root." + device);
      }

      DataPartition dataPartition =
          fetchDataPartitionByDevices(
              deviceSet,
              treeModelDatabase,
              context.getQueryContext().getGlobalTimeFilter(),
              context.getPartitionFetcher());
      context.getAnalysis().setDataPartition(dataPartition);

      if (dataPartition.getDataPartitionMap().size() > 1) {
        throw new IllegalStateException(
            "Table model can only process data only in one database yet!");
      }

      if (dataPartition.getDataPartitionMap().isEmpty()) {
        context.getAnalysis().setFinishQueryAfterAnalyze();
      } else {
        Set<TRegionReplicaSet> regionReplicaSet = new HashSet<>();
        for (Map.Entry<
                String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
            e1 : dataPartition.getDataPartitionMap().entrySet()) {
          for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
              e2 : e1.getValue().entrySet()) {
            for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> e3 :
                e2.getValue().entrySet()) {
              regionReplicaSet.addAll(e3.getValue());
            }
          }
        }
        node.setRegionReplicaSetList(new ArrayList<>(regionReplicaSet));
      }

      return node;
    }
  }

  private static DataPartition fetchDataPartitionByDevices(
      Set<String> deviceSet,
      String database,
      Filter globalTimeFilter,
      IPartitionFetcher partitionFetcher) {
    Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
        getTimePartitionSlotList(globalTimeFilter);

    // there is no satisfied time range
    if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
      return new DataPartition(
          Collections.emptyMap(),
          CONFIG.getSeriesPartitionExecutorClass(),
          CONFIG.getSeriesPartitionSlotNum());
    }

    Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
    for (String devicePath : deviceSet) {
      DataPartitionQueryParam queryParam =
          new DataPartitionQueryParam(devicePath, res.left, res.right.left, res.right.right);
      sgNameToQueryParamsMap.computeIfAbsent(database, key -> new ArrayList<>()).add(queryParam);
    }

    if (res.right.left || res.right.right) {
      return partitionFetcher.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
    } else {
      return partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
    }
  }

  private static class RewriterContext {
    private Expression predicate;
    private Metadata metadata;
    private final SessionInfo sessionInfo;
    private final Analysis analysis;
    private final IPartitionFetcher partitionFetcher;
    private final MPPQueryContext queryContext;
    private FilterNode filterNode;

    RewriterContext(
        Expression predicate,
        Metadata metadata,
        SessionInfo sessionInfo,
        Analysis analysis,
        IPartitionFetcher partitionFetcher,
        MPPQueryContext queryContext) {
      this.predicate = predicate;
      this.metadata = metadata;
      this.sessionInfo = sessionInfo;
      this.analysis = analysis;
      this.partitionFetcher = partitionFetcher;
      this.queryContext = queryContext;
    }

    public Expression getPredicate() {
      return this.predicate;
    }

    public void setPredicate(Expression predicate) {
      this.predicate = predicate;
    }

    public Metadata getMetadata() {
      return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
    }

    public SessionInfo getSessionInfo() {
      return this.sessionInfo;
    }

    public Analysis getAnalysis() {
      return this.analysis;
    }

    public IPartitionFetcher getPartitionFetcher() {
      return partitionFetcher;
    }

    public MPPQueryContext getQueryContext() {
      return queryContext;
    }

    public FilterNode getFilterNode() {
      return filterNode;
    }

    public void setFilterNode(FilterNode filterNode) {
      this.filterNode = filterNode;
    }
  }
}
