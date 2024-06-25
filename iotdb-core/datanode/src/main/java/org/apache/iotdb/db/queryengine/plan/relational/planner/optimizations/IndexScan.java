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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;

/** Extract IDeviceID */
public class IndexScan implements RelationalPlanOptimizer {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      SessionInfo sessionInfo,
      MPPQueryContext queryContext) {
    return planNode.accept(new Rewriter(metadata, analysis, queryContext), null);
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Void> {

    private final Metadata metadata;
    private final Analysis analysis;
    private final MPPQueryContext queryContext;

    Rewriter(Metadata metadata, Analysis analysis, MPPQueryContext queryContext) {
      this.metadata = metadata;
      this.analysis = analysis;
      this.queryContext = queryContext;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Void context) {
      List<Expression> metadataExpressions =
          analysis.getTableModelPredicateExpressions() == null
                  || analysis.getTableModelPredicateExpressions().get(0).isEmpty()
              ? Collections.emptyList()
              : analysis.getTableModelPredicateExpressions().get(0);
      List<String> attributeColumns =
          node.getOutputSymbols().stream()
              .filter(
                  symbol -> ATTRIBUTE.equals(node.getAssignments().get(symbol).getColumnCategory()))
              .map(Symbol::getName)
              .collect(Collectors.toList());
      List<DeviceEntry> deviceEntries =
          metadata.indexScan(node.getQualifiedObjectName(), metadataExpressions, attributeColumns);
      node.setDeviceEntries(deviceEntries);
      if (deviceEntries.isEmpty()) {
        analysis.setFinishQueryAfterAnalyze();
        analysis.setEmptyDataSource(true);
      } else {
        String treeModelDatabase = "root." + node.getQualifiedObjectName().getDatabaseName();
        DataPartition dataPartition =
            fetchDataPartitionByDevices(
                deviceEntries, treeModelDatabase, queryContext.getGlobalTimeFilter());

        if (dataPartition.getDataPartitionMap().size() > 1) {
          throw new IllegalStateException(
              "Table model can only process data only in one database yet!");
        }

        if (dataPartition.getDataPartitionMap().isEmpty()) {
          analysis.setFinishQueryAfterAnalyze();
          analysis.setEmptyDataSource(true);
        } else {
          Set<TRegionReplicaSet> regionReplicaSet = new HashSet<>();
          for (Map.Entry<
                  String,
                  Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
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
      }

      return node;
    }

    private DataPartition fetchDataPartitionByDevices(
        List<DeviceEntry> deviceEntries, String database, Filter globalTimeFilter) {
      Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
          getTimePartitionSlotList(globalTimeFilter, queryContext);

      // there is no satisfied time range
      if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
        return new DataPartition(
            Collections.emptyMap(),
            CONFIG.getSeriesPartitionExecutorClass(),
            CONFIG.getSeriesPartitionSlotNum());
      }

      List<DataPartitionQueryParam> dataPartitionQueryParams =
          deviceEntries.stream()
              .map(
                  deviceEntry ->
                      new DataPartitionQueryParam(
                          deviceEntry.getDeviceID(), res.left, res.right.left, res.right.right))
              .collect(Collectors.toList());
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap =
          Collections.singletonMap(database, dataPartitionQueryParams);

      if (res.right.left || res.right.right) {
        return metadata.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
      } else {
        return metadata.getDataPartition(sgNameToQueryParamsMap);
      }
    }
  }
}
