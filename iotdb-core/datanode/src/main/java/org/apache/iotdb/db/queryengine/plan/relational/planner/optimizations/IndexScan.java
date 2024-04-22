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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;

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

/** Extract IDeviceID and */
public class IndexScan implements RelationalPlanOptimizer {

  static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private MPPQueryContext mppQueryContext;

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      SessionInfo sessionInfo,
      MPPQueryContext context) {

    return planNode.accept(
        new Rewriter(), new RewriterContext(null, metadata, sessionInfo, analysis));
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
      node.getChild().accept(this, context);
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      List<String> attributeColumns =
          node.getAssignments().entrySet().stream()
              .filter(e -> e.getValue().getColumnCategory().equals(ATTRIBUTE))
              .map(e -> e.getKey().getName())
              .collect(Collectors.toList());

      // TODO extract predicate to expression list
      List<DeviceEntry> deviceEntries =
          context
              .getMetadata()
              .indexScan(
                  new QualifiedObjectName(
                      context.getSessionInfo().getDatabaseName().get(),
                      node.getQualifiedTableName()),
                  Collections.singletonList(context.getPredicate()),
                  attributeColumns);
      node.setDeviceEntries(deviceEntries);

      // TODO getDataPartition, Change globalTimeFilter to Filter
      String database = "root." + context.getSessionInfo().getDatabaseName().get();
      IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
      Filter globalTimeFilter = null;
      Set<String> deviceSet = new HashSet<>();
      for (DeviceEntry deviceEntry : deviceEntries) {
        StringArrayDeviceID arrayDeviceID = (StringArrayDeviceID) deviceEntry.getDeviceID();
        String device = arrayDeviceID.toString();
        deviceSet.add("root." + device);
      }

      DataPartition dataPartition =
          fetchDataPartitionByDevices(deviceSet, database, globalTimeFilter, partitionFetcher);
      context.getAnalysis().setDataPartition(dataPartition);

      if (dataPartition.getDataPartitionMap().size() != 1) {
        throw new IllegalStateException("Table model can only process data only in data region!");
      }

      // TODO add the real impl
      TRegionReplicaSet regionReplicaSet =
          dataPartition
              .getDataPartitionMap()
              .values()
              .iterator()
              .next()
              .values()
              .iterator()
              .next()
              .values()
              .iterator()
              .next()
              .get(0);
      node.setRegionReplicaSet(regionReplicaSet);

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

    //    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
    //            dataPartitionMap = new HashMap<>();
    //    dataPartitionMap.put("root.db", Collections.singletonMap(new TSeriesPartitionSlot(1),
    //            Collections.singletonMap(new TTimePartitionSlot(1),
    //                    Collections.singletonList(new TRegionReplicaSet(
    //                            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
    //                            Arrays.asList(genDataNodeLocation(1, "127.0.0.1")))))));
    //    return new DataPartition(dataPartitionMap, "hkb", 1);

    if (res.right.left || res.right.right) {
      return partitionFetcher.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
    } else {
      return partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
    }
  }

  private static TDataNodeLocation genDataNodeLocation(int dataNodeId, String ip) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(new TEndPoint(ip, 9000))
        .setMPPDataExchangeEndPoint(new TEndPoint(ip, 9001))
        .setInternalEndPoint(new TEndPoint(ip, 9002));
  }

  private static class RewriterContext {
    private Expression predicate;
    private Metadata metadata;
    private final SessionInfo sessionInfo;
    private Analysis analysis;

    RewriterContext(
        Expression predicate, Metadata metadata, SessionInfo sessionInfo, Analysis analysis) {
      this.predicate = predicate;
      this.metadata = metadata;
      this.sessionInfo = sessionInfo;
      this.analysis = analysis;
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
  }
}
