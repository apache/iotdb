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

package org.apache.iotdb.db.mpp.plan.plan.distribution;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.Analyzer;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Util {
  public static final Analysis ANALYSIS = constructAnalysis();

  public static Analysis constructAnalysis() {
    try {
      SeriesPartitionExecutor executor =
          SeriesPartitionExecutor.getSeriesPartitionExecutor(
              IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
              IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
      Analysis analysis = new Analysis();

      String device1 = "root.sg.d1";
      String device2 = "root.sg.d22";
      String device3 = "root.sg.d333";
      String device4 = "root.sg.d4444";
      String device5 = "root.sg.d55555";
      String device6 = "root.sg.d666666";

      TRegionReplicaSet dataRegion1 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
              Arrays.asList(
                  genDataNodeLocation(11, "192.0.1.1"), genDataNodeLocation(12, "192.0.1.2")));

      TRegionReplicaSet dataRegion2 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
              Arrays.asList(
                  genDataNodeLocation(21, "192.0.2.1"), genDataNodeLocation(22, "192.0.2.2")));

      TRegionReplicaSet dataRegion3 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
              Arrays.asList(
                  genDataNodeLocation(31, "192.0.3.1"), genDataNodeLocation(32, "192.0.3.2")));

      TRegionReplicaSet dataRegion4 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
              Arrays.asList(
                  genDataNodeLocation(41, "192.0.4.1"), genDataNodeLocation(42, "192.0.4.2")));

      TRegionReplicaSet dataRegion5 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 5),
              Arrays.asList(
                  genDataNodeLocation(51, "192.0.5.1"), genDataNodeLocation(52, "192.0.5.2")));

      DataPartition dataPartition =
          new DataPartition(
              IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
              IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> sgPartitionMap =
          new HashMap<>();

      List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
      d1DataRegions.add(dataRegion1);
      d1DataRegions.add(dataRegion2);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
      d1DataRegionMap.put(new TTimePartitionSlot(), d1DataRegions);

      List<TRegionReplicaSet> d2DataRegions = new ArrayList<>();
      d2DataRegions.add(dataRegion3);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
      d2DataRegionMap.put(new TTimePartitionSlot(), d2DataRegions);

      List<TRegionReplicaSet> d3DataRegions = new ArrayList<>();
      d3DataRegions.add(dataRegion1);
      d3DataRegions.add(dataRegion4);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> d3DataRegionMap = new HashMap<>();
      d3DataRegionMap.put(new TTimePartitionSlot(), d3DataRegions);

      List<TRegionReplicaSet> d4DataRegions = new ArrayList<>();
      d4DataRegions.add(dataRegion1);
      d4DataRegions.add(dataRegion4);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> d4DataRegionMap = new HashMap<>();
      d4DataRegionMap.put(new TTimePartitionSlot(), d4DataRegions);

      List<TRegionReplicaSet> d5DataRegions = new ArrayList<>();
      d5DataRegions.add(dataRegion4);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> d5DataRegionMap = new HashMap<>();
      d5DataRegionMap.put(new TTimePartitionSlot(), d5DataRegions);

      List<TRegionReplicaSet> d6DataRegions = new ArrayList<>();
      d6DataRegions.add(dataRegion1);
      d6DataRegions.add(dataRegion2);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> d6DataRegionMap = new HashMap<>();
      d6DataRegionMap.put(new TTimePartitionSlot(), d6DataRegions);

      sgPartitionMap.put(executor.getSeriesPartitionSlot(device1), d1DataRegionMap);
      sgPartitionMap.put(executor.getSeriesPartitionSlot(device2), d2DataRegionMap);
      sgPartitionMap.put(executor.getSeriesPartitionSlot(device3), d3DataRegionMap);
      sgPartitionMap.put(executor.getSeriesPartitionSlot(device4), d4DataRegionMap);
      sgPartitionMap.put(executor.getSeriesPartitionSlot(device5), d5DataRegionMap);
      sgPartitionMap.put(executor.getSeriesPartitionSlot(device6), d6DataRegionMap);

      dataPartitionMap.put("root.sg", sgPartitionMap);

      dataPartition.setDataPartitionMap(dataPartitionMap);

      analysis.setDataPartitionInfo(dataPartition);

      // construct AggregationExpression for GroupByLevel
      Map<String, Set<Expression>> aggregationExpression = new HashMap<>();
      Set<Expression> s1Expression = new HashSet<>();
      s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")));
      s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d22.s1")));
      s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d333.s1")));
      s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d4444.s1")));

      Set<Expression> s2Expression = new HashSet<>();
      s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")));
      s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d22.s2")));
      s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d333.s2")));
      s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d4444.s2")));

      aggregationExpression.put("root.sg.*.s1", s1Expression);
      aggregationExpression.put("root.sg.*.s2", s2Expression);
      // analysis.setAggregationExpressions(aggregationExpression);

      // construct schema partition
      SchemaPartition schemaPartition =
          new SchemaPartition(
              IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
              IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();

      TRegionReplicaSet schemaRegion1 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 11),
              Arrays.asList(
                  genDataNodeLocation(11, "192.0.1.1"), genDataNodeLocation(12, "192.0.1.2")));

      TRegionReplicaSet schemaRegion2 =
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 21),
              Arrays.asList(
                  genDataNodeLocation(21, "192.0.2.1"), genDataNodeLocation(22, "192.0.2.2")));

      Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaRegionMap = new HashMap<>();
      schemaRegionMap.put(executor.getSeriesPartitionSlot(device1), schemaRegion1);
      schemaRegionMap.put(executor.getSeriesPartitionSlot(device2), schemaRegion2);
      schemaRegionMap.put(executor.getSeriesPartitionSlot(device3), schemaRegion2);
      schemaPartitionMap.put("root.sg", schemaRegionMap);
      schemaPartition.setSchemaPartitionMap(schemaPartitionMap);

      analysis.setDataPartitionInfo(dataPartition);
      analysis.setSchemaPartitionInfo(schemaPartition);
      analysis.setSchemaTree(genSchemaTree());
      return analysis;
    } catch (IllegalPathException e) {
      return new Analysis();
    }
  }

  private static ISchemaTree genSchemaTree() {
    SchemaNode root = new SchemaInternalNode("root");

    SchemaNode sg = new SchemaInternalNode("sg");
    root.addChild("sg", sg);

    SchemaMeasurementNode s1 =
        new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32));
    s1.setTagMap(Collections.singletonMap("key1", "value1"));
    SchemaMeasurementNode s2 =
        new SchemaMeasurementNode("s2", new MeasurementSchema("s2", TSDataType.DOUBLE));
    s2.setTagMap(Collections.singletonMap("key1", "value1"));

    SchemaEntityNode d1 = new SchemaEntityNode("d1");
    sg.addChild("d1", d1);
    d1.addChild("s1", s1);
    d1.addChild("s2", s2);

    SchemaEntityNode d2 = new SchemaEntityNode("d22");
    sg.addChild("d22", d2);
    d2.addChild("s1", s1);
    d2.addChild("s2", s2);

    SchemaEntityNode d3 = new SchemaEntityNode("d333");
    sg.addChild("d333", d3);
    d3.addChild("s1", s1);
    d3.addChild("s2", s2);

    SchemaEntityNode d4 = new SchemaEntityNode("d4444");
    sg.addChild("d4444", d4);
    d4.addChild("s1", s1);
    d4.addChild("s2", s2);

    SchemaEntityNode d5 = new SchemaEntityNode("d55555");
    sg.addChild("d55555", d5);
    d5.addChild("s1", s1);
    d5.addChild("s2", s2);

    SchemaEntityNode d6 = new SchemaEntityNode("d666666");
    d6.setAligned(true);
    sg.addChild("d666666", d6);
    d6.addChild("s1", s1);
    d6.addChild("s2", s2);

    ClusterSchemaTree tree = new ClusterSchemaTree(root);
    tree.setDatabases(Collections.singleton("root.sg"));

    return tree;
  }

  public static Analysis analyze(String sql, MPPQueryContext context) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    Analyzer analyzer = new Analyzer(context, getFakePartitionFetcher(), getFakeSchemaFetcher());
    return analyzer.analyze(statement);
  }

  public static PlanNode genLogicalPlan(Analysis analysis, MPPQueryContext context) {
    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    return planner.plan(analysis).getRootNode();
  }

  private static ISchemaFetcher getFakeSchemaFetcher() {
    return new ISchemaFetcher() {
      @Override
      public ISchemaTree fetchSchema(PathPatternTree patternTree) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public ISchemaTree fetchSchemaWithTags(PathPatternTree patternTree) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public void fetchAndComputeSchemaWithAutoCreate(
          ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation) {}

      @Override
      public void fetchAndComputeSchemaWithAutoCreate(
          List<? extends ISchemaComputationWithAutoCreation>
              schemaComputationWithAutoCreationList) {}

      @Override
      public ISchemaTree fetchSchemaListWithAutoCreate(
          List<PartialPath> devicePath,
          List<String[]> measurements,
          List<TSDataType[]> tsDataTypes,
          List<TSEncoding[]> encodings,
          List<CompressionType[]> compressionTypes,
          List<Boolean> aligned) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path) {
        return null;
      }

      @Override
      public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
        return null;
      }

      @Override
      public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
        return null;
      }

      @Override
      public void invalidAllCache() {}
    };
  }

  private static IPartitionFetcher getFakePartitionFetcher() {
    return new IPartitionFetcher() {
      @Override
      public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
        return ANALYSIS.getSchemaPartitionInfo();
      }

      @Override
      public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
        return ANALYSIS.getSchemaPartitionInfo();
      }

      @Override
      public DataPartition getDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return ANALYSIS.getDataPartitionInfo();
      }

      @Override
      public DataPartition getDataPartitionWithUnclosedTimeRange(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return ANALYSIS.getDataPartitionInfo();
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return ANALYSIS.getDataPartitionInfo();
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          List<DataPartitionQueryParam> dataPartitionQueryParams) {
        return ANALYSIS.getDataPartitionInfo();
      }

      @Override
      public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
          PathPatternTree patternTree, Integer level) {
        return null;
      }

      @Override
      public boolean updateRegionCache(TRegionRouteReq req) {
        return false;
      }

      @Override
      public void invalidAllCache() {}
    };
  }

  private static TDataNodeLocation genDataNodeLocation(int dataNodeId, String ip) {
    return new TDataNodeLocation()
        .setDataNodeId(dataNodeId)
        .setClientRpcEndPoint(new TEndPoint(ip, 9000))
        .setMPPDataExchangeEndPoint(new TEndPoint(ip, 9001))
        .setInternalEndPoint(new TEndPoint(ip, 9002));
  }
}
