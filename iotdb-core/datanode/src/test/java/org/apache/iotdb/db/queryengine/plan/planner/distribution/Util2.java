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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.mockito.Mockito;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Util2 {
  public static final Analysis ANALYSIS = constructAnalysis();

  private static final String device1 = "root.sg.d1";
  private static final String device2 = "root.sg.d2";
  private static final String device3 = "root.sg.d3";

  public static Analysis constructAnalysis() {
    TRegionReplicaSet dataRegion1 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Collections.singletonList(genDataNodeLocation(11, "192.0.1.1")));
    List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
    d1DataRegions.add(dataRegion1);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
    d1DataRegionMap.put(new TTimePartitionSlot(), d1DataRegions);

    TRegionReplicaSet dataRegion2 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            Collections.singletonList(genDataNodeLocation(21, "192.0.1.1")));
    List<TRegionReplicaSet> d2DataRegions = new ArrayList<>();
    d2DataRegions.add(dataRegion2);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> d2DataRegionMap = new HashMap<>();
    d2DataRegionMap.put(new TTimePartitionSlot(), d2DataRegions);

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> sgPartitionMap =
        new HashMap<>();

    SeriesPartitionExecutor executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device1), d1DataRegionMap);
    sgPartitionMap.put(executor.getSeriesPartitionSlot(device2), d2DataRegionMap);
    DataPartition dataPartition =
        new DataPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    dataPartitionMap.put("root.sg", sgPartitionMap);
    dataPartition.setDataPartitionMap(dataPartitionMap);

    Analysis analysis = new Analysis();
    analysis.setDataPartitionInfo(dataPartition);

    // construct schema partition
    TRegionReplicaSet schemaRegion1 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 11),
            Collections.singletonList(genDataNodeLocation(11, "192.0.1.1")));

    TRegionReplicaSet schemaRegion2 =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 21),
            Collections.singletonList(genDataNodeLocation(21, "192.0.1.1")));

    Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaRegionMap = new HashMap<>();
    schemaRegionMap.put(executor.getSeriesPartitionSlot(device1), schemaRegion1);
    schemaRegionMap.put(executor.getSeriesPartitionSlot(device2), schemaRegion2);
    schemaRegionMap.put(executor.getSeriesPartitionSlot(device3), schemaRegion2);
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();
    schemaPartitionMap.put("root.sg", schemaRegionMap);
    SchemaPartition schemaPartition =
        new SchemaPartition(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    schemaPartition.setSchemaPartitionMap(schemaPartitionMap);

    analysis.setDataPartitionInfo(dataPartition);
    analysis.setSchemaPartitionInfo(schemaPartition);
    analysis.setSchemaTree(genSchemaTree());
    // to avoid some special case which is not the point of test
    analysis.setRealStatement(Mockito.mock(QueryStatement.class));
    Mockito.when(analysis.getTreeStatement().isQuery()).thenReturn(false);
    return analysis;
  }

  private static ISchemaTree genSchemaTree() {
    SchemaNode root = new SchemaInternalNode("root");

    SchemaNode sg = new SchemaInternalNode("sg");
    root.addChild("sg", sg);

    SchemaEntityNode d1 = new SchemaEntityNode("d1");
    SchemaMeasurementNode s1 =
        new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32));
    SchemaMeasurementNode s2 =
        new SchemaMeasurementNode("s2", new MeasurementSchema("s2", TSDataType.DOUBLE));
    sg.addChild("d1", d1);
    d1.addChild("s1", s1);
    d1.addChild("s2", s2);

    SchemaEntityNode d2 = new SchemaEntityNode("d2");
    SchemaMeasurementNode t1 =
        new SchemaMeasurementNode("s1", new MeasurementSchema("t1", TSDataType.INT32));
    SchemaMeasurementNode t2 =
        new SchemaMeasurementNode("s2", new MeasurementSchema("t2", TSDataType.DOUBLE));
    SchemaMeasurementNode t3 =
        new SchemaMeasurementNode("s3", new MeasurementSchema("t3", TSDataType.DOUBLE));
    sg.addChild("d2", d2);
    d2.addChild("s1", t1);
    d2.addChild("s2", t2);
    d2.addChild("s3", t3);

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
    LogicalPlanner planner = new LogicalPlanner(context);
    return planner.plan(analysis).getRootNode();
  }

  private static ISchemaFetcher getFakeSchemaFetcher() {
    return new ISchemaFetcher() {
      @Override
      public ISchemaTree fetchSchema(
          PathPatternTree patternTree, boolean withTemplate, MPPQueryContext context) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public ISchemaTree fetchRawSchemaInDeviceLevel(
          PathPatternTree patternTree, PathPatternTree authorityScope, MPPQueryContext context) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public ISchemaTree fetchRawSchemaInMeasurementLevel(
          PathPatternTree patternTree, PathPatternTree authorityScope, MPPQueryContext context) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public ISchemaTree fetchSchemaWithTags(
          PathPatternTree patternTree, boolean withTemplate, MPPQueryContext context) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public void fetchAndComputeSchemaWithAutoCreate(
          ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation,
          MPPQueryContext context) {}

      @Override
      public void fetchAndComputeSchemaWithAutoCreate(
          List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList,
          MPPQueryContext context) {}

      @Override
      public ISchemaTree fetchSchemaListWithAutoCreate(
          List<PartialPath> devicePath,
          List<String[]> measurements,
          List<TSDataType[]> tsDataTypes,
          List<TSEncoding[]> encodings,
          List<CompressionType[]> compressionTypes,
          List<Boolean> aligned,
          MPPQueryContext context) {
        return ANALYSIS.getSchemaTree();
      }

      @Override
      public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath devicePath) {
        return null;
      }

      @Override
      public Pair<Template, PartialPath> checkTemplateSetAndPreSetInfo(
          PartialPath timeSeriesPath, String alias) {
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
    };
  }

  private static IPartitionFetcher getFakePartitionFetcher() {
    return new IPartitionFetcher() {
      @Override
      public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
        return ANALYSIS.getSchemaPartitionInfo();
      }

      @Override
      public SchemaPartition getOrCreateSchemaPartition(
          PathPatternTree patternTree, String userName) {
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
          List<DataPartitionQueryParam> dataPartitionQueryParams, String userName) {
        return ANALYSIS.getDataPartitionInfo();
      }

      @Override
      public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
          PathPatternTree patternTree, PathPatternTree scope, Integer level) {
        return null;
      }

      @Override
      public boolean updateRegionCache(TRegionRouteReq req) {
        return false;
      }

      @Override
      public void invalidAllCache() {}

      @Override
      public SchemaPartition getOrCreateSchemaPartition(
          String database, List<IDeviceID> deviceIDList, String userName) {
        return null;
      }

      @Override
      public SchemaPartition getSchemaPartition(String database, List<IDeviceID> deviceIDList) {
        return null;
      }

      @Override
      public SchemaPartition getSchemaPartition(String database) {
        return null;
      }
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
