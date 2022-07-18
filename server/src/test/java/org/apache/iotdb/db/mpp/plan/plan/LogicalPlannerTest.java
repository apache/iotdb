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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.Analyzer;
import org.apache.iotdb.db.mpp.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.mpp.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsConvertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.mpp.plan.plan.QueryLogicalPlanUtil.querySQLs;
import static org.apache.iotdb.db.mpp.plan.plan.QueryLogicalPlanUtil.sqlToPlanMap;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class LogicalPlannerTest {

  @Test
  public void testQueryPlan() {
    for (String sql : querySQLs) {
      Assert.assertEquals(sqlToPlanMap.get(sql), parseSQLToPlanNode(sql));
      System.out.printf("\"%s\" TEST PASSED\n", sql);
    }
  }

  @Test
  public void testCreateTimeseriesPlan() {
    String sql =
        "CREATE TIMESERIES root.ln.wf01.wt01.status(状态) BOOLEAN ENCODING=PLAIN COMPRESSOR=SNAPPY "
            + "TAGS('tag1'='v1', 'tag2'='v2') ATTRIBUTES('attr1'='v1', 'attr2'='v2')";
    try {
      CreateTimeSeriesNode createTimeSeriesNode = (CreateTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(createTimeSeriesNode);
      Assert.assertEquals(
          new PartialPath("root.ln.wf01.wt01.status"), createTimeSeriesNode.getPath());
      Assert.assertEquals("状态", createTimeSeriesNode.getAlias());
      Assert.assertEquals(TSDataType.BOOLEAN, createTimeSeriesNode.getDataType());
      Assert.assertEquals(TSEncoding.PLAIN, createTimeSeriesNode.getEncoding());
      Assert.assertEquals(CompressionType.SNAPPY, createTimeSeriesNode.getCompressor());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag1", "v1");
              put("tag2", "v2");
            }
          },
          createTimeSeriesNode.getTags());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("attr1", "v1");
              put("attr2", "v2");
            }
          },
          createTimeSeriesNode.getAttributes());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateAlignedTimeseriesPlan() {
    String sql =
        "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude(meter1) FLOAT encoding=PLAIN compressor=SNAPPY tags('tag1'='t1') attributes('attr1'='a1'), longitude FLOAT encoding=PLAIN compressor=SNAPPY)";
    try {
      CreateAlignedTimeSeriesNode createAlignedTimeSeriesNode =
          (CreateAlignedTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(createAlignedTimeSeriesNode);
      Assert.assertEquals(
          new PartialPath("root.ln.wf01.GPS"), createAlignedTimeSeriesNode.getDevicePath());
      Assert.assertEquals(
          new ArrayList<String>() {
            {
              add("meter1");
              add(null);
            }
          },
          createAlignedTimeSeriesNode.getAliasList());
      Assert.assertEquals(
          new ArrayList<TSDataType>() {
            {
              add(TSDataType.FLOAT);
              add(TSDataType.FLOAT);
            }
          },
          createAlignedTimeSeriesNode.getDataTypes());
      Assert.assertEquals(
          new ArrayList<TSEncoding>() {
            {
              add(TSEncoding.PLAIN);
              add(TSEncoding.PLAIN);
            }
          },
          createAlignedTimeSeriesNode.getEncodings());
      Assert.assertEquals(
          new ArrayList<CompressionType>() {
            {
              add(CompressionType.SNAPPY);
              add(CompressionType.SNAPPY);
            }
          },
          createAlignedTimeSeriesNode.getCompressors());
      Assert.assertEquals(
          new ArrayList<Map<String, String>>() {
            {
              add(
                  new HashMap<String, String>() {
                    {
                      put("attr1", "a1");
                    }
                  });
              add(null);
            }
          },
          createAlignedTimeSeriesNode.getAttributesList());
      Assert.assertEquals(
          new ArrayList<Map<String, String>>() {
            {
              add(
                  new HashMap<String, String>() {
                    {
                      put("tag1", "t1");
                    }
                  });
              add(null);
            }
          },
          createAlignedTimeSeriesNode.getTagsList());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      createAlignedTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      CreateAlignedTimeSeriesNode createAlignedTimeSeriesNode1 =
          (CreateAlignedTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(createAlignedTimeSeriesNode.equals(createAlignedTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateMultiTimeSeriesPlan() {
    try {
      TSCreateMultiTimeseriesReq req = new TSCreateMultiTimeseriesReq();
      req.setPaths(
          new ArrayList<String>() {
            {
              add("root.sg1.d2.s1");
              add("root.sg1.d2.s2");
            }
          });
      req.setMeasurementAliasList(
          new ArrayList<String>() {
            {
              add("meter1");
              add(null);
            }
          });
      req.setDataTypes(
          new ArrayList<Integer>() {
            {
              add(TSDataType.FLOAT.ordinal());
              add(TSDataType.FLOAT.ordinal());
            }
          });
      req.setEncodings(
          new ArrayList<Integer>() {
            {
              add(TSEncoding.PLAIN.ordinal());
              add(TSEncoding.PLAIN.ordinal());
            }
          });
      req.setCompressors(
          new ArrayList<Integer>() {
            {
              add(CompressionType.SNAPPY.ordinal());
              add(CompressionType.SNAPPY.ordinal());
            }
          });
      req.setAttributesList(
          new ArrayList<Map<String, String>>() {
            {
              add(
                  new HashMap<String, String>() {
                    {
                      put("attr1", "a1");
                    }
                  });
              add(null);
            }
          });
      req.setTagsList(
          new ArrayList<Map<String, String>>() {
            {
              add(
                  new HashMap<String, String>() {
                    {
                      put("tag1", "t1");
                    }
                  });
              add(null);
            }
          });
      CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement =
          (CreateMultiTimeSeriesStatement) StatementGenerator.createStatement(req);
      MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
      Analyzer analyzer =
          new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
      Analysis analysis = analyzer.analyze(createMultiTimeSeriesStatement);
      LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
      CreateMultiTimeSeriesNode createMultiTimeSeriesNode =
          (CreateMultiTimeSeriesNode) planner.plan(analysis).getRootNode();
      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      createMultiTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();
      CreateMultiTimeSeriesNode createMultiTimeSeriesNode1 =
          (CreateMultiTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertEquals(createMultiTimeSeriesNode, createMultiTimeSeriesNode1);
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testAlterTimeseriesPlan() {
    String sql = "ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'newTag1'";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.RENAME, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag1", "newTag1");
            }
          },
          alterTimeSeriesNode.getAlterMap());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      alterTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      AlterTimeSeriesNode alterTimeSeriesNode1 =
          (AlterTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(alterTimeSeriesNode.equals(alterTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 SET 'newTag1'='newV1', 'attr1'='newV1'";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.SET, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("newTag1", "newV1");
              put("attr1", "newV1");
            }
          },
          alterTimeSeriesNode.getAlterMap());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      alterTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      AlterTimeSeriesNode alterTimeSeriesNode1 =
          (AlterTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(alterTimeSeriesNode.equals(alterTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 DROP 'tag1', 'tag2'";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.DROP, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag1", null);
              put("tag2", null);
            }
          },
          alterTimeSeriesNode.getAlterMap());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      alterTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      AlterTimeSeriesNode alterTimeSeriesNode1 =
          (AlterTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(alterTimeSeriesNode.equals(alterTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 ADD TAGS 'tag3'='v3', 'tag4'='v4'";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.ADD_TAGS, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag3", "v3");
              put("tag4", "v4");
            }
          },
          alterTimeSeriesNode.getAlterMap());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      alterTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      AlterTimeSeriesNode alterTimeSeriesNode1 =
          (AlterTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(alterTimeSeriesNode.equals(alterTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3'='v3', 'attr4'='v4'";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("attr3", "v3");
              put("attr4", "v4");
            }
          },
          alterTimeSeriesNode.getAlterMap());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      alterTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      AlterTimeSeriesNode alterTimeSeriesNode1 =
          (AlterTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(alterTimeSeriesNode.equals(alterTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql =
        "ALTER timeseries root.turbine.d1.s1 UPSERT ALIAS='newAlias' "
            + "TAGS('tag2'='newV2', 'tag3'='v3') ATTRIBUTES('attr3'='v3', 'attr4'='v4')";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.UPSERT, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag2", "newV2");
              put("tag3", "v3");
            }
          },
          alterTimeSeriesNode.getTagsMap());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("attr3", "v3");
              put("attr4", "v4");
            }
          },
          alterTimeSeriesNode.getAttributesMap());

      // Test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
      alterTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();

      AlterTimeSeriesNode alterTimeSeriesNode1 =
          (AlterTimeSeriesNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
      Assert.assertTrue(alterTimeSeriesNode.equals(alterTimeSeriesNode1));
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testShowTimeSeries() {
    String sql =
        "SHOW LATEST TIMESERIES root.ln.wf01.wt01.status WHERE 'tagK' = 'tagV' limit 20 offset 10";

    try {
      LimitNode limitNode = (LimitNode) parseSQLToPlanNode(sql);
      OffsetNode offsetNode = (OffsetNode) limitNode.getChild();
      SchemaQueryOrderByHeatNode schemaQueryOrderByHeatNode =
          (SchemaQueryOrderByHeatNode) offsetNode.getChild();
      SchemaQueryMergeNode metaMergeNode =
          (SchemaQueryMergeNode) schemaQueryOrderByHeatNode.getChildren().get(0);
      metaMergeNode.getChildren().forEach(n -> System.out.println(n.toString()));
      TimeSeriesSchemaScanNode showTimeSeriesNode =
          (TimeSeriesSchemaScanNode) metaMergeNode.getChildren().get(0);
      Assert.assertNotNull(showTimeSeriesNode);
      Assert.assertEquals(
          new PartialPath("root.ln.wf01.wt01.status"), showTimeSeriesNode.getPath());
      Assert.assertEquals("root.ln.wf01.wt01", showTimeSeriesNode.getPath().getDevice());
      Assert.assertTrue(showTimeSeriesNode.isOrderByHeat());
      Assert.assertFalse(showTimeSeriesNode.isContains());
      Assert.assertEquals("tagK", showTimeSeriesNode.getKey());
      Assert.assertEquals("tagV", showTimeSeriesNode.getValue());
      Assert.assertEquals(0, showTimeSeriesNode.getLimit());
      Assert.assertEquals(0, showTimeSeriesNode.getOffset());
      Assert.assertFalse(showTimeSeriesNode.isHasLimit());

      // test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
      showTimeSeriesNode.serialize(byteBuffer);
      byteBuffer.flip();
      TimeSeriesSchemaScanNode showTimeSeriesNode2 =
          (TimeSeriesSchemaScanNode) PlanNodeType.deserialize(byteBuffer);
      Assert.assertNotNull(showTimeSeriesNode2);
      Assert.assertEquals(
          new PartialPath("root.ln.wf01.wt01.status"), showTimeSeriesNode2.getPath());
      Assert.assertEquals("root.ln.wf01.wt01", showTimeSeriesNode2.getPath().getDevice());
      Assert.assertTrue(showTimeSeriesNode2.isOrderByHeat());
      Assert.assertFalse(showTimeSeriesNode2.isContains());
      Assert.assertEquals("tagK", showTimeSeriesNode2.getKey());
      Assert.assertEquals("tagV", showTimeSeriesNode2.getValue());
      Assert.assertEquals(0, showTimeSeriesNode2.getLimit());
      Assert.assertEquals(0, showTimeSeriesNode2.getOffset());
      Assert.assertFalse(showTimeSeriesNode2.isHasLimit());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testShowDevices() {
    String sql = "SHOW DEVICES root.ln.wf01.wt01 WITH STORAGE GROUP limit 20 offset 10";
    try {
      LimitNode limitNode = (LimitNode) parseSQLToPlanNode(sql);
      OffsetNode offsetNode = (OffsetNode) limitNode.getChild();
      SchemaQueryMergeNode metaMergeNode = (SchemaQueryMergeNode) offsetNode.getChild();
      DevicesSchemaScanNode showDevicesNode =
          (DevicesSchemaScanNode) metaMergeNode.getChildren().get(0);
      Assert.assertNotNull(showDevicesNode);
      Assert.assertEquals(new PartialPath("root.ln.wf01.wt01"), showDevicesNode.getPath());
      Assert.assertTrue(showDevicesNode.isHasSgCol());
      Assert.assertEquals(30, showDevicesNode.getLimit());
      Assert.assertEquals(0, showDevicesNode.getOffset());
      Assert.assertTrue(showDevicesNode.isHasLimit());

      // test serialize and deserialize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
      showDevicesNode.serialize(byteBuffer);
      byteBuffer.flip();
      DevicesSchemaScanNode showDevicesNode2 =
          (DevicesSchemaScanNode) PlanNodeType.deserialize(byteBuffer);
      Assert.assertNotNull(showDevicesNode2);
      Assert.assertEquals(new PartialPath("root.ln.wf01.wt01"), showDevicesNode2.getPath());
      Assert.assertEquals(30, showDevicesNode2.getLimit());
      Assert.assertEquals(0, showDevicesNode2.getOffset());
      Assert.assertTrue(showDevicesNode2.isHasLimit());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCountNodes() {
    String sql = "COUNT NODES root.ln LEVEL=1";
    try {
      NodePathsCountNode nodePathsCountNode = (NodePathsCountNode) parseSQLToPlanNode(sql);
      NodeManagementMemoryMergeNode nodeManagementMemoryMergeNode =
          (NodeManagementMemoryMergeNode) nodePathsCountNode.getChildren().get(0);
      SchemaQueryMergeNode schemaQueryMergeNode =
          (SchemaQueryMergeNode) nodeManagementMemoryMergeNode.getChildren().get(0);
      NodePathsSchemaScanNode nodePathsSchemaScanNode =
          (NodePathsSchemaScanNode) schemaQueryMergeNode.getChildren().get(0);
      Assert.assertNotNull(nodePathsSchemaScanNode);
      Assert.assertEquals(new PartialPath("root.ln"), nodePathsSchemaScanNode.getPrefixPath());
      Assert.assertEquals(1, nodePathsSchemaScanNode.getLevel());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testShowChildPaths() {
    String sql = "SHOW CHILD PATHS root.ln";
    try {
      NodeManagementMemoryMergeNode memorySourceNode =
          (NodeManagementMemoryMergeNode) parseSQLToPlanNode(sql);
      SchemaQueryMergeNode schemaQueryMergeNode =
          (SchemaQueryMergeNode) memorySourceNode.getChildren().get(0);
      NodePathsSchemaScanNode nodePathsSchemaScanNode =
          (NodePathsSchemaScanNode) schemaQueryMergeNode.getChildren().get(0);
      Assert.assertNotNull(nodePathsSchemaScanNode);
      Assert.assertEquals(new PartialPath("root.ln"), nodePathsSchemaScanNode.getPrefixPath());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testShowChildNodes() {
    String sql = "SHOW CHILD NODES root.ln";
    try {
      NodePathsConvertNode nodePathsConvertNode = (NodePathsConvertNode) parseSQLToPlanNode(sql);
      NodeManagementMemoryMergeNode memorySourceNode =
          (NodeManagementMemoryMergeNode) nodePathsConvertNode.getChildren().get(0);
      SchemaQueryMergeNode schemaQueryMergeNode =
          (SchemaQueryMergeNode) memorySourceNode.getChildren().get(0);
      NodePathsSchemaScanNode nodePathsSchemaScanNode =
          (NodePathsSchemaScanNode) schemaQueryMergeNode.getChildren().get(0);
      assertNotNull(nodePathsConvertNode);
      Assert.assertEquals(new PartialPath("root.ln"), nodePathsSchemaScanNode.getPrefixPath());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private PlanNode parseSQLToPlanNode(String sql) {
    PlanNode planNode = null;
    try {
      Statement statement =
          StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
      MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
      Analyzer analyzer =
          new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
      Analysis analysis = analyzer.analyze(statement);
      LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
      planNode = planner.plan(analysis).getRootNode();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    return planNode;
  }
}
