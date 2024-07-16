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

package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaStatisticsTest extends AbstractSchemaRegionTest {

  public SchemaStatisticsTest(AbstractSchemaRegionTest.SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Test
  public void testPBTreeMemoryStatistics() throws Exception {
    final ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    final ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion1, Arrays.asList("root.sg1.n.s0", "root.sg1.n.v.d1.s1", "root.sg1.n.v.d2.s2"));
    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion1, Arrays.asList("root.sg1.d0.s0"));
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.appendPathPattern(new PartialPath("root.**.s2"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree).getLeft() >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);

    if (testParams.getTestModeName().equals("PBTree-PartialMemory")
        || testParams.getTestModeName().equals("PBTree-NonMemory")) {
      final IMNodeFactory<ICachedMNode> nodeFactory =
          MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
      // wait release and flush task
      Thread.sleep(6000);
      // schemaRegion1
      final IMNode<ICachedMNode> sg1 = nodeFactory.createDatabaseMNode(null, "sg1");
      sg1.setFullPath("root.sg1");
      final long size1 = sg1.estimateSize();
      if (size1 != schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage()) {
        // There are two possibilities here in PartialMemory mode:
        // 1. only the "sg1" node remains
        // 2. the "sg1" node and the "n" node remain
        Assert.assertEquals("PBTree-PartialMemory", testParams.getTestModeName());
        Assert.assertEquals(
            size1 + nodeFactory.createDeviceMNode(sg1.getAsMNode(), "n").estimateSize(),
            schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
        ReleaseFlushMonitor.getInstance().forceFlushAndRelease();
        Assert.assertEquals(
            size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      }
    }
    Assert.assertEquals(0, schemaRegion1.getSchemaRegionStatistics().getSchemaRegionId());
    checkPBTreeStatistics(engineStatistics);
  }

  @Test
  public void testMemoryStatistics2() throws Exception {
    final ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    final ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    final ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion1, Arrays.asList("root.sg1.v.d0", "root.sg1.d1.v.s1", "root.sg1.d1.s2.v.t1"));
    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion2, Arrays.asList("root.sg2.d1.v.s3", "root.sg2.d2.v.s1", "root.sg2.d2.v.s2"));
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree).getLeft() >= 1);
    Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree).getLeft() >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);
    schemaRegion2.deleteTimeseriesInBlackList(patternTree);

    if (testParams.getTestModeName().equals("PBTree-PartialMemory")
        || testParams.getTestModeName().equals("PBTree-NonMemory")) {

      final IMNodeFactory<?> nodeFactory =
          MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
      // wait release and flush task
      Thread.sleep(1000);
      // schemaRegion1
      final IMNode<?> sg1 = nodeFactory.createDatabaseMNode(null, "sg1");
      sg1.setFullPath("root.sg1");
      final long size1 = sg1.estimateSize();
      Assert.assertEquals(size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      // schemaRegion2
      final IMNode<?> sg2 = nodeFactory.createDatabaseMNode(null, "sg2");
      sg2.setFullPath("root.sg2");
      long size2 = sg2.estimateSize();
      Assert.assertEquals(size2, schemaRegion2.getSchemaRegionStatistics().getRegionMemoryUsage());
      Assert.assertEquals(size1 + size2, engineStatistics.getMemoryUsage());
    } else {
      final IMNodeFactory nodeFactory =
          testParams.getSchemaEngineMode().equals("Memory")
              ? MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory()
              : MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
      // schemaRegion1
      final IMNode<?> sg1 = nodeFactory.createDatabaseMNode(null, "sg1");
      sg1.setFullPath("root.sg1");
      long size1 = sg1.estimateSize();
      IMNode<?> tmp = nodeFactory.createDeviceMNode(sg1, "v");
      size1 += tmp.estimateSize();
      tmp =
          nodeFactory.createMeasurementMNode(
              tmp.getAsDeviceMNode(),
              "d0",
              new MeasurementSchema(
                  "d0", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
              null);
      size1 += tmp.estimateSize();
      tmp = nodeFactory.createInternalMNode(sg1.getAsMNode(), "d1");
      size1 += tmp.estimateSize();
      tmp = nodeFactory.createInternalMNode(tmp, "s2");
      size1 += tmp.estimateSize();
      tmp = nodeFactory.createDeviceMNode(tmp, "v");
      size1 += tmp.estimateSize();
      size1 +=
          nodeFactory
              .createMeasurementMNode(
                  tmp.getAsDeviceMNode(),
                  "t1",
                  new MeasurementSchema(
                      "t1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null)
              .estimateSize();
      Assert.assertEquals(size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      // schemaRegion2
      final IMNode<?> sg2 = nodeFactory.createDatabaseMNode(null, "sg2");
      sg2.setFullPath("root.sg2");
      long size2 = sg2.estimateSize();
      tmp = nodeFactory.createInternalMNode(sg2, "d1");
      size2 += tmp.estimateSize();
      tmp = nodeFactory.createDeviceMNode(tmp, "v");
      size2 += tmp.estimateSize();
      size2 +=
          nodeFactory
              .createMeasurementMNode(
                  tmp.getAsDeviceMNode(),
                  "s3",
                  new MeasurementSchema(
                      "s3", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null)
              .estimateSize();
      tmp = nodeFactory.createInternalMNode(sg2, "d2");
      size2 += tmp.estimateSize();
      tmp = nodeFactory.createDeviceMNode(tmp, "v");
      size2 += tmp.estimateSize();
      size2 +=
          nodeFactory
              .createMeasurementMNode(
                  tmp.getAsDeviceMNode(),
                  "s2",
                  new MeasurementSchema(
                      "s2", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null)
              .estimateSize();
      Assert.assertEquals(size2, schemaRegion2.getSchemaRegionStatistics().getRegionMemoryUsage());
      Assert.assertEquals(size1 + size2, engineStatistics.getMemoryUsage());
    }
    Assert.assertEquals(0, schemaRegion1.getSchemaRegionStatistics().getSchemaRegionId());
    Assert.assertEquals(1, schemaRegion2.getSchemaRegionStatistics().getSchemaRegionId());
    checkPBTreeStatistics(engineStatistics);
  }

  @Test
  public void testMemoryStatistics() throws Exception {
    final ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    final ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    final ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree).getLeft() >= 1);
    Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree).getLeft() >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);
    schemaRegion2.deleteTimeseriesInBlackList(patternTree);

    if (testParams.getTestModeName().equals("PBTree-PartialMemory")
        || testParams.getTestModeName().equals("PBTree-NonMemory")) {
      final IMNodeFactory<ICachedMNode> nodeFactory =
          MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
      // wait release and flush task
      Thread.sleep(1000);
      // schemaRegion1
      final IMNode<ICachedMNode> sg1 = nodeFactory.createDatabaseDeviceMNode(null, "sg1");
      sg1.setFullPath("root.sg1");
      final long size1 = sg1.estimateSize();
      if (sg1.estimateSize() != schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage()) {
        // "d0" or "d1" node may remain in PartialMemory mode
        Assert.assertEquals("PBTree-PartialMemory", testParams.getTestModeName());
        final long d0ExistSize =
            size1
                + nodeFactory
                    .createMeasurementMNode(
                        sg1.getAsDeviceMNode(),
                        "d0",
                        new MeasurementSchema(
                            "d0", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                        null)
                    .estimateSize();
        final long d1ExistSize =
            size1 + nodeFactory.createInternalMNode(sg1.getAsMNode(), "d1").estimateSize();
        Assert.assertTrue(
            d0ExistSize == schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage()
                || d1ExistSize == schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
        ReleaseFlushMonitor.getInstance().forceFlushAndRelease();
        // wait release and flush task
        Thread.sleep(1000);
        Assert.assertEquals(
            size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      }
      // schemaRegion2
      final IMNode<?> sg2 = nodeFactory.createDatabaseMNode(null, "sg2");
      sg2.setFullPath("root.sg2");
      final long size2 = sg2.estimateSize();
      Assert.assertEquals(size2, schemaRegion2.getSchemaRegionStatistics().getRegionMemoryUsage());
      Assert.assertEquals(size1 + size2, engineStatistics.getMemoryUsage());
    } else {
      final IMNodeFactory nodeFactory =
          testParams.getSchemaEngineMode().equals("Memory")
              ? MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory()
              : MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();
      // schemaRegion1
      final IMNode<?> sg1 = nodeFactory.createDatabaseDeviceMNode(null, "sg1");
      sg1.setFullPath("root.sg1");
      long size1 = sg1.estimateSize();
      IMNode<?> tmp =
          nodeFactory.createMeasurementMNode(
              sg1.getAsDeviceMNode(),
              "d0",
              new MeasurementSchema(
                  "d0", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
              null);
      size1 += tmp.estimateSize();
      tmp = nodeFactory.createInternalMNode(sg1.getAsMNode(), "d1");
      size1 += tmp.estimateSize();
      tmp = nodeFactory.createDeviceMNode(tmp, "s2");
      size1 += tmp.estimateSize();
      size1 +=
          nodeFactory
              .createMeasurementMNode(
                  tmp.getAsDeviceMNode(),
                  "t1",
                  new MeasurementSchema(
                      "t1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null)
              .estimateSize();
      Assert.assertEquals(size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      // schemaRegion2
      final IMNode<?> sg2 = nodeFactory.createDatabaseMNode(null, "sg2");
      sg2.setFullPath("root.sg2");
      long size2 = sg2.estimateSize();
      tmp = nodeFactory.createDeviceMNode(sg2, "d1");
      size2 += tmp.estimateSize();
      size2 +=
          nodeFactory
              .createMeasurementMNode(
                  tmp.getAsDeviceMNode(),
                  "s3",
                  new MeasurementSchema(
                      "s3", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null)
              .estimateSize();
      tmp = nodeFactory.createDeviceMNode(sg2, "d2");
      size2 += tmp.estimateSize();
      size2 +=
          nodeFactory
              .createMeasurementMNode(
                  tmp.getAsDeviceMNode(),
                  "s2",
                  new MeasurementSchema(
                      "s2", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null)
              .estimateSize();
      Assert.assertEquals(size2, schemaRegion2.getSchemaRegionStatistics().getRegionMemoryUsage());
      Assert.assertEquals(size1 + size2, engineStatistics.getMemoryUsage());
    }
    Assert.assertEquals(0, schemaRegion1.getSchemaRegionStatistics().getSchemaRegionId());
    Assert.assertEquals(1, schemaRegion2.getSchemaRegionStatistics().getSchemaRegionId());
    checkPBTreeStatistics(engineStatistics);
  }

  private void checkPBTreeStatistics(ISchemaEngineStatistics engineStatistics) {
    if (engineStatistics instanceof CachedSchemaEngineStatistics) {
      CachedSchemaEngineStatistics cachedEngineStatistics =
          (CachedSchemaEngineStatistics) engineStatistics;
      Assert.assertEquals(
          cachedEngineStatistics.getMemoryUsage(),
          cachedEngineStatistics.getPinnedMemorySize()
              + cachedEngineStatistics.getUnpinnedMemorySize());
    }
  }

  @Test
  public void testSeriesNumStatistics() throws Exception {
    final ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    final ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    final ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree).getLeft() >= 1);
    Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree).getLeft() >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);
    schemaRegion2.deleteTimeseriesInBlackList(patternTree);

    // check series number
    Assert.assertEquals(2, schemaRegion1.getSchemaRegionStatistics().getSeriesNumber(true));
    Assert.assertEquals(2, schemaRegion2.getSchemaRegionStatistics().getSeriesNumber(true));
    Assert.assertEquals(4, engineStatistics.getTotalSeriesNumber());
  }

  @Test
  public void testDeviceNumStatistics() throws Exception {
    final ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    final ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    final ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
    SchemaRegionTestUtil.createSimpleTimeSeriesByList(
        schemaRegion2, Collections.singletonList("root.sg2.s1"));
    // Check device number
    Assert.assertEquals(3, schemaRegion1.getSchemaRegionStatistics().getDevicesNumber());
    Assert.assertEquals(3, schemaRegion2.getSchemaRegionStatistics().getDevicesNumber());

    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree).getLeft() >= 1);
    Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree).getLeft() >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);
    schemaRegion2.deleteTimeseriesInBlackList(patternTree);

    // Check device number
    Assert.assertEquals(2, schemaRegion1.getSchemaRegionStatistics().getDevicesNumber());
    Assert.assertEquals(2, schemaRegion2.getSchemaRegionStatistics().getDevicesNumber());
    Assert.assertEquals(4, engineStatistics.getTotalDevicesNumber());
  }

  @Test
  public void testPBTreeNodeStatistics() throws Exception {
    if (testParams.getSchemaEngineMode().equals("PBTree")) {
      final ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
      final ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
      final CachedSchemaEngineStatistics engineStatistics =
          SchemaEngine.getInstance()
              .getSchemaEngineStatistics()
              .getAsCachedSchemaEngineStatistics();
      SchemaRegionTestUtil.createSimpleTimeSeriesByList(
          schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
      SchemaRegionTestUtil.createSimpleTimeSeriesByList(
          schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
      final PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(new PartialPath("root.**.s1"));
      patternTree.constructTree();
      Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree).getLeft() >= 1);
      Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree).getLeft() >= 1);
      schemaRegion1.deleteTimeseriesInBlackList(patternTree);
      schemaRegion2.deleteTimeseriesInBlackList(patternTree);

      Thread.sleep(1000);
      final CachedSchemaRegionStatistics cachedRegionStatistics1 =
          schemaRegion1.getSchemaRegionStatistics().getAsCachedSchemaRegionStatistics();
      final CachedSchemaRegionStatistics cachedRegionStatistics2 =
          schemaRegion2.getSchemaRegionStatistics().getAsCachedSchemaRegionStatistics();
      // check correctness of statistics
      if (testParams.getCachedMNodeSize() > 3) {
        Assert.assertEquals(1, cachedRegionStatistics1.getPinnedMNodeNum());
        Assert.assertEquals(4, cachedRegionStatistics1.getUnpinnedMNodeNum());
        Assert.assertEquals(1, cachedRegionStatistics2.getPinnedMNodeNum());
        Assert.assertEquals(4, cachedRegionStatistics2.getUnpinnedMNodeNum());
      } else {
        Assert.assertEquals(1, cachedRegionStatistics1.getPinnedMNodeNum());
        if (0 != cachedRegionStatistics1.getUnpinnedMNodeNum()) {
          // "d0" may remain in PartialMemory mode
          Assert.assertEquals("PBTree-PartialMemory", testParams.getTestModeName());
          ReleaseFlushMonitor.getInstance().forceFlushAndRelease();
          Thread.sleep(1000);
          Assert.assertEquals(0, cachedRegionStatistics1.getUnpinnedMNodeNum());
        }
        Assert.assertEquals(1, cachedRegionStatistics2.getPinnedMNodeNum());
        Assert.assertEquals(0, cachedRegionStatistics2.getUnpinnedMNodeNum());
      }
      // check consistence between region and engine
      Assert.assertEquals(
          cachedRegionStatistics1.getPinnedMNodeNum() + cachedRegionStatistics2.getPinnedMNodeNum(),
          engineStatistics.getPinnedMNodeNum());
      Assert.assertEquals(
          cachedRegionStatistics1.getUnpinnedMNodeNum()
              + cachedRegionStatistics2.getUnpinnedMNodeNum(),
          engineStatistics.getUnpinnedMNodeNum());
      Assert.assertEquals(
          cachedRegionStatistics1.getPinnedMemorySize()
              + cachedRegionStatistics2.getPinnedMemorySize(),
          engineStatistics.getPinnedMemorySize());
      Assert.assertEquals(
          cachedRegionStatistics1.getUnpinnedMemorySize()
              + cachedRegionStatistics2.getUnpinnedMemorySize(),
          engineStatistics.getUnpinnedMemorySize());
    }
  }

  @Test
  public void testTemplateStatistics() throws Exception {
    ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();
    ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    schemaRegion1.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    Template template1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    template1.setId(1);
    Template template2 =
        new Template(
            "t2",
            Arrays.asList("temperature", "status"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template2.setId(2);
    ClusterTemplateManager.getInstance().putTemplate(template1);
    ClusterTemplateManager.getInstance().putTemplate(template2);
    for (int i = 0; i < 4; i++) {
      schemaRegion1.activateSchemaTemplate(
          SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
              new PartialPath("root.sg1.d" + i), 2, 1),
          template1);
      schemaRegion2.activateSchemaTemplate(
          SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
              new PartialPath("root.sg2.d" + i), 2, 1),
          template1);
    }
    schemaRegion2.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg2.wf01.wt02"), 3, 2),
        template2);

    // check template statistic
    Assert.assertEquals(26, engineStatistics.getTemplateSeriesNumber());
    Assert.assertEquals(27, engineStatistics.getTotalSeriesNumber());
    Assert.assertEquals(13, schemaRegion1.getSchemaRegionStatistics().getSeriesNumber(true));
    Assert.assertEquals(12, schemaRegion1.getSchemaRegionStatistics().getTemplateSeriesNumber());
    Assert.assertEquals(14, schemaRegion2.getSchemaRegionStatistics().getSeriesNumber(true));
    Assert.assertEquals(14, schemaRegion2.getSchemaRegionStatistics().getTemplateSeriesNumber());

    // deactivate template
    // construct schema blacklist with template on root.sg.wf01.wt01 and root.sg.wf02
    Map<PartialPath, List<Integer>> allDeviceTemplateMap = new HashMap<>();
    allDeviceTemplateMap.put(new PartialPath("root.**.d0"), Arrays.asList(1, 2));
    schemaRegion1.constructSchemaBlackListWithTemplate(
        SchemaRegionWritePlanFactory.getPreDeactivateTemplatePlan(allDeviceTemplateMap));
    schemaRegion2.constructSchemaBlackListWithTemplate(
        SchemaRegionWritePlanFactory.getPreDeactivateTemplatePlan(allDeviceTemplateMap));
    schemaRegion1.deactivateTemplateInBlackList(
        SchemaRegionWritePlanFactory.getDeactivateTemplatePlan(allDeviceTemplateMap));
    schemaRegion2.deactivateTemplateInBlackList(
        SchemaRegionWritePlanFactory.getDeactivateTemplatePlan(allDeviceTemplateMap));

    // check template statistic
    Assert.assertEquals(20, engineStatistics.getTemplateSeriesNumber());
    Assert.assertEquals(21, engineStatistics.getTotalSeriesNumber());
    Assert.assertEquals(10, schemaRegion1.getSchemaRegionStatistics().getSeriesNumber(true));
    Assert.assertEquals(9, schemaRegion1.getSchemaRegionStatistics().getTemplateSeriesNumber());
    Assert.assertEquals(11, schemaRegion2.getSchemaRegionStatistics().getSeriesNumber(true));
    Assert.assertEquals(11, schemaRegion2.getSchemaRegionStatistics().getTemplateSeriesNumber());
  }
}
