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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.estimator.BasicMNodSizeEstimator;
import org.apache.iotdb.db.metadata.mnode.estimator.IMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.CachedMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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
  public void testMemoryStatistics() throws Exception {
    ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree) >= 1);
    Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree) >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);
    schemaRegion2.deleteTimeseriesInBlackList(patternTree);

    if (testParams.getTestModeName().equals("SchemaFile-PartialMemory")
        || testParams.getTestModeName().equals("SchemaFile-NonMemory")) {
      // wait release and flush task
      Thread.sleep(1000);
      IMNodeSizeEstimator estimator = new CachedMNodeSizeEstimator();
      // schemaRegion1
      IMNode sg1 =
          new StorageGroupEntityMNode(
              null, "sg1", CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
      sg1.setFullPath("root.sg1");
      long size1 = estimator.estimateSize(sg1);
      Assert.assertEquals(size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      // schemaRegion2
      IMNode sg2 =
          new StorageGroupMNode(
              null, "sg2", CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
      sg2.setFullPath("root.sg2");
      long size2 = estimator.estimateSize(sg2);
      Assert.assertEquals(size2, schemaRegion2.getSchemaRegionStatistics().getRegionMemoryUsage());
      Assert.assertEquals(size1 + size2, engineStatistics.getMemoryUsage());
    } else {
      IMNodeSizeEstimator estimator =
          testParams.getSchemaEngineMode().equals("Memory")
              ? new BasicMNodSizeEstimator()
              : new CachedMNodeSizeEstimator();
      // schemaRegion1
      IMNode sg1 =
          new StorageGroupEntityMNode(
              null, "sg1", CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
      sg1.setFullPath("root.sg1");
      long size1 = estimator.estimateSize(sg1);
      IMNode tmp =
          new MeasurementMNode(
              sg1,
              "d0",
              new MeasurementSchema(
                  "d0", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
              null);
      size1 += estimator.estimateSize(tmp);
      tmp = new InternalMNode(sg1, "d1");
      size1 += estimator.estimateSize(tmp);
      tmp = new EntityMNode(tmp, "s2");
      size1 += estimator.estimateSize(tmp);
      size1 +=
          estimator.estimateSize(
              new MeasurementMNode(
                  tmp,
                  "t1",
                  new MeasurementSchema(
                      "t1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null));
      Assert.assertEquals(size1, schemaRegion1.getSchemaRegionStatistics().getRegionMemoryUsage());
      // schemaRegion2
      IMNode sg2 =
          new StorageGroupMNode(
              null, "sg2", CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
      sg2.setFullPath("root.sg2");
      long size2 = estimator.estimateSize(sg2);
      tmp = new EntityMNode(sg2, "d1");
      size2 += estimator.estimateSize(tmp);
      size2 +=
          estimator.estimateSize(
              new MeasurementMNode(
                  tmp,
                  "s3",
                  new MeasurementSchema(
                      "s3", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null));
      tmp = new EntityMNode(sg2, "d2");
      size2 += estimator.estimateSize(tmp);
      size2 +=
          estimator.estimateSize(
              new MeasurementMNode(
                  tmp,
                  "s2",
                  new MeasurementSchema(
                      "s2", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
                  null));
      Assert.assertEquals(size2, schemaRegion2.getSchemaRegionStatistics().getRegionMemoryUsage());
      Assert.assertEquals(size1 + size2, engineStatistics.getMemoryUsage());
    }
    Assert.assertEquals(0, schemaRegion1.getSchemaRegionStatistics().getSchemaRegionId());
    Assert.assertEquals(1, schemaRegion2.getSchemaRegionStatistics().getSchemaRegionId());
    checkSchemaFileStatistics(engineStatistics);
  }

  private void checkSchemaFileStatistics(ISchemaEngineStatistics engineStatistics) {
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
    ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
    ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
    ISchemaEngineStatistics engineStatistics =
        SchemaEngine.getInstance().getSchemaEngineStatistics();

    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree) >= 1);
    Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree) >= 1);
    schemaRegion1.deleteTimeseriesInBlackList(patternTree);
    schemaRegion2.deleteTimeseriesInBlackList(patternTree);

    // check series number
    Assert.assertEquals(2, schemaRegion1.getSchemaRegionStatistics().getSeriesNumber());
    Assert.assertEquals(2, schemaRegion2.getSchemaRegionStatistics().getSeriesNumber());
    Assert.assertEquals(4, engineStatistics.getTotalSeriesNumber());
  }

  @Test
  public void testSchemaFileNodeStatistics() throws Exception {
    if (testParams.getSchemaEngineMode().equals("Schema_File")) {
      ISchemaRegion schemaRegion1 = getSchemaRegion("root.sg1", 0);
      ISchemaRegion schemaRegion2 = getSchemaRegion("root.sg2", 1);
      CachedSchemaEngineStatistics engineStatistics =
          SchemaEngine.getInstance()
              .getSchemaEngineStatistics()
              .getAsCachedSchemaEngineStatistics();
      SchemaRegionTestUtil.createSimpleTimeseriesByList(
          schemaRegion1, Arrays.asList("root.sg1.d0", "root.sg1.d1.s1", "root.sg1.d1.s2.t1"));
      SchemaRegionTestUtil.createSimpleTimeseriesByList(
          schemaRegion2, Arrays.asList("root.sg2.d1.s3", "root.sg2.d2.s1", "root.sg2.d2.s2"));
      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(new PartialPath("root.**.s1"));
      patternTree.constructTree();
      Assert.assertTrue(schemaRegion1.constructSchemaBlackList(patternTree) >= 1);
      Assert.assertTrue(schemaRegion2.constructSchemaBlackList(patternTree) >= 1);
      schemaRegion1.deleteTimeseriesInBlackList(patternTree);
      schemaRegion2.deleteTimeseriesInBlackList(patternTree);

      Thread.sleep(1000);
      CachedSchemaRegionStatistics cachedRegionStatistics1 =
          schemaRegion1.getSchemaRegionStatistics().getAsCachedSchemaRegionStatistics();
      CachedSchemaRegionStatistics cachedRegionStatistics2 =
          schemaRegion2.getSchemaRegionStatistics().getAsCachedSchemaRegionStatistics();
      // check correctness of statistics
      if (testParams.getCachedMNodeSize() > 3) {
        Assert.assertEquals(1, cachedRegionStatistics1.getPinnedMNodeNum());
        Assert.assertEquals(4, cachedRegionStatistics1.getUnpinnedMNodeNum());
        Assert.assertEquals(1, cachedRegionStatistics2.getPinnedMNodeNum());
        Assert.assertEquals(4, cachedRegionStatistics2.getUnpinnedMNodeNum());
      } else {
        Assert.assertEquals(1, cachedRegionStatistics1.getPinnedMNodeNum());
        Assert.assertEquals(0, cachedRegionStatistics1.getUnpinnedMNodeNum());
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
    schemaRegion1.createTimeseries(
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
            Arrays.asList(
                Collections.singletonList("s1"),
                Collections.singletonList("s2"),
                Collections.singletonList("s3")),
            Arrays.asList(
                Collections.singletonList(TSDataType.DOUBLE),
                Collections.singletonList(TSDataType.INT32),
                Collections.singletonList(TSDataType.BOOLEAN)),
            Arrays.asList(
                Collections.singletonList(TSEncoding.RLE),
                Collections.singletonList(TSEncoding.RLE),
                Collections.singletonList(TSEncoding.RLE)),
            Arrays.asList(
                Collections.singletonList(CompressionType.SNAPPY),
                Collections.singletonList(CompressionType.SNAPPY),
                Collections.singletonList(CompressionType.SNAPPY)));
    template1.setId(1);
    Template template2 =
        new Template(
            "t2",
            Arrays.asList(
                Collections.singletonList("temperature"), Collections.singletonList("status")),
            Arrays.asList(
                Collections.singletonList(TSDataType.DOUBLE),
                Collections.singletonList(TSDataType.INT32)),
            Arrays.asList(
                Collections.singletonList(TSEncoding.RLE),
                Collections.singletonList(TSEncoding.RLE)),
            Arrays.asList(
                Collections.singletonList(CompressionType.SNAPPY),
                Collections.singletonList(CompressionType.SNAPPY)));
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
    Assert.assertEquals(13, schemaRegion1.getSchemaRegionStatistics().getSeriesNumber());
    Assert.assertEquals(12, schemaRegion1.getSchemaRegionStatistics().getTemplateSeriesNumber());
    Assert.assertEquals(14, schemaRegion2.getSchemaRegionStatistics().getSeriesNumber());
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
    Assert.assertEquals(10, schemaRegion1.getSchemaRegionStatistics().getSeriesNumber());
    Assert.assertEquals(9, schemaRegion1.getSchemaRegionStatistics().getTemplateSeriesNumber());
    Assert.assertEquals(11, schemaRegion2.getSchemaRegionStatistics().getSeriesNumber());
    Assert.assertEquals(11, schemaRegion2.getSchemaRegionStatistics().getTemplateSeriesNumber());
  }
}
