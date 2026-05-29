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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.MeasurementCollector;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MTreeBelowSGCachedImplTest {

  private static final Field STORE_FIELD;
  private static final Field ROOT_NODE_FIELD;

  static {
    try {
      STORE_FIELD = MTreeBelowSGCachedImpl.class.getDeclaredField("store");
      STORE_FIELD.setAccessible(true);
      ROOT_NODE_FIELD = MTreeBelowSGCachedImpl.class.getDeclaredField("rootNode");
      ROOT_NODE_FIELD.setAccessible(true);
    } catch (final NoSuchFieldException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private int rawCachedMNodeSize;
  private MTreeBelowSGCachedImpl mtree;

  @Before
  public void setUp() {
    rawCachedMNodeSize = config.getCachedMNodeSizeInPBTreeMode();
    config.setCachedMNodeSizeInPBTreeMode(10000);
    ReleaseFlushMonitor.getInstance().clear();
    ReleaseFlushMonitor.getInstance().init(new CachedSchemaEngineStatistics());
  }

  @After
  public void tearDown() throws Exception {
    if (mtree != null) {
      mtree.clear();
      mtree = null;
    }
    ReleaseFlushMonitor.getInstance().clear();
    FileUtils.deleteDirectory(new File(config.getSchemaDir()));
    config.setCachedMNodeSizeInPBTreeMode(rawCachedMNodeSize);
  }

  @Test
  public void testDeviceDescendantFlagIsMaintainedAcrossCreateDeleteAndLazyRecompute()
      throws Exception {
    mtree = newMTree();

    createBooleanTimeSeries("root.sg.a.b.s1");

    ICachedMNode database = mtree.getNodeByPath(new PartialPath("root.sg"));
    ICachedMNode aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    ICachedMNode bNode = mtree.getNodeByPath(new PartialPath("root.sg.a.b"));

    Assert.assertTrue(database.hasDeviceDescendant());
    Assert.assertTrue(database.isDeviceDescendantComputed());
    Assert.assertFalse(aNode.isDevice());
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertTrue(aNode.isDeviceDescendantComputed());
    Assert.assertTrue(bNode.isDevice());
    Assert.assertFalse(bNode.hasDeviceDescendant());
    Assert.assertTrue(bNode.isDeviceDescendantComputed());
    mtree.unPinMNode(database);
    mtree.unPinMNode(aNode);
    mtree.unPinMNode(bNode);

    createBooleanTimeSeries("root.sg.a.s0");
    createBooleanTimeSeries("root.sg.c.d.s2");

    aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    ICachedMNode cNode = mtree.getNodeByPath(new PartialPath("root.sg.c"));
    Assert.assertTrue(aNode.isDevice());
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertTrue(aNode.isDeviceDescendantComputed());
    Assert.assertTrue(cNode.hasDeviceDescendant());
    Assert.assertTrue(cNode.isDeviceDescendantComputed());

    aNode.setHasDeviceDescendant(false);
    aNode.setDeviceDescendantComputed(false);
    mtree.unPinMNode(aNode);
    cNode.setHasDeviceDescendant(false);
    cNode.setDeviceDescendantComputed(false);
    mtree.unPinMNode(cNode);

    Assert.assertEquals(
        Collections.singletonList("root.sg.a.b.s1"),
        collectMeasurementPaths(new PartialPath("root.sg.a.**.s1")));

    aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertTrue(aNode.isDeviceDescendantComputed());
    mtree.unPinMNode(aNode);
    cNode = mtree.getNodeByPath(new PartialPath("root.sg.c"));
    Assert.assertFalse(cNode.isDeviceDescendantComputed());
    mtree.unPinMNode(cNode);

    mtree.deleteTimeseries(new PartialPath("root.sg.a.s0"));
    aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    Assert.assertFalse(aNode.isDevice());
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertTrue(aNode.isDeviceDescendantComputed());
    mtree.unPinMNode(aNode);

    mtree.deleteTimeseries(new PartialPath("root.sg.a.b.s1"));
    database = mtree.getNodeByPath(new PartialPath("root.sg"));
    Assert.assertTrue(database.hasDeviceDescendant());
    Assert.assertTrue(database.isDeviceDescendantComputed());
    mtree.unPinMNode(database);
    assertPathNotExist(new PartialPath("root.sg.a"));

    mtree.deleteTimeseries(new PartialPath("root.sg.c.d.s2"));
    database = mtree.getNodeByPath(new PartialPath("root.sg"));
    Assert.assertFalse(database.hasDeviceDescendant());
    Assert.assertTrue(database.isDeviceDescendantComputed());
    mtree.unPinMNode(database);
    assertPathNotExist(new PartialPath("root.sg.c"));
  }

  private MTreeBelowSGCachedImpl newMTree() throws Exception {
    final CachedSchemaRegionStatistics regionStatistics =
        new CachedSchemaRegionStatistics(0, new CachedSchemaEngineStatistics());
    return new MTreeBelowSGCachedImpl(
        PartialPath.getQualifiedDatabasePartialPath("root.sg"),
        node -> Collections.emptyMap(),
        node -> Collections.emptyMap(),
        () -> {},
        node -> {},
        node -> {},
        0,
        regionStatistics,
        new SchemaRegionCachedMetric(regionStatistics, "root.sg"));
  }

  private void createBooleanTimeSeries(final String path) throws MetadataException {
    mtree.createTimeSeries(
        new MeasurementPath(path),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null);
  }

  private List<String> collectMeasurementPaths(final PartialPath pattern) throws Exception {
    final ICachedMNode rootNode = (ICachedMNode) ROOT_NODE_FIELD.get(mtree);
    final CachedMTreeStore store = (CachedMTreeStore) STORE_FIELD.get(mtree);
    final List<String> matchedPaths = new ArrayList<>();
    try (MeasurementCollector<Void, ICachedMNode> collector =
        new MeasurementCollector<Void, ICachedMNode>(
            rootNode, pattern, store, false, SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected Void collectMeasurement(final IMeasurementMNode<ICachedMNode> node) {
            matchedPaths.add(getCurrentMeasurementPathInTraverse(node).getFullPath());
            return null;
          }
        }) {
      collector.traverse();
    }
    return matchedPaths;
  }

  private void assertPathNotExist(final PartialPath path) throws MetadataException {
    try {
      mtree.getNodeByPath(path);
      Assert.fail("Expected path not exist: " + path.getFullPath());
    } catch (final PathNotExistException ignored) {
      // expected
    }
  }
}
