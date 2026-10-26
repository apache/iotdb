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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionMemMetric;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.container.MemMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.BasicInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector.MeasurementCollector;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MTreeBelowSGMemoryImplTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWideDeviceDeletionDoesNotScanChildrenPerMeasurement() throws Exception {
    final MTreeBelowSGMemoryImpl mtree = newMTree();
    final int measurementCount = 1024;
    final List<String> measurements = new ArrayList<>();
    for (int i = 0; i < measurementCount; i++) {
      measurements.add("s" + i);
    }
    mtree.createAlignedTimeSeries(
        new PartialPath("root.sg.d"),
        measurements,
        Collections.nCopies(measurementCount, TSDataType.INT32),
        Collections.nCopies(measurementCount, TSEncoding.PLAIN),
        Collections.nCopies(measurementCount, CompressionType.SNAPPY),
        null,
        false,
        Collections.emptySet(),
        new AtomicBoolean(true));
    final IMemMNode device = mtree.getNodeByPath(new PartialPath("root.sg.d"));
    final CountingChildren children = new CountingChildren();
    children.putAll(device.getChildren());
    device.setChildren(children);
    final PartialPath pattern = new PartialPath("root.sg.d.*");
    mtree.constructSchemaBlackList(pattern, new AtomicBoolean());
    final List<PartialPath> paths = mtree.getPreDeletedTimeSeries(pattern);
    Assert.assertEquals(measurementCount, paths.size());
    children.scans = 0;
    for (final PartialPath path : paths) {
      mtree.deleteTimeSeries(path);
    }
    Assert.assertEquals(0, children.scans);
    assertPathNotExist(mtree, new PartialPath("root.sg.d"));
    Assert.assertEquals(
        0, mtree.getNodeByPath(new PartialPath("root.sg")).getSubtreeMeasurementCount());
  }

  @Test
  public void testDirectChildCountsAfterSnapshotWithNestedDevice() throws Exception {
    final MTreeBelowSGMemoryImpl mtree = newMTree();
    for (final String path : Arrays.asList("root.sg.d.s1", "root.sg.d.child.s2")) {
      mtree.createTimeSeries(
          new MeasurementPath(path),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.SNAPPY,
          null,
          null,
          false,
          null);
    }
    final File snapshotDir = temporaryFolder.newFolder();
    Assert.assertTrue(mtree.createSnapshot(snapshotDir));
    final MemSchemaRegionStatistics statistics = newRegionStatistics();
    final MTreeBelowSGMemoryImpl recovered =
        MTreeBelowSGMemoryImpl.loadFromSnapshot(
            snapshotDir,
            "root.sg",
            statistics,
            new SchemaRegionMemMetric(statistics, "root.sg"),
            node -> {},
            node -> {},
            (node, table) -> {},
            node -> Collections.emptyMap(),
            node -> Collections.emptyMap());
    final IMemMNode device = recovered.getNodeByPath(new PartialPath("root.sg.d"));
    Assert.assertEquals(
        1, ((BasicInternalMNode) device.getBasicMNode()).getMeasurementChildCount());
    Assert.assertEquals(2, device.getSubtreeMeasurementCount());
    recovered.deleteTimeSeries(new PartialPath("root.sg.d.s1"));
    Assert.assertFalse(recovered.getNodeByPath(new PartialPath("root.sg.d")).isDevice());
    Assert.assertTrue(recovered.getNodeByPath(new PartialPath("root.sg.d.child")).isDevice());
    recovered.deleteTimeSeries(new PartialPath("root.sg.d.child.s2"));
    assertPathNotExist(recovered, new PartialPath("root.sg.d"));
  }

  @Test
  public void testAlignmentResetsWhenOnlyLogicalViewsRemain() throws Exception {
    final MTreeBelowSGMemoryImpl mtree = newMTree();
    mtree.createTimeSeries(
        new MeasurementPath("root.sg.d.s"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        false,
        null);
    final IMemMNode device = mtree.getNodeByPath(new PartialPath("root.sg.d"));
    // The OSS factory does not create logical views. Model a view child to verify cleanup
    // semantics.
    final IMemMNode view = mock(IMemMNode.class);
    final IMeasurementMNode<IMemMNode> viewMeasurement = mock(IMeasurementMNode.class);
    when(view.isMeasurement()).thenReturn(true);
    when(view.getAsMeasurementMNode()).thenReturn(viewMeasurement);
    when(viewMeasurement.isLogicalView()).thenReturn(true);
    device.addChild("view", view);
    device.addChild("view", view);
    Assert.assertEquals(
        2, ((BasicInternalMNode) device.getBasicMNode()).getMeasurementChildCount());
    mtree.deleteTimeSeries(new PartialPath("root.sg.d.s"));
    Assert.assertTrue(device.isDevice());
    Assert.assertNull(device.getAsDeviceMNode().isAlignedNullable());
    device.deleteChild("view");
    mtree.deleteEmptyInternalMNode(device.getAsDeviceMNode());
    assertPathNotExist(mtree, new PartialPath("root.sg.d"));
  }

  private static class CountingChildren extends MemMNodeContainer {
    private int scans;

    @Override
    public Collection<IMemMNode> values() {
      scans++;
      return super.values();
    }
  }

  @Test
  public void testPathAndTableDeviceChecksAfterClear() throws Exception {
    final MTreeBelowSGMemoryImpl mtree = newMTree();
    mtree.clear();

    Assert.assertFalse(mtree.checkDeviceNodeExists(new PartialPath("root.sg.d")));
    assertPathNotExist(mtree, new PartialPath("root.sg.d"));
    Assert.assertEquals(
        2,
        mtree.getTableDeviceNotExistNum(
            "table", Arrays.asList(new Object[] {"a"}, new Object[] {"b"})));
  }

  @Test
  public void testDeviceDescendantFlagIsMaintainedAcrossCreateDeleteAndRebuild() throws Exception {
    final MTreeBelowSGMemoryImpl mtree = newMTree();

    mtree.createTimeSeries(
        new MeasurementPath("root.sg.a.b.s1"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        false,
        null);

    IMemMNode database = mtree.getNodeByPath(new PartialPath("root.sg"));
    IMemMNode aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    IMemMNode bNode = mtree.getNodeByPath(new PartialPath("root.sg.a.b"));

    Assert.assertTrue(database.hasDeviceDescendant());
    Assert.assertFalse(aNode.isDevice());
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertTrue(bNode.isDevice());
    Assert.assertFalse(bNode.hasDeviceDescendant());

    mtree.createTimeSeries(
        new MeasurementPath("root.sg.a.s0"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        false,
        null);
    mtree.createTimeSeries(
        new MeasurementPath("root.sg.c.d.s2"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        false,
        null);

    aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    IMemMNode cNode = mtree.getNodeByPath(new PartialPath("root.sg.c"));
    Assert.assertTrue(aNode.isDevice());
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertTrue(cNode.hasDeviceDescendant());

    database.setHasDeviceDescendant(false);
    aNode.setHasDeviceDescendant(false);
    bNode.setHasDeviceDescendant(true);
    cNode.setHasDeviceDescendant(false);
    mtree.rebuildSubtreeMeasurementCount();

    database = mtree.getNodeByPath(new PartialPath("root.sg"));
    aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    bNode = mtree.getNodeByPath(new PartialPath("root.sg.a.b"));
    cNode = mtree.getNodeByPath(new PartialPath("root.sg.c"));
    Assert.assertTrue(database.hasDeviceDescendant());
    Assert.assertTrue(aNode.hasDeviceDescendant());
    Assert.assertFalse(bNode.hasDeviceDescendant());
    Assert.assertTrue(cNode.hasDeviceDescendant());

    mtree.deleteTimeSeries(new PartialPath("root.sg.a.s0"));
    aNode = mtree.getNodeByPath(new PartialPath("root.sg.a"));
    Assert.assertFalse(aNode.isDevice());
    Assert.assertTrue(aNode.hasDeviceDescendant());

    mtree.deleteTimeSeries(new PartialPath("root.sg.a.b.s1"));
    database = mtree.getNodeByPath(new PartialPath("root.sg"));
    Assert.assertTrue(database.hasDeviceDescendant());
    assertPathNotExist(mtree, new PartialPath("root.sg.a"));

    mtree.deleteTimeSeries(new PartialPath("root.sg.c.d.s2"));
    database = mtree.getNodeByPath(new PartialPath("root.sg"));
    Assert.assertFalse(database.hasDeviceDescendant());
    assertPathNotExist(mtree, new PartialPath("root.sg.c"));
  }

  @Test
  public void testDeviceDescendantFlagIsMaintainedAcrossTableDeviceDrop() throws Exception {
    final MTreeBelowSGMemoryImpl mtree = newMTree();
    final AtomicInteger attributePointer = new AtomicInteger();

    mtree.createOrUpdateTableDevice(
        "t",
        new String[] {"hebei", "p_1", "d_0"},
        attributePointer::getAndIncrement,
        ignored -> {});

    IMemMNode database = mtree.getNodeByPath(new PartialPath("root.sg"));
    final IMemMNode table = mtree.getNodeByPath(new PartialPath("root.sg.t"));
    final IMemMNode province = mtree.getNodeByPath(new PartialPath("root.sg.t.hebei"));
    final IMemMNode device = mtree.getNodeByPath(new PartialPath("root.sg.t.hebei.p_1.d_0"));
    Assert.assertTrue(database.hasDeviceDescendant());
    Assert.assertTrue(table.hasDeviceDescendant());
    Assert.assertTrue(province.hasDeviceDescendant());
    Assert.assertTrue(device.isDevice());
    Assert.assertFalse(device.hasDeviceDescendant());

    Assert.assertTrue(mtree.deleteTableDevice("t", ignored -> {}));
    database = mtree.getNodeByPath(new PartialPath("root.sg"));
    Assert.assertFalse(database.hasDeviceDescendant());
    assertPathNotExist(mtree, new PartialPath("root.sg.t"));
  }

  @Test
  public void testLeafDeviceWildcardSuffixUsesDirectMeasurementLookup() throws Exception {
    final PartialPath databasePath = PartialPath.getQualifiedDatabasePartialPath("root.sg");
    final MemSchemaRegionStatistics regionStatistics = newRegionStatistics();
    final CountingMemMTreeStore store =
        new CountingMemMTreeStore(
            databasePath, regionStatistics, new SchemaRegionMemMetric(regionStatistics, "root.sg"));
    final IMNodeFactory<IMemMNode> nodeFactory =
        MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();

    final IMemMNode databaseMNode = store.getRoot();
    final IMemMNode rootNode = store.generatePrefix(databasePath);
    final IMemMNode deviceNode =
        store.addChild(databaseMNode, "d1", nodeFactory.createInternalMNode(databaseMNode, "d1"));
    store.setToEntity(deviceNode);

    for (int i = 0; i < 64; i++) {
      final String measurement = i == 0 ? "target" : "s" + i;
      final IMeasurementMNode<IMemMNode> measurementMNode =
          nodeFactory.createMeasurementMNode(
              deviceNode.getAsDeviceMNode(),
              measurement,
              new MeasurementSchema(
                  measurement, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY),
              null);
      store.addChild(deviceNode, measurement, measurementMNode.getAsMNode());
    }

    store.watchParent(deviceNode);
    final List<String> matchedPaths = new ArrayList<>();
    try (final MeasurementCollector<Void, IMemMNode> collector =
        new MeasurementCollector<Void, IMemMNode>(
            rootNode,
            new PartialPath("root.sg.**.target"),
            store,
            false,
            SchemaConstant.ALL_MATCH_SCOPE) {
          @Override
          protected Void collectMeasurement(final IMeasurementMNode<IMemMNode> node) {
            matchedPaths.add(getCurrentMeasurementPathInTraverse(node).getFullPath());
            return null;
          }
        }) {
      collector.traverse();
    }

    Assert.assertEquals(Collections.singletonList("root.sg.d1.target"), matchedPaths);
    Assert.assertEquals(0, store.getWatchedTraverserIteratorCount());
    Assert.assertEquals(1, store.getWatchedChildLookupCount());
  }

  private static MTreeBelowSGMemoryImpl newMTree() throws Exception {
    final MemSchemaRegionStatistics regionStatistics = newRegionStatistics();
    return new MTreeBelowSGMemoryImpl(
        PartialPath.getQualifiedDatabasePartialPath("root.sg"),
        node -> Collections.emptyMap(),
        node -> Collections.emptyMap(),
        regionStatistics,
        new SchemaRegionMemMetric(regionStatistics, "root.sg"));
  }

  private static MemSchemaRegionStatistics newRegionStatistics() {
    return new MemSchemaRegionStatistics(0, new MemSchemaEngineStatistics());
  }

  private static void assertPathNotExist(final MTreeBelowSGMemoryImpl mtree, final PartialPath path)
      throws MetadataException {
    try {
      mtree.getNodeByPath(path);
      Assert.fail("Expected path not exist: " + path.getFullPath());
    } catch (final PathNotExistException ignored) {
      // expected
    }
  }

  private static class CountingMemMTreeStore extends MemMTreeStore {
    private IMemMNode watchedParent;
    private int watchedTraverserIteratorCount;
    private int watchedChildLookupCount;

    private CountingMemMTreeStore(
        final PartialPath rootPath,
        final MemSchemaRegionStatistics regionStatistics,
        final SchemaRegionMemMetric metric) {
      super(rootPath, regionStatistics, metric);
    }

    private void watchParent(final IMemMNode parent) {
      this.watchedParent = parent;
      this.watchedTraverserIteratorCount = 0;
      this.watchedChildLookupCount = 0;
    }

    private int getWatchedTraverserIteratorCount() {
      return watchedTraverserIteratorCount;
    }

    private int getWatchedChildLookupCount() {
      return watchedChildLookupCount;
    }

    @Override
    public IMemMNode getChild(final IMemMNode parent, final String name) {
      if (parent == watchedParent) {
        watchedChildLookupCount++;
      }
      return super.getChild(parent, name);
    }

    @Override
    public IMNodeIterator<IMemMNode> getTraverserIterator(
        final IMemMNode parent,
        final Map<Integer, Template> templateMap,
        final boolean skipPreDeletedSchema)
        throws MetadataException {
      if (parent == watchedParent) {
        watchedTraverserIteratorCount++;
      }
      return super.getTraverserIterator(parent, templateMap, skipPreDeletedSchema);
    }
  }
}
