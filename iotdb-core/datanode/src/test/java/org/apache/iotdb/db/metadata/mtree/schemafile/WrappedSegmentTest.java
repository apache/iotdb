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
package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.SchemaEngineMode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISegment;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.RecordUtils;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.WrappedSegment;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class WrappedSegmentTest {

  private static final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();

  @Before
  public void setUp() {
    CommonDescriptor.getInstance()
        .getConfig()
        .setSchemaEngineMode(SchemaEngineMode.PBTree.toString());
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    CommonDescriptor.getInstance()
        .getConfig()
        .setSchemaEngineMode(SchemaEngineMode.Memory.toString());
  }

  @Test
  public void testUpdateAlias() throws MetadataException {
    WrappedSegment sf = new WrappedSegment(500);
    ICachedMNode node = getMeasurementNode(null, "s1", null);
    sf.insertRecord("s1", RecordUtils.node2Buffer(node));
    node.getAsMeasurementMNode().setAlias("alias1");
    sf.updateRecord("s1", RecordUtils.node2Buffer(node));
    node.getAsMeasurementMNode().setAlias("alias2");
    sf.updateRecord("s1", RecordUtils.node2Buffer(node));
    Assert.assertEquals(null, sf.getRecordByAlias("alias1"));
    ICachedMNode node1 = sf.getRecordByAlias("alias2");
    Assert.assertTrue(node1.isMeasurement());
    Assert.assertEquals("alias2", node1.getAsMeasurementMNode().getAlias());
  }

  @Test
  public void flatTreeInsert() throws MetadataException {
    WrappedSegment sf = new WrappedSegment(500);
    ICachedMNode rNode = virtualFlatMTree(10);
    for (ICachedMNode node : rNode.getChildren().values()) {
      sf.insertRecord(node.getName(), RecordUtils.node2Buffer(node));
    }
    sf.syncBuffer();

    ByteBuffer recMid01 = sf.getRecord("mid1");
    Assert.assertEquals(
        "[measurementNode, alias: mid1als, type: FLOAT, encoding: GORILLA, compressor: LZ4]",
        RecordUtils.buffer2String(recMid01));

    int resInsertNode = sf.insertRecord(rNode.getName(), RecordUtils.node2Buffer(rNode));
    System.out.println(resInsertNode);
    System.out.println(sf);
    Assert.assertEquals(
        "[entityNode, not aligned, not using template.]",
        RecordUtils.buffer2String(sf.getRecord("vRoot1")));

    WrappedSegment nsf = new WrappedSegment(sf.getBufferCopy(), false);
    System.out.println(nsf);
    ByteBuffer nrec = nsf.getRecord("mid1");
    Assert.assertEquals(
        "[measurementNode, alias: mid1als, type: FLOAT, encoding: GORILLA, compressor: LZ4]",
        RecordUtils.buffer2String(nsf.getRecord("mid1")));
    Assert.assertEquals(
        "[entityNode, not aligned, not using template.]",
        RecordUtils.buffer2String(nsf.getRecord("vRoot1")));

    ByteBuffer newBuffer = ByteBuffer.allocate(1500);
    sf.extendsTo(newBuffer);
    ISegment newSeg = WrappedSegment.loadAsSegment(newBuffer);
    System.out.println(newSeg);
    Assert.assertEquals(
        RecordUtils.buffer2String(sf.getRecord("mid4")),
        RecordUtils.buffer2String(((WrappedSegment) newSeg).getRecord("mid4")));
    Assert.assertEquals(sf.getRecord("aaa"), nsf.getRecord("aaa"));
  }

  private ICachedMNode virtualFlatMTree(int childSize) {
    ICachedMNode internalNode = nodeFactory.createDeviceMNode(null, "vRoot1").getAsMNode();

    for (int idx = 0; idx < childSize; idx++) {
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      internalNode.addChild(
          nodeFactory
              .createMeasurementMNode(
                  internalNode.getAsDeviceMNode(), measurementId, schema, measurementId + "als")
              .getAsMNode());
    }
    return internalNode;
  }

  @Test
  public void evenSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(500);
    ISegment<ByteBuffer, ICachedMNode> seg = WrappedSegment.initAsSegment(buffer);
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"};
    ICachedMNode mNode = getMeasurementNode(null, "m", null);
    ByteBuffer buf = RecordUtils.node2Buffer(mNode);

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], buf);
    }

    ByteBuffer buf2 = ByteBuffer.allocate(500);
    String sk = seg.splitByKey("a55", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a5", sk);
    Assert.assertEquals(4, seg.getAllRecords().size());
    Assert.assertEquals(6, WrappedSegment.loadAsSegment(buf2).getAllRecords().size());

    seg.syncBuffer();
    ISegment seg3 = WrappedSegment.loadAsSegment(buffer);
    Assert.assertEquals(seg.getAllRecords().size(), seg3.getAllRecords().size());

    buf2.clear();
    seg.splitByKey(null, null, buf2, false);
    Assert.assertEquals(2, seg.getAllRecords().size());

    buf2.clear();
    seg.splitByKey(null, null, buf2, false);
    Assert.assertEquals(1, seg.getAllRecords().size());

    buf2.clear();
    seg.splitByKey("b", buf, buf2, false);
    Assert.assertEquals(1, seg.getAllRecords().size());
  }

  @Test
  public void increasingSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(500);
    ByteBuffer buf2 = ByteBuffer.allocate(500);
    ISegment<ByteBuffer, ICachedMNode> seg = WrappedSegment.initAsSegment(buffer);
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"};
    ICachedMNode mNode = nodeFactory.createInternalMNode(null, "m");
    ByteBuffer buf = RecordUtils.node2Buffer(mNode);

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], buf);
    }
    // at largest
    String sk = seg.splitByKey("a9", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a9", sk);
    Assert.assertEquals(1, WrappedSegment.loadAsSegment(buf2).getAllRecords().size());

    // at second-largest
    seg.insertRecord("a71", buf);
    seg.insertRecord("a72", buf);
    buf2.clear();
    sk = seg.splitByKey("a73", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a73", sk);
    Assert.assertEquals(2, WrappedSegment.loadAsSegment(buf2).getAllRecords().size());

    // at lower half
    seg.insertRecord("a00", buf);
    seg.insertRecord("a01", buf);
    buf2.clear();
    sk = seg.splitByKey("a02", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a3", sk);
    Assert.assertEquals(5, seg.getAllRecords().size());
  }

  @Test
  public void decreasingSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(500);
    ByteBuffer buf2 = ByteBuffer.allocate(500);
    ISegment<ByteBuffer, ICachedMNode> seg = WrappedSegment.initAsSegment(buffer);
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"};
    ICachedMNode mNode = nodeFactory.createInternalMNode(null, "m");
    ByteBuffer buf = RecordUtils.node2Buffer(mNode);

    for (int i = test.length - 1; i >= 0; i--) {
      seg.insertRecord(test[i], buf);
    }
    // at second-smallest
    seg.insertRecord("a12", buf);
    seg.insertRecord("a11", buf);

    String sk = seg.splitByKey("a10", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a11", sk);
    Assert.assertEquals(2, seg.getAllRecords().size());
    Assert.assertEquals(9, WrappedSegment.loadAsSegment(buf2).getAllRecords().size());

    // at higher half
    test = new String[] {"a5", "a6", "a7", "a8"};
    for (int i = test.length - 1; i >= 0; i--) {
      seg.insertRecord(test[i], buf);
    }
    seg.insertRecord("a84", buf);
    seg.insertRecord("a83", buf);
    buf2.clear();
    sk = seg.splitByKey("a82", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a7", sk);
    Assert.assertEquals(4, seg.getAllRecords().size());

    // at third-smallest
    seg.insertRecord("a43", buf);
    seg.insertRecord("a42", buf);

    buf2.clear();
    sk = seg.splitByKey("a41", buf, buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a42", sk);
    Assert.assertEquals(3, seg.getAllRecords().size());
  }

  public void print(ByteBuffer buf) throws MetadataException {
    System.out.println(WrappedSegment.loadAsSegment(buf).inspect());
  }

  public void print(Object s) {
    System.out.println(s);
  }

  private ICachedMNode getMeasurementNode(ICachedMNode par, String name, String alias) {
    IMeasurementSchema schema = new MeasurementSchema(name, TSDataType.FLOAT);
    return nodeFactory
        .createMeasurementMNode(par == null ? null : par.getAsDeviceMNode(), name, schema, alias)
        .getAsMNode();
  }
}
