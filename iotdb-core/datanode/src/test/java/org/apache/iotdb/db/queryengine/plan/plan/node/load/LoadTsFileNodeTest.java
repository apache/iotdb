package org.apache.iotdb.db.queryengine.plan.plan.node.load;

import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class LoadTsFileNodeTest {

  @Test
  public void testLoadSingleTsFileNode() {
    TsFileResource resource = new TsFileResource(new File("1"));
    LoadSingleTsFileNode node = new LoadSingleTsFileNode(new PlanNodeId(""), resource, true);
    Assert.assertTrue(node.isDeleteAfterLoad());
    Assert.assertEquals(resource, node.getTsFileResource());
    Assert.assertNull(node.getLocalRegionReplicaSet());
    Assert.assertNull(node.getRegionReplicaSet());
    Assert.assertNull(node.getChildren());
    Assert.assertNull(node.getOutputColumnNames());
    try {
      node.clone();
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    try {
      node.splitByPartition(new Analysis());
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    Assert.assertEquals(0, node.allowedChildCount());
    Assert.assertEquals("LoadSingleTsFileNode{tsFile=1, needDecodeTsFile=false}", node.toString());
    node.clean();
  }

  @Test
  public void testLoadTsFilePieceNode() {
    LoadTsFilePieceNode node = new LoadTsFilePieceNode(new PlanNodeId(""), new File("1"));
    Assert.assertEquals(0, node.getDataSize());
    Assert.assertEquals(new ArrayList<>(), node.getAllTsFileData());
    Assert.assertEquals(node.getTsFile(), new File("1"));
    Assert.assertNull(node.getRegionReplicaSet());
    Assert.assertNull(node.getChildren());
    try {
      node.clone();
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    try {
      node.splitByPartition(new Analysis());
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    Assert.assertEquals(0, node.allowedChildCount());
    Assert.assertEquals("LoadTsFilePieceNode{tsFile=1, dataSize=0}", node.toString());
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    node.serialize(buffer);
    LoadTsFilePieceNode node1 = (LoadTsFilePieceNode) LoadTsFilePieceNode.deserialize(buffer);
    Assert.assertEquals(node.getTsFile(), node1.getTsFile());
  }
}
