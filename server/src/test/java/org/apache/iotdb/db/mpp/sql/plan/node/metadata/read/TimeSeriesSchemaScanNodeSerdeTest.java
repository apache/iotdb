package org.apache.iotdb.db.mpp.sql.plan.node.metadata.read;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class TimeSeriesSchemaScanNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() throws IllegalPathException {
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("offset"), 10);
    LimitNode limitNode = new LimitNode(new PlanNodeId("limit"), 10);
    SchemaMergeNode schemaMergeNode = new SchemaMergeNode(new PlanNodeId("schemaMerge"));
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("exchange"));
    TimeSeriesSchemaScanNode timeSeriesSchemaScanNode =
        new TimeSeriesSchemaScanNode(
            new PlanNodeId("timeSeriesSchemaScan"),
            new PartialPath("root.sg.device.sensor"),
            null,
            null,
            10,
            0,
            false,
            false,
            false);
    FragmentSinkNode fragmentSinkNode = new FragmentSinkNode(new PlanNodeId("fragmentSink"));
    fragmentSinkNode.addChild(timeSeriesSchemaScanNode);
    fragmentSinkNode.setDownStream(
        new Endpoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    exchangeNode.addChild(schemaMergeNode);
    exchangeNode.setRemoteSourceNode(fragmentSinkNode);
    exchangeNode.setUpstream(
        new Endpoint("127.0.0.1", 6667),
        new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
        new PlanNodeId("test"));
    offsetNode.addChild(exchangeNode);
    limitNode.addChild(offsetNode);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    limitNode.serialize(byteBuffer);
    byteBuffer.flip();
    LimitNode limitNode1 = (LimitNode) PlanNodeDeserializeHelper.deserialize(byteBuffer);
    Assert.assertEquals(limitNode, limitNode1);
  }
}
