package org.apache.iotdb.db.mpp.plan.plan.node.process;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class SingleDeviceViewNodeSerdeTest {

  private static final List<String> outputColumnNames = Arrays.asList("Time", "Device", "value");

  @Test
  public void testCacheOutputColumnNames() throws IllegalPathException {
    SingleDeviceViewNode singleDeviceViewNode =
        new SingleDeviceViewNode(
            new PlanNodeId("TestSingleDeviceViewNode"),
            outputColumnNames,
            "TestDevice",
            Arrays.asList(1, 2));

    // when cache is false
    ByteBuffer byteBufferWithoutCache = ByteBuffer.allocate(2048);
    singleDeviceViewNode.serialize(byteBufferWithoutCache);
    byteBufferWithoutCache.flip();
    Assert.assertNull(
        PlanNodeDeserializeHelper.deserialize(byteBufferWithoutCache).getOutputColumnNames());

    // when cache is true
    singleDeviceViewNode.setCacheOutputColumnNames(true);
    ByteBuffer byteBufferWithCache = ByteBuffer.allocate(2048);
    singleDeviceViewNode.serialize(byteBufferWithCache);
    byteBufferWithCache.flip();
    Assert.assertEquals(
        PlanNodeDeserializeHelper.deserialize(byteBufferWithCache), singleDeviceViewNode);
  }
}
