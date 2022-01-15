package org.apache.iotdb.db.metadata.schemafile;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class RecordUtilTests {

  @Before
  public void setUp() {
    // EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    // EnvironmentUtils.cleanEnv();
  }

  @Test
  public void internalNodeTest() {
    IMNode oneNode = new InternalMNode(null, "abcd");
    oneNode.getChildren().getSegment().setSegmentAddress(1234567L);
    oneNode.setUseTemplate(true);
    ByteBuffer buffer = RecordUtils.node2Buffer(oneNode);
    IMNode node2 = RecordUtils.buffer2Node("abcd", buffer);
    Assert.assertEquals(node2.getChildren().getSegment().getSegmentAddress(), 1234567L);
    Assert.assertEquals(node2.isUseTemplate(), oneNode.isUseTemplate());
  }

  @Test
  public void measurementTest() {
    IMeasurementSchema schema = new MeasurementSchema("amn", TSDataType.FLOAT, TSEncoding.BITMAP, CompressionType.GZIP);
    IMNode amn = MeasurementMNode.getMeasurementMNode(null, "amn", schema, "anothername");
    ByteBuffer buffer = RecordUtils.node2Buffer(amn);
    IMNode node2 = RecordUtils.buffer2Node("amn", buffer);

    Assert.assertEquals(TSDataType.FLOAT, node2.getAsMeasurementMNode().getDataType("amn"));
    Assert.assertEquals(node2.getAsMeasurementMNode().getAlias(), amn.getAsMeasurementMNode().getAlias());
  }

}
