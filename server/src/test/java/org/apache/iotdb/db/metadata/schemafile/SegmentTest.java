package org.apache.iotdb.db.metadata.schemafile;

import org.apache.iotdb.db.exception.metadata.RecordDuplicatedException;
import org.apache.iotdb.db.metadata.mnode.*;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISegment;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.Segment;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SegmentTest {

  @Before
  public void setUp() {
    // EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    // EnvironmentUtils.cleanEnv();
  }

  @Test
  public void flatTreeInsert() throws RecordDuplicatedException {
    Segment sf = new Segment(450);
    IMNode rNode = virtualFlatMTree(10);
    for (IMNode node: rNode.getChildren().values()) {
      int res = sf.insertRecord(node.getName(), RecordUtils.node2Buffer(node));
      // System.out.println(res);
    }
    sf.syncBuffer();

    // System.out.println(sf);
    ByteBuffer recMid01 = sf.getRecord("mid1");
    Assert.assertEquals("[measurementNode, alias: mid1als, type: FLOAT, encoding: PLAIN, compressor: SNAPPY]", RecordUtils.buffer2String(recMid01));

    int resInsertNode = sf.insertRecord(rNode.getName(), RecordUtils.node2Buffer(rNode));
    System.out.println(resInsertNode);
    System.out.println(sf);
    Assert.assertEquals("[entityNode, not aligned, not using template.]", RecordUtils.buffer2String(sf.getRecord("vRoot1")));

    Segment nsf = new Segment(sf.getBufferCopy(), false);
    System.out.println(nsf);
    printBuffer(nsf.getBufferCopy());
    ByteBuffer nrec = nsf.getRecord("mid1");
    Assert.assertEquals("[measurementNode, alias: mid1als, type: FLOAT, encoding: PLAIN, compressor: SNAPPY]", RecordUtils.buffer2String(nsf.getRecord("mid1")));
    Assert.assertEquals("[entityNode, not aligned, not using template.]", RecordUtils.buffer2String(nsf.getRecord("vRoot1")));

    ByteBuffer newBuffer = ByteBuffer.allocate(1500);
    sf.extendsTo(newBuffer);
    ISegment newSeg = Segment.loadAsSegment(newBuffer);
    System.out.println(newSeg);
    Assert.assertEquals(RecordUtils.buffer2String(sf.getRecord("mid4")),
        RecordUtils.buffer2String(newSeg.getRecord("mid4")));
    Assert.assertEquals(sf.getRecord("aaa"), nsf.getRecord("aaa"));
  }

  private IMNode virtualFlatMTree(int childSize) {
    IMNode internalNode = new EntityMNode(null, "vRoot1");

    for (int idx = 0; idx < childSize; idx++){
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode = MeasurementMNode.getMeasurementMNode(internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }
    return internalNode;
  }

  @Test
  public void bufferTest() {
    ByteBuffer buffer1 = ByteBuffer.allocate(100);
    ByteBuffer buffer2 = buffer1.slice();
    buffer1.put("12346".getBytes());
    buffer1.clear();

    buffer2.position(10);
    buffer2.put("091234".getBytes());
    buffer2.clear();
    printBuffer(buffer1);
    printBuffer(buffer2);

    byte[] a = new byte[10];
    byte[] b = a;

    a[0] = (byte) 7;
    System.out.println(a[0]);
    System.out.println(b[0]);
  }

  private void printBuffer(ByteBuffer buf) {
    int pos = buf.position();
    int lim = buf.limit();
    ByteBuffer bufRep = buf.slice();
    while (pos < lim) {
      System.out.print(buf.get(pos));
      System.out.print(" ");
      pos++;
    }
    System.out.println("");
  }
}
