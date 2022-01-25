package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.db.exception.metadata.RecordDuplicatedException;
import org.apache.iotdb.db.exception.metadata.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.SegmentNotFoundException;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SchemaPageTest {

  @Before
  public void setUp() {
    // EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    // EnvironmentUtils.cleanEnv();
  }

  @Test
  public void flatTreeInsert()
      throws SchemaPageOverflowException, IOException, SegmentNotFoundException,
          RecordDuplicatedException {
    ISchemaPage page = SchemaPage.initPage(ByteBuffer.allocate(SchemaFile.PAGE_LENGTH), 0);
    IMNode root = virtualFlatMTree(15);
    for (int i = 0; i < 7; i++) {
      page.allocNewSegment(SchemaFile.SEG_SIZE_LST[0]);
      int cnt = 0;
      for (IMNode child : root.getChildren().values()) {
        cnt++;
        try {
          page.write((short) i, child.getName(), RecordUtils.node2Buffer(child));
        } catch (org.apache.iotdb.db.exception.metadata.MetadataException e) {
          e.printStackTrace();
        }
        if (cnt > i) {
          break;
        }
      }
    }

    ByteBuffer newBuf = ByteBuffer.allocate(SchemaFile.PAGE_LENGTH);
    page.syncPageBuffer();
    page.getPageBuffer(newBuf);
    ISchemaPage newPage = SchemaPage.loadPage(newBuf, 0);
    System.out.println(newPage.inspect());
  }

  private IMNode virtualFlatMTree(int childSize) {
    IMNode internalNode = new EntityMNode(null, "vRoot1");

    for (int idx = 0; idx < childSize; idx++) {
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode =
          MeasurementMNode.getMeasurementMNode(
              internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
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
