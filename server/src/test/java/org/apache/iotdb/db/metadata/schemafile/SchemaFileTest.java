package org.apache.iotdb.db.metadata.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.org.bouncycastle.asn1.cms.MetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class SchemaFileTest {

  @Before
  public void setUp() {
    // EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    // EnvironmentUtils.cleanEnv();
  }

  @Test
  public void essentialTest() {
  }

  @Test
  public void initSchemaFile() throws IOException, MetadataException {
    ISchemaFile sf = new SchemaFile("tsg5");
    Iterator<IMNode> ite = new MTreeBFTIterator(virtualTriangleMTree(5));
    while (ite.hasNext()) {
      IMNode curNode = ite.next();
      if (!curNode.isMeasurement()) {
        sf.write(curNode);
      }
    }
    System.out.println(sf.inspect());
    sf.close();
  }

  @Test
  public void inspectFile() throws MetadataException, IOException {
    ISchemaFile sf = new SchemaFile("tsg5");
    System.out.println(sf.inspect());
  }

  /**
   * Steps inside SchemaFile:
   * 1. check storage group name with files
   * 2.
   * */
  @Test
  public void storeTree() {
    int x = 1;
    System.out.println(++x);
  }

  @Test
  public void testBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    short i = ReadWriteIOUtils.readShort(buffer);
    String a = "a";
    String b = "b";
    print(a.compareTo(b));
  }

  @Test
  public void bitOperation() {
    long src = 50332416L;

    int pi = ISchemaFile.getPageIndex(src);
    short si = ISchemaFile.getSegIndex(src);

    Assert.assertEquals(src, ISchemaFile.getGlobalIndex(pi, si));

    pi += 10;
    si += 5;

    long src2 = ISchemaFile.getGlobalIndex(pi, si);

    Assert.assertEquals(pi, ISchemaFile.getPageIndex(src2));
    Assert.assertEquals(si, ISchemaFile.getSegIndex(src2));

  }

  @Test
  public void keyBytes() {
    Iterator<IMNode> ite = new MTreeBFTIterator(virtualTriangleMTree(5));
    while (ite.hasNext()) {
      print(ite.next().getName());
    }
  }

  public static void print(Object o) {
    System.out.println(o.toString());
  }

  private IMNode virtualTriangleMTree(int size) {
    IMNode internalNode = new EntityMNode(null, "vRoot1");

    for (int idx = 0; idx < size; idx++){
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode = MeasurementMNode.getMeasurementMNode(internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }

    IMNode curNode = internalNode;
    for (int idx = 0; idx < size; idx++) {
      String nodeName = "int" + idx;
      IMNode newNode = new InternalMNode(curNode, nodeName);
      curNode.addChild(newNode);
      curNode = newNode;
    }

    for(int idx = 0; idx < 1000; idx ++) {
      IMeasurementSchema schema = new MeasurementSchema("finalM"+idx, TSDataType.FLOAT);
      IMeasurementMNode mNode = MeasurementMNode.getMeasurementMNode(
          internalNode.getAsEntityMNode(), "finalM"+idx, schema,"finalals");
      curNode.addChild(mNode);
    }
    IMeasurementSchema schema = new MeasurementSchema("finalM", TSDataType.FLOAT);
    IMeasurementMNode mNode = MeasurementMNode.getMeasurementMNode(
        internalNode.getAsEntityMNode(), "finalM", schema,"finalals");
    curNode.addChild(mNode);
    return internalNode;
  }
}
