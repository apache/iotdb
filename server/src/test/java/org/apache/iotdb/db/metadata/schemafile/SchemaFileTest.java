package org.apache.iotdb.db.metadata.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.Segment;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

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
    ISchemaFile sf = new SchemaFile("tsg5", true);

    IMNode root = virtualTriangleMTree(5);
    IMNode int0 = root.getChild("int0");
    IMNode int1 = root.getChild("int0").getChild("int1");
    IMNode int4 = root.getChild("int0").getChild("int1").getChild("int2").getChild("int3").getChild("int4");
    int0.getChildren().getSegment().getNewChildBuffer().put("mint1", getMeasurementNode(int0, "mint1", "alas"));

    Iterator<IMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      IMNode curNode = ite.next();
      if (!curNode.isMeasurement()) {
        sf.writeMNode(curNode);
      }
    }
    System.out.println(((SchemaFile)sf).inspect());

    int0.getChildren().getSegment().getNewChildBuffer().clear();
    int0.getChildren().getSegment().getUpdatedChildBuffer().put("mint1", getMeasurementNode(int0, "mint1", "alas99999"));

    sf.writeMNode(int0);
    System.out.println(((SchemaFile)sf).inspect());

    int1.getChildren().getSegment().getNewChildBuffer().clear();
    int1.getChildren().getSegment().getNewChildBuffer().put("int1newM", getMeasurementNode(int0, "int1newM", "alas"));

    sf.writeMNode(int1);
    System.out.println(((SchemaFile)sf).inspect());

    int4.getChildren().getSegment().getNewChildBuffer().clear();
    int4.getChildren().getSegment().getNewChildBuffer().put("AAAAA", getMeasurementNode(int0, "AAAAA", "alas"));

    sf.writeMNode(int4);
    System.out.println(((SchemaFile)sf).inspect());

    int4.getChildren().getSegment().getNewChildBuffer().clear();
    int4.getChildren().getSegment().getUpdatedChildBuffer().put("AAAAA", getMeasurementNode(int0, "AAAAA", "BBBBBB"));

    sf.writeMNode(int4);
    System.out.println(((SchemaFile)sf).inspect());

    int4.getChildren().getSegment().getUpdatedChildBuffer().clear();
    int4.getChildren().getSegment().getUpdatedChildBuffer().put("finalM191", getMeasurementNode(int0, "finalM191", "ALLLLLLLLLLLLLLLLLLLLfinalM191"));

    sf.writeMNode(int4);
    System.out.println(((SchemaFile)sf).inspect());

    sf.close();
  }

  @Test
  public void inspectFile() throws MetadataException, IOException {
    ISchemaFile sf = new SchemaFile("tsg5");
    System.out.println(((SchemaFile)sf).inspect());
  }

  @Test
  public void testRead() throws MetadataException, IOException {
    IMNode node = new InternalMNode(null, "test");
    node.getChildren().getSegment().setSegmentAddress(0L);
    ISchemaFile sf = new SchemaFile("tsg5");
    IMNode target = sf.getChildNode(node, "aa1");
    Assert.assertEquals("aa1als", target.getAsMeasurementMNode().getAlias());
    sf.close();
  }

  @Test
  public void testGetChildren() throws MetadataException, IOException {
    IMNode node = new InternalMNode(null, "test");
    node.getChildren().getSegment().setSegmentAddress(196608L);
    ISchemaFile sf = new SchemaFile("tsg5");

    Iterator<IMNode> res = sf.getChildren(node);
    int cnt = 0;
    while (res.hasNext()) {
      System.out.println(res.next().getName());
      cnt ++;
    }
    System.out.println(cnt);

  }

  @Test
  public void testVerticalTree() throws MetadataException, IOException {
    ISchemaFile sf = new SchemaFile("vt", true);
    IMNode root = getVerticalTree(100, "VT");
    Iterator<IMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      sf.writeMNode(ite.next());
    }
    printSF(sf);

    IMNode vt1 = getNode(root, "root.VT_0.VT_1");
    vt1.getChildren().getSegment().getNewChildBuffer().clear();
    addMeasurementChild(vt1, "newM");
    sf.writeMNode(vt1);
    printSF(sf);

    IMNode vt0 = getNode(root, "root.VT_0");
    Assert.assertEquals(vt1.getChildren().getSegment().getSegmentAddress(),
        RecordUtils.getRecordSegAddr(getSegment(sf, vt0.getChildren().getSegment().getSegmentAddress()).getRecord("VT_1")));
    Assert.assertEquals(2, getSegment(sf, vt1.getChildren().getSegment().getSegmentAddress()).getKeyOffsetList().size());
    sf.close();
  }

  private void printSF(ISchemaFile file) throws IOException, MetadataException {
    System.out.println(((SchemaFile)file).inspect());
  }

  private SchemaPage getPage(ISchemaFile sf, long addr) throws MetadataException, IOException {
    return ((SchemaFile)sf).getPage(SchemaFile.getPageIndex(addr));
  }

  private Segment getSegment(ISchemaFile sf, long addr) throws MetadataException, IOException {
    return getPage(sf, addr).getSegmentTest(SchemaFile.getSegIndex(addr));
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
      IMNode newNode = new EntityMNode(curNode, nodeName);
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

  private IMNode getFlatTree(int flatSize, String id) {
    IMNode internalNode = new EntityMNode(null, "vRoot1");

    for (int idx = 0; idx < flatSize; idx++){
      String measurementId = id + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode = MeasurementMNode.getMeasurementMNode(internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }

    return internalNode;
  }

  private IMNode getVerticalTree(int height, String id) {
    IMNode root = new EntityMNode(null, "root");
    int cnt = 0;
    IMNode cur = root;
    while (cnt < height) {
      cur.addChild(new EntityMNode(cur, id + "_" + cnt));
      cur = cur.getChild(id+"_"+cnt);
      cnt++;
    }
    return root;
  }

  private void addMeasurementChild(IMNode par, String mid) {
    par.addChild(getMeasurementNode(par, mid, mid+"alias"));
  }

  private IMeasurementSchema getSchema(String id) {
    return new MeasurementSchema(id, TSDataType.FLOAT);
  }

  private IMNode getNode(IMNode root, String path) throws MetadataException{
    String[] pathNodes = MetaUtils.splitPathToDetachedPath(path);
    IMNode cur = root;
    for (String node : pathNodes) {
      if (!node.equals("root")) {
        cur = cur.getChild(node);
      }
    }
    return cur;
  }

  private IMNode getFlatTree(int flatSize) {
    return getFlatTree(flatSize, "app");
  }

  private Iterator<IMNode> getTreeBFT(IMNode root) {
    return new Iterator<IMNode>() {
      Queue<IMNode> queue = new LinkedList<IMNode>();
      {
        this.queue.add(root);
      }

      @Override
      public boolean hasNext() {
        return queue.size() > 0;
      }

      @Override
      public IMNode next() {
        IMNode curNode = queue.poll();
        if (!curNode.isMeasurement() && curNode.getChildren().size() > 0) {
          for (IMNode child : curNode.getChildren().values()) {
            queue.add(child);
          }
        }
        return curNode;
      }
    };
  }

  private IMNode getInternalWithSegAddr(IMNode par, String name, long segAddr) {
    IMNode node = new EntityMNode(par, name);
    node.getChildren().getSegment().setSegmentAddress(segAddr);
    return node;
  }

  private IMNode getMeasurementNode(IMNode par, String name, String alias) {
    IMeasurementSchema schema = new MeasurementSchema(name, TSDataType.FLOAT);
    IMeasurementMNode mNode = MeasurementMNode.getMeasurementMNode(par.getAsEntityMNode(), name, schema,  alias);
    return mNode;
  }

}
