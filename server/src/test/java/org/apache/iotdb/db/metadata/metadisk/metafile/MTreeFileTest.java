package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MTreeFileTest {

  private static final String BASE_PATH = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
  private static final String MTREE_FILEPATH = BASE_PATH + File.separator + "mtreefile_test.bin";

  private MTreeFile mTreeFile;

  @Before
  public void setUp() throws IOException {
    EnvironmentUtils.envSetUp();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    mTreeFile = new MTreeFile(MTREE_FILEPATH);
  }

  @After
  public void tearDown() throws Exception {
    mTreeFile.close();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSimpleMNodeRW() throws IOException {
    IMNode mNode = new InternalMNode(null, "root");
    mTreeFile.write(mNode);
    mNode = mTreeFile.read(mNode.getPersistenceInfo());
    Assert.assertEquals("root", mNode.getName());
  }

  @Test
  public void testPathRW() throws IOException {
    IMNode root = new InternalMNode(null, "root");
    IMNode p = new InternalMNode(root, "p");
    root.addChild("p", p);
    StorageGroupMNode s = new StorageGroupMNode(root.getChild("p"), "s", 0);
    p.addChild("s", s);
    MeasurementMNode m =
        new MeasurementMNode(null, "t", new MeasurementSchema("t", TSDataType.TEXT), "m");
    m.updateCachedLast(
        new TimeValuePair(1L, TsPrimitiveType.getByType(TSDataType.TEXT, new Binary("cache"))),
        false,
        0L);
    s.addChild("t", m);

    mTreeFile.writeRecursively(root);
    IMNode mNode = mTreeFile.read("root.p.s");
    Assert.assertTrue(mNode.isStorageGroup());
    Assert.assertTrue(mNode.hasChild("t"));
    mNode = mTreeFile.read("root.p.s.t");
    Assert.assertEquals("t", mNode.getName());
    Assert.assertTrue(mNode.isMeasurement());
    m = (MeasurementMNode) mNode;
    Assert.assertEquals("m", m.getAlias());
    Assert.assertEquals(1L, m.getCachedLast().getTimestamp());
    Assert.assertEquals("cache", m.getCachedLast().getValue().getBinary().getStringValue());
  }

  @Test
  public void testBigMNode() throws IOException {
    IMNode root = new InternalMNode(null, "root");
    IMNode p = new InternalMNode(root, "p");
    root.addChild("p", p);
    StorageGroupMNode s = new StorageGroupMNode(root.getChild("p"), "s", 0);
    p.addChild("s", s);
    for (int i = 0; i < 10000; i++) {
      MeasurementMNode m =
          new MeasurementMNode(null, "t" + i, new MeasurementSchema("t", TSDataType.TEXT), "m" + i);
      m.updateCachedLast(
          new TimeValuePair(1L, TsPrimitiveType.getByType(TSDataType.TEXT, new Binary("cache"))),
          false,
          0L);
      s.addChild("t" + i, m);
    }
    mTreeFile.writeRecursively(root);
    IMNode mNode = mTreeFile.read("root.p.s");
    Assert.assertEquals(10000, mNode.getChildren().size());
  }

  @Test
  public void testSimpleTreeRW() throws IOException {
    IMNode mNode = getSimpleTree();
    mTreeFile.writeRecursively(mNode);
    mNode = mTreeFile.readRecursively(mNode.getPersistenceInfo());
    Assert.assertEquals(
        "root\r\n"
            + "root.s1\r\n"
            + "root.s1.t1\r\n"
            + "root.s1.t2\r\n"
            + "root.s1.t2.z1\r\n"
            + "root.s2\r\n"
            + "root.s2.t1\r\n"
            + "root.s2.t2\r\n",
        treeToStringDFT(mNode));
  }

  private IMNode getSimpleTree() {
    IMNode root = new InternalMNode(null, "root");
    root.addChild("s1", new InternalMNode(root, "s1"));
    root.addChild("s2", new InternalMNode(root, "s2"));
    root.getChild("s1").addChild("t1", new InternalMNode(root.getChild("s1"), "t1"));
    root.getChild("s1").addChild("t2", new InternalMNode(root.getChild("s1"), "t2"));
    root.getChild("s1")
        .getChild("t2")
        .addChild("z1", new InternalMNode(root.getChild("s1").getChild("t2"), "z1"));
    root.getChild("s2").addChild("t1", new InternalMNode(root.getChild("s2"), "t1"));
    root.getChild("s2").addChild("t2", new InternalMNode(root.getChild("s2"), "t2"));
    return root;
  }

  @Test
  public void testMTreeRW() throws IOException {
    IMNode mNode = getMTree();
    mTreeFile.writeRecursively(mNode);
    mNode = mTreeFile.readRecursively(mNode.getPersistenceInfo());
    Assert.assertEquals(
        "root\r\n"
            + "root.p\r\n"
            + "root.p.s1\r\n"
            + "root.p.s1.t1\r\n"
            + "root.p.s1.t2\r\n"
            + "root.p.s2\r\n"
            + "root.p.s2.t1\r\n"
            + "root.p.s2.t2\r\n",
        treeToStringDFT(mNode));
    Assert.assertTrue(mNode.getChild("p").getChild("s1").isStorageGroup());
    Assert.assertTrue(mNode.getChild("p").getChild("s2").isStorageGroup());
    StorageGroupMNode s1 = (StorageGroupMNode) mNode.getChild("p").getChild("s1");
    StorageGroupMNode s2 = (StorageGroupMNode) mNode.getChild("p").getChild("s2");
    Assert.assertEquals(1000, s1.getDataTTL());
    Assert.assertEquals(2000, s2.getDataTTL());
    Assert.assertTrue(mNode.getChild("p").getChild("s1").getChild("t1").isMeasurement());
    Assert.assertTrue(mNode.getChild("p").getChild("s2").getChild("t2").isMeasurement());
  }

  private IMNode getMTree() {
    IMNode root = new InternalMNode(null, "root");
    IMNode p = new InternalMNode(root, "p");
    root.addChild("p", p);
    StorageGroupMNode s1 = new StorageGroupMNode(p, "s1", 1000);
    StorageGroupMNode s2 = new StorageGroupMNode(p, "s2", 2000);
    p.addChild("s1", s1);
    p.addChild("s2", s2);
    s1.addChild("t1", new MeasurementMNode(null, "t1", new MeasurementSchema(), null));
    s1.addChild("t2", new MeasurementMNode(null, "t2", new MeasurementSchema(), null));
    s2.addChild("t1", new MeasurementMNode(null, "t1", new MeasurementSchema(), null));
    s2.addChild("t2", new MeasurementMNode(null, "t2", new MeasurementSchema(), null));
    Map<String, String> props = new HashMap<>();
    props.put("a", "1");
    ((MeasurementMNode) s1.getChild("t1")).getSchema().setProps(props);
    ((MeasurementMNode) s2.getChild("t2")).getSchema().setProps(props);
    return root;
  }

  private void showTree(IMNode mNode) {
    if (mNode == null) {
      return;
    }
    System.out.println(mNode.getFullPath());
    for (IMNode child : mNode.getChildren().values()) {
      showTree(child);
    }
  }

  private String treeToStringDFT(IMNode mNode) {
    StringBuilder stringBuilder = new StringBuilder();
    dft(mNode, stringBuilder);
    return stringBuilder.toString();
  }

  private void dft(IMNode mNode, StringBuilder stringBuilder) {
    if (mNode == null) {
      return;
    }
    stringBuilder.append(mNode.getFullPath()).append("\r\n");
    for (IMNode child : mNode.getChildren().values()) {
      dft(child, stringBuilder);
    }
  }
}
