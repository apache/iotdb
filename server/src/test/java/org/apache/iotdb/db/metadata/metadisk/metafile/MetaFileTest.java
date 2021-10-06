package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetaFileTest {

  private static String BASE_PATH = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
  private static String MTREE_FILEPATH = BASE_PATH + File.separator + "MetaFileTest_metafile.bin";

  private MetaFile metaFile;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    metaFile = new MetaFile(MTREE_FILEPATH);
  }

  @After
  public void tearDown() throws Exception {
    metaFile.close();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSimpleMNodeRW() throws IOException {
    IMNode mNode = new InternalMNode(null, "root");
    metaFile.write(mNode);
    Assert.assertNotEquals(0, mNode.getPersistenceInfo().getStartPosition());
    mNode = metaFile.read(mNode.getPersistenceInfo());
    Assert.assertEquals("root", mNode.getName());
  }

  @Test
  public void testPathRW() throws IOException {
    IMNode root = new InternalMNode(null, "root");
    IMNode p = new InternalMNode(root, "p");
    root.addChild("p", p);
    StorageGroupMNode s = new StorageGroupMNode(root.getChild("p"), "s", 0);
    p.addChild("s", s);
    s.addChild("t", new MeasurementMNode(null, "t", new MeasurementSchema(), null));
    metaFile.writeRecursively(root);
    IMNode mNode = metaFile.read("root.p.s.t");
    Assert.assertEquals("t", mNode.getName());
    Assert.assertTrue(mNode.isLoaded());
  }

  @Test
  public void testSimpleTreeRW() throws IOException {
    IMNode mNode = getSimpleTree();
    metaFile.writeRecursively(mNode);
    mNode = metaFile.readRecursively(mNode.getPersistenceInfo());
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
    root.addChild("s1", new InternalMNode(null, "s1"));
    root.addChild("s2", new InternalMNode(null, "s2"));
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
    metaFile.writeRecursively(mNode);
    mNode = metaFile.readRecursively(mNode.getPersistenceInfo());
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
    StorageGroupMNode s1 = (StorageGroupMNode) (mNode.getChild("p").getChild("s1"));
    StorageGroupMNode s2 = (StorageGroupMNode) (mNode.getChild("p").getChild("s2"));
    Assert.assertEquals(1000, s1.getDataTTL());
    Assert.assertEquals(2000, s2.getDataTTL());
    MeasurementMNode t1 = (MeasurementMNode) mNode.getChild("p").getChild("s1").getChild("t1");
    MeasurementMNode t2 = (MeasurementMNode) mNode.getChild("p").getChild("s2").getChild("t2");
    Assert.assertEquals(t1.getSchema().getProps(), t2.getSchema().getProps());
    Assert.assertEquals(1, t1.getSchema().getProps().size());
    Assert.assertEquals("1", t1.getSchema().getProps().get("a"));
  }

  private IMNode getMTree() {
    IMNode root = new InternalMNode(null, "root");
    IMNode p = new InternalMNode(root, "p");
    root.addChild("p", p);
    StorageGroupMNode s1 = new StorageGroupMNode(null, "s1", 1000);
    StorageGroupMNode s2 = new StorageGroupMNode(null, "s2", 2000);
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

  @Test
  public void testIOPerformance() throws IOException {
    int sgNum = 1;
    int deviceNum = 10;
    int schemaNum = 10000;

    long startTime, endTime;
    startTime = System.currentTimeMillis();
    IMNode root = new InternalMNode(null, "root");
    String s, d, t;
    StorageGroupMNode sg;
    IMNode device;
    MeasurementMNode m;
    for (int k = 0; k < sgNum; k++) {
      s = "sg" + k;
      sg = new StorageGroupMNode(root, s, 1000);
      root.addChild(s, sg);
      for (int i = 0; i < deviceNum; i++) {
        d = "d" + i;
        device = new InternalMNode(sg, d);
        sg.addChild(d, device);
        for (int j = 0; j < schemaNum; j++) {
          t = "t" + j;
          m = new MeasurementMNode(device, t, new MeasurementSchema(t, TSDataType.INT32), null);
          device.addChild(t, m);
        }
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("MTree creation time: " + (endTime - startTime) + "ms.");

    startTime = System.currentTimeMillis();
    metaFile.writeRecursively(root);
    endTime = System.currentTimeMillis();
    System.out.println("MTree persistence time: " + (endTime - startTime) + "ms.");

    IPersistenceInfo persistenceInfo = root.getPersistenceInfo();
    root = null;
    sg = null;
    device = null;
    m = null;
    startTime = System.currentTimeMillis();
    System.gc();
    endTime = System.currentTimeMillis();
    System.out.println("GC time: " + (endTime - startTime) + "ms.");

    startTime = System.currentTimeMillis();
    root = metaFile.readRecursively(persistenceInfo);
    endTime = System.currentTimeMillis();
    System.out.println(count(root));
    System.out.println("MTree read from file time: " + (endTime - startTime) + "ms.");
  }

  private int count(IMNode mNode) {
    int num = 1;
    for (IMNode child : mNode.getChildren().values()) {
      num += count(child);
    }
    return num;
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
