package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetaFileTest {

  private static String BASE_PATH = MetaFileTest.class.getResource("").getPath();
  private static String MTREE_FILEPATH = BASE_PATH + "mtree.txt";
  private static String MEASUREMENT_FILEPATH = BASE_PATH + "measurement.txt";

  private MetaFile metaFile;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }

    file = new File(MEASUREMENT_FILEPATH);
    if (file.exists()) {
      file.delete();
    }

    metaFile = new MetaFile(MTREE_FILEPATH, MEASUREMENT_FILEPATH);
  }

  @After
  public void tearDown() throws Exception {
    metaFile.close();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }

    file = new File(MEASUREMENT_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSimpleMNodeRW() throws IOException {
    MNode mNode = new MNode(null, "root");
    metaFile.write(mNode);
    System.out.println(mNode.getPosition());
    mNode = metaFile.readMNode(mNode.getPosition(), false);
    System.out.println(mNode.getName());
  }

  @Test
  public void testPathRW() throws IOException {
    MNode root = new MNode(null, "root");
    MNode p = new MNode(root, "p");
    root.addChild("p", p);
    StorageGroupMNode s = new StorageGroupMNode(root.getChild("p"), "s", 0);
    p.addChild("s", s);
    s.addChild("t", new MeasurementMNode(null, "t", new MeasurementSchema(), null));
    metaFile.writeRecursively(root);
    MNode mNode = metaFile.readMNode("root.p.s.t");
    System.out.println(mNode.getFullPath() + " " + mNode.isLoaded());
  }

  @Test
  public void testSimpleTreeRW() throws IOException {
    MNode mNode = getSimpleTree();
    metaFile.writeRecursively(mNode);
    mNode = metaFile.readRecursively(mNode.getPosition());
    showTree(mNode);
  }

  private MNode getSimpleTree() {
    MNode root = new MNode(null, "root");
    root.addChild("s1", new MNode(null, "s1"));
    root.addChild("s2", new MNode(null, "s2"));
    root.getChild("s1").addChild("t1", new MNode(root.getChild("s1"), "t1"));
    root.getChild("s1").addChild("t2", new MNode(root.getChild("s1"), "t2"));
    root.getChild("s1")
        .getChild("t2")
        .addChild("z1", new MNode(root.getChild("s1").getChild("t2"), "z1"));
    root.getChild("s2").addChild("t1", new MNode(root.getChild("s2"), "t1"));
    root.getChild("s2").addChild("t2", new MNode(root.getChild("s2"), "t2"));
    return root;
  }

  @Test
  public void testMTreeRW() throws IOException {
    MNode mNode = getMTree();
    metaFile.writeRecursively(mNode);
    mNode = metaFile.readRecursively(mNode.getPosition());
    showTree(mNode);
    StorageGroupMNode s1 = (StorageGroupMNode) (mNode.getChild("p").getChild("s1"));
    StorageGroupMNode s2 = (StorageGroupMNode) (mNode.getChild("p").getChild("s2"));
    System.out.println("s1.TTL: " + s1.getDataTTL() + "; s2.TTL: " + s2.getDataTTL());
    MeasurementMNode t1 = (MeasurementMNode) mNode.getChild("p").getChild("s1").getChild("t1");
    MeasurementMNode t2 = (MeasurementMNode) mNode.getChild("p").getChild("s2").getChild("t2");
    System.out.println((t1.getSchema().getProps()).equals(t2.getSchema().getProps()));
    System.out.println("Props: ");
    for (Map.Entry<String, String> entry : t1.getSchema().getProps().entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
  }

  private MNode getMTree() {
    MNode root = new MNode(null, "root");
    MNode p = new MNode(root, "p");
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

  private void showTree(MNode mNode) {
    if (mNode == null) {
      return;
    }
    System.out.println(mNode.getFullPath());
    for (MNode child : mNode.getChildren().values()) {
      showTree(child);
    }
  }

  @Test
  public void testIOPerformance() throws IOException {
    int deviceNum = 100;
    int schemaNum = 100;

    long startTime, endTime;
    startTime = System.currentTimeMillis();
    MNode root = new MNode(null, "root");
    StorageGroupMNode sg = new StorageGroupMNode(root, "sg", 1000);
    root.addChild("sg", sg);
    String d, t;
    MeasurementMNode m;
    for (int i = 0; i < deviceNum; i++) {
      d = "d" + i;
      sg.addChild(d, new MNode(sg, d));
      for (int j = 0; j < schemaNum; j++) {
        t = "t" + j;
        m =
            new MeasurementMNode(
                sg.getChild(d), t, new MeasurementSchema(t, TSDataType.INT32), null);
        sg.getChild(d).addChild(t, m);
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("MTree creation time: " + (endTime - startTime) + "ms.");

    startTime = System.currentTimeMillis();
    metaFile.writeRecursively(root);
    endTime = System.currentTimeMillis();
    System.out.println("MTree persistence time: " + (endTime - startTime) + "ms.");

    startTime = System.currentTimeMillis();
    root = metaFile.readRecursively(root.getPosition());
    endTime = System.currentTimeMillis();
    System.out.println(count(root));
    System.out.println("MTree read from file time: " + (endTime - startTime) + "ms.");
  }

  private int count(MNode mNode) {
    int num = 1;
    for (MNode child : mNode.getChildren().values()) {
      num += count(child);
    }
    return num;
  }
}
