package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;

import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MTreeFileTest {

  private static String BASE_PATH = MTreeFileTest.class.getResource("").getPath();
  private static String MTREE_FILEPATH = BASE_PATH + "mtree.txt";

  private MTreeFile mTreeFile;

  @Before
  public void setUp() throws IOException {
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    mTreeFile = new MTreeFile(MTREE_FILEPATH);
  }

  @After
  public void tearDown() throws IOException {
    mTreeFile.close();
    File file = new File(MTREE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testSimpleMNodeRW() throws IOException {
    MNode mNode = new MNode(null, "root");
    mTreeFile.write(mNode);
    mNode = mTreeFile.read(mNode.getPosition(), null);
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
    mTreeFile.writeRecursively(root);
    MNode mNode = mTreeFile.read("root.p.s.t");
    System.out.println(mNode.getFullPath() + " " + mNode.isLoaded());
  }

  @Test
  public void testSimpleTreeRW() throws IOException {
    MNode mNode = getSimpleTree();
    mTreeFile.writeRecursively(mNode);
    mNode = mTreeFile.readRecursively(mNode.getPosition(), null);
    showTree(mNode);
  }

  private MNode getSimpleTree() {
    MNode root = new MNode(null, "root");
    root.addChild("s1", new MNode(root, "s1"));
    root.addChild("s2", new MNode(root, "s2"));
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
    mTreeFile.writeRecursively(mNode);
    mNode = mTreeFile.readRecursively(mNode.getPosition(), null);
    showTree(mNode);
    StorageGroupMNode s1 = (StorageGroupMNode) mNode.getChild("p").getChild("s1");
    StorageGroupMNode s2 = (StorageGroupMNode) mNode.getChild("p").getChild("s2");
    System.out.println("s1.TTL: " + s1.getDataTTL() + "; s2.TTL: " + s2.getDataTTL());
    MeasurementMNode t1 = (MeasurementMNode) mNode.getChild("p").getChild("s1").getChild("t1");
    MeasurementMNode t2 = (MeasurementMNode) mNode.getChild("p").getChild("s2").getChild("t2");
  }

  private MNode getMTree() {
    MNode root = new MNode(null, "root");
    MNode p = new MNode(root, "p");
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

  private void showTree(MNode mNode) {
    if (mNode == null) {
      return;
    }
    System.out.println(mNode.getFullPath());
    for (MNode child : mNode.getChildren().values()) {
      showTree(child);
    }
  }
}
