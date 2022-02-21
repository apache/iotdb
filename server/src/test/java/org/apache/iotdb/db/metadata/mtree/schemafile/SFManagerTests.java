package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SFManager;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.management.snmp.jvmmib.JvmMemMgrPoolRelTableMeta;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class SFManagerTests {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    // EnvironmentUtils.cleanEnv();
  }

  @Test
  public void initManager() throws MetadataException, IOException {
    SFManager sfManager = SFManager.getInstance();
    sfManager.init();
    printPreOrderTree(sfManager.getUpperMTree());
  }

  @Test
  public void readWriteTest() throws MetadataException, IOException{
    IMNode root = getAnUpperTree();
    Iterator<IMNode> ite = preOrderTraverse(root);
    int seed = 0;
    while (ite.hasNext()) {
      IMNode node = ite.next();
      seed ++;
      if (node.isStorageGroup()) {
        fillLowerTree(node, "sed" + seed);
      }
    }
    printPreOrderSeries(root);

    SFManager sfManager = SFManager.getInstance();
    sfManager.init();
    ite = preOrderTraverse(root);

    while (ite.hasNext()) {
      IMNode sgNode = ite.next();
      if (sgNode.isStorageGroup()) {
        Iterator<IMNode> innerIte = preOrderTraverseSeries(sgNode);
        for (IMNode cur = innerIte.next(); innerIte.hasNext(); cur = innerIte.next()) {
          if (!cur.isMeasurement()) {
            sfManager.writeMNode(cur);
          }
        }
      }
    }

    sfManager.close();
  }

  // region Tools to build a tree

  private void printPreOrderSeries(IMNode node) {
    Iterator<IMNode> ite = preOrderTraverseSeries(node);
    while (ite.hasNext()) {
      IMNode cur = ite.next();
      if (cur.isMeasurement()) {
        System.out.println(cur.getFullPath());
      }
    }
  }

  private void printPreOrderTree(IMNode node) {
    Iterator<IMNode> ite = preOrderTraverse(node);
    while (ite.hasNext()) {
      IMNode cur = ite.next();
      if (cur.isStorageGroup()) {
        System.out.println(cur.getFullPath());
      }
    }
  }


  private IMNode fillLowerTree(IMNode sgNode, String seed) throws MetadataException{
    buildSeries(sgNode, "GPS.x");
    buildSeries(sgNode, "GPS.y");
    buildSeries(sgNode, seed + ".vehicle.gas");
    buildSeries(sgNode, seed + ".vehicle.speed");
    return sgNode;
  }

  private IMNode getAnUpperTree() throws MetadataException{
    IMNode res = new InternalMNode(null, "root");
    buildUpperTree(res, "root.sg1");
    buildUpperTree(res, "root.kv1.sg2");
    buildUpperTree(res, "root.ph.pre.sg3");
    return res;
  }

  private IMNode buildUpperTree(IMNode root, String sgPath) throws MetadataException {
    return buildUpperTree(root, sgPath, 1000000L);
  }

  private IMNode buildUpperTree(IMNode root, String sgPath, long dataTTL) throws MetadataException{
    String[] nodes = MetaUtils.splitPathToDetachedPath(sgPath);
    IMNode cur = root;
    for (int i = 1; i < nodes.length - 1; i++) {
      if (!cur.hasChild(nodes[i])) {
        cur.addChild(new InternalMNode(cur, nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
    }
    cur.addChild(new StorageGroupMNode(cur, nodes[nodes.length - 1], dataTTL));
    return cur.getChild(nodes[nodes.length - 1]);
  }

  private IMNode buildSeries(IMNode sgNode, String path) throws MetadataException {
    return buildSeries(sgNode, path, null);
  }

  private IMNode buildSeries(IMNode sgNode, String path, IMeasurementSchema schema) throws MetadataException{
    String[] nodes = MetaUtils.splitPathToDetachedPath(path);
    IMNode cur = sgNode;
    for (int i = 0; i < nodes.length - 1; i++) {
      if (!cur.hasChild(nodes[i])) {
        cur.addChild(new InternalMNode(cur, nodes[i]));
      }
      cur = cur.getChild(nodes[i]);
    }
    IMeasurementSchema curSchema = schema == null ? new MeasurementSchema(nodes[nodes.length-1], TSDataType.INT32) : schema;
    if (!cur.isEntity()) {
      cur = MNodeUtils.setToEntity(cur);
    }
    cur.addChild(MeasurementMNode.getMeasurementMNode(cur.getAsEntityMNode(), curSchema.getMeasurementId(), curSchema, curSchema + "alias"));
    return cur.getChild(curSchema.getMeasurementId());
  }

  private Iterator<IMNode> preOrderTraverse(IMNode node) {
    return new Iterator<IMNode>() {
      Deque<IMNode> stack = new ArrayDeque<>();

      {
        stack.push(node);
      }

      @Override
      public boolean hasNext() {
        return stack.size() != 0;
      }

      @Override
      public IMNode next() {
        IMNode cur = stack.pop();
        if (!cur.isStorageGroup()) {
          for (IMNode node: cur.getChildren().values()) {
            stack.push(node);
          }
        }
        return cur;
      }
    };
  }

  private Iterator<IMNode> preOrderTraverseSeries(IMNode node) {
    return new Iterator<IMNode>() {
      Deque<IMNode> stack = new ArrayDeque<>();

      {
        stack.push(node);
      }

      @Override
      public boolean hasNext() {
        return stack.size() != 0;
      }

      @Override
      public IMNode next() {
        IMNode cur = stack.pop();
        if (!cur.isMeasurement()) {
          for (IMNode node: cur.getChildren().values()) {
            stack.push(node);
          }
        }
        return cur;
      }
    };
  }

  // endregion


}
