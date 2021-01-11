package org.apache.iotdb.db.metadata.mnode;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.junit.Test;

public class MNodeTest{

  @Test
  public void testReplaceChild() {
    // after replacing a with c, the timeseries root.a.b becomes root.c.d
    MNode rootNode = new MNode(null, "root");

    MNode aNode = new MNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode);

    MNode bNode = new MNode(aNode, "b");
    aNode.addChild(bNode.getName(), bNode);

    List<Thread> threadList = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      threadList.add(new Thread(() -> rootNode.replaceChild(aNode.getName(), new MNode(null, "c"))));
    }
    threadList.forEach(Thread::start);

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    assertEquals("root.c.b", multiFullPaths.get(0));
  }

}