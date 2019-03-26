package org.apache.iotdb.cluster.utils;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PhysicalNodeTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testEqualsObject1() {
    PhysicalNode n1 = new PhysicalNode("192.168.130.1", 6666);
    PhysicalNode n2 = new PhysicalNode("192.168.130.1", 6666);
    assertEquals(n1, n2);
    assertEquals(n1.getKey(), n2.getKey());
    assertEquals(n1.hashCode(), n2.hashCode());
  }

  @Test
  public void testEqualsObject2() {
    PhysicalNode n1 = new PhysicalNode(null, 6666);
    PhysicalNode n2 = new PhysicalNode(null, 6666);
    assertEquals(n1, n2);
    assertEquals(n1.getKey(), n2.getKey());
    assertEquals(n1.hashCode(), n2.hashCode());
  }
}
