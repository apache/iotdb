package org.apache.iotdb.db.metadata;

import java.util.Arrays;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartialPathTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testConcatPath() {
    String[] arr1 = new String[2];
    arr1[0] = "root";
    arr1[1] = "sg1";
    PartialPath a = new PartialPath(arr1);
    String[] arr2 = new String[2];
    arr2[0] = "d1";
    arr2[1] = "s1";
    PartialPath b = new PartialPath(arr2);
    a.concatPath(b);
    Assert.assertEquals("[root, sg1, d1, s1]", Arrays.toString(a.getNodes()));
  }

}
