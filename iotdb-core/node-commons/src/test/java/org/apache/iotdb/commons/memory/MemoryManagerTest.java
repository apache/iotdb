package org.apache.iotdb.commons.memory;

import org.junit.Assert;
import org.junit.Test;

public class MemoryManagerTest {

  @Test
  public void TestGetName() {
    MemoryManager memoryManager = MemoryManager.global();
    Assert.assertEquals("GlobalMemoryManager", memoryManager.getName());
  }
}