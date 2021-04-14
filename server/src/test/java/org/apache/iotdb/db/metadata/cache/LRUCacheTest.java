package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import org.junit.Assert;
import org.junit.Test;

public class LRUCacheTest {

  @Test
  public void testLRUCache() throws Exception {
    LRUCache cache = new LRUCache(5);
    cache.put("1", new MNode(null, "1"));
    cache.put("2", new MNode(null, "2"));
    cache.put("3", new MNode(null, "3"));
    cache.put("4", new MNode(null, "4"));
    cache.put("5", new MNode(null, "5"));
    cache.put("6", new MNode(null, "6"));
    Assert.assertNull(cache.get("1"));
    Assert.assertNotNull(cache.get("2"));
    cache.put("7", new MNode(null, "7"));
    Assert.assertNotNull(cache.get("2"));
    Assert.assertNull(cache.get("3"));
  }
}
