package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import org.junit.Assert;
import org.junit.Test;

public class LRUCacheTest {

  @Test
  public void testLRUCache() throws Exception {
    LRUCache cache = new LRUCache(5);
    cache.put(new MNode(null, "1"));
    cache.put(new MNode(null, "2"));
    cache.put(new MNode(null, "3"));
    cache.put(new MNode(null, "4"));
    cache.put(new MNode(null, "5"));
    cache.put(new MNode(null, "6"));
    Assert.assertNull(cache.get(new PartialPath("1")));
    Assert.assertNotNull(cache.get(new PartialPath("2")));
    cache.put(new MNode(null, "7"));
    Assert.assertNotNull(cache.get(new PartialPath("2")));
    Assert.assertNull(cache.get(new PartialPath("3")));
  }
}
