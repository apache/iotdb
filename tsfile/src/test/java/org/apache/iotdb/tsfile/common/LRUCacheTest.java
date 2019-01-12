package org.apache.iotdb.tsfile.common;

import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class LRUCacheTest {

    private LRUCache<Integer, Integer> cache;

    @Test
    public void test() {
        try {
            int testCount = 1000;
            int cacheSize = 5;
            cache = new LRUCache<Integer, Integer>(cacheSize) {

                @Override
                public Integer loadObjectByKey(Integer key) {
                    return key * 10;
                }
            };

            for (int i = 1; i < testCount; i++) {
                Assert.assertEquals(i * 10, (int) cache.get(i));
                Assert.assertEquals((i - 1) * 10, (int) cache.get(i - 1));
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
