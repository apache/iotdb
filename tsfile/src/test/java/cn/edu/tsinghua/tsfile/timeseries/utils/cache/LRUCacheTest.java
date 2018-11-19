package cn.edu.tsinghua.tsfile.timeseries.utils.cache;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class LRUCacheTest {

    private LRUCache<Integer, Integer> cache;

    @Test
    public void test() {
        try {
            int testCount = 1000;
            int cacheSize = 5;
            cache = new LRUCache<Integer, Integer>(cacheSize) {
                @Override
                public void beforeRemove(Integer object) {
                    return;
                }

                @Override
                public Integer loadObjectByKey(Integer key) {
                    return key * 10;
                }
            };

            for (int i = 1; i < testCount; i++) {
                Assert.assertEquals(i * 10, (int) cache.get(i));
                Assert.assertEquals((i - 1) * 10, (int) cache.get(i - 1));
            }
        } catch (CacheException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
